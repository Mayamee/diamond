/*  Diamond - Embedded NoSQL Database
**  Copyright (C) 2020  Zach Perkitny
**
**  This program is free software: you can redistribute it and/or modify
**  it under the terms of the GNU General Public License as published by
**  the Free Software Foundation, either version 3 of the License, or
**  (at your option) any later version.
**
**  This program is distributed in the hope that it will be useful,
**  but WITHOUT ANY WARRANTY; without even the implied warranty of
**  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
**  GNU General Public License for more details.
**
**  You should have received a copy of the GNU General Public License
**  along with this program.  If not, see <https://www.gnu.org/licenses/>.
*/

#include "diamond/storage_engine.h"
#include "diamond/exception.h"

namespace diamond {

    StorageEngine::StorageEngine(PageManager& page_manager)
            : _manager(page_manager) {
        if (_manager.storage().size() == 0) {
            _manager.create_page(Page::Type::ROOTS);
            _manager.create_page(Page::Type::FREE_LIST);
        }
    }

    uint64_t StorageEngine::count(const Buffer& id) {
        Page::ID page_id;
        if (!get_root_node_id(id, page_id)) {
            return 0;
        }

        uint64_t count = 0;
        while (page_id) {
            PageAccessor page = _manager.get_page(page_id);
            SharedPageLock page_lock(page);
            switch (page->get_type()) {
            case Page::Type::INTERNAL_NODE:
                page_id = (*++page->internal_node_entries_begin()).next_node_id();
                break;
            case Page::Type::LEAF_NODE:
                count += page->get_num_leaf_node_entries();
                page_id = page->get_next_leaf_node_page();
                break;
            default:
                throw Exception(ErrorCode::CORRUPTED_FILE);
            }
        }

        return count;
    }

    bool StorageEngine::exists(const Buffer& id, const Buffer& key, Page::Compare compare_func) {
        PageAccessor page = get_leaf_page(id, key, compare_func);
        SharedPageLock page_lock(page);
        Page::LeafNodeEntryListIterator iter = page->find_leaf_node_entry(key, compare_func);
        return iter != page->leaf_node_entries_end();
    }

    Buffer StorageEngine::get(const Buffer& id, const Buffer& key, Page::Compare compare_func) {
        PageAccessor page = get_leaf_page(id, key, compare_func);
        SharedPageLock page_lock(page);
        Page::LeafNodeEntryListIterator iter = page->find_leaf_node_entry(key, compare_func);
        if (iter == page->leaf_node_entries_end()) {
            throw Exception(ErrorCode::ENTRY_NOT_FOUND);
        }
        const Page::LeafNodeEntry& entry = *iter;
        PageAccessor data_page = _manager.get_page(entry.data_id());
        SharedPageLock data_page_lock(data_page);
        return Buffer(data_page->get_data_entry(entry.data_index()).data());      
    }

    void StorageEngine::insert(const Buffer& id, Buffer key, Buffer val, Page::Compare compare_func) {
        Page::ID data_page_id;
        size_t data_page_index;
        {
            // Make optimisitic descent
            PageAccessor page = get_leaf_page(id, key, compare_func);
            UniquePageLock page_lock(page);
            Page::LeafNodeEntryListIterator iter = page->find_leaf_node_entry(key, compare_func);
            if (iter != page->leaf_node_entries_end()) {
                throw Exception(ErrorCode::DUPLICATE_ENTRY_KEY);
            }

            // Leaf Page is safe, insert
            if (page->can_insert_leaf_node_entry(key)) {
                insert_into_data_page(val, data_page_id, data_page_index);
                page->insert_leaf_node_entry(key, data_page_id, data_page_index, compare_func);
                return;
            }
        }

        // PageAccessor leaf_accessor = _manager.create_page(Page::Type::LEAF_NODE);
        // page->split_leaf_node_entries(leaf_accessor.instance());
    }

    StorageEngine::Iterator StorageEngine::get_iterator(const Buffer& id) {
        Page::ID page_id;
        if (!get_root_node_id(id, page_id)) {
            return Iterator(_manager, create_root_node_page(id));
        }

        while (true) {
            PageAccessor page = _manager.get_page(page_id);
            switch (page->get_type()) {
            case Page::Type::INTERNAL_NODE: {
                SharedPageLock page_lock(page);
                page_id = (*++page->internal_node_entries_begin()).next_node_id();
                break;
            }
            case Page::Type::LEAF_NODE:
                return Iterator(_manager, std::move(page));
            default:
                throw Exception(ErrorCode::CORRUPTED_FILE);
            }
        }
    }

    PageAccessor StorageEngine::get_leaf_page(const Buffer& id, const Buffer& key, Page::Compare compare_func) {
        Page::ID page_id;
        if (!get_root_node_id(id, page_id)) {
            return create_root_node_page(id);
        }

        while (true) {
            PageAccessor page = _manager.get_page(page_id);
            Page::Type type = page->get_type();
            switch (type) {
            case Page::Type::INTERNAL_NODE: {
                SharedPageLock page_lock(page);
                page_id = (*page->search_internal_node_entries(key, compare_func)).next_node_id();
                break;
            }
            case Page::Type::LEAF_NODE:
                return page;
            default:
                throw Exception(ErrorCode::CORRUPTED_FILE);
            }
        }
    }

    void StorageEngine::insert_into_data_page(const Buffer& val, Page::ID& data_page_id, size_t& data_page_index) {
        Page::ID page_id = 2;
        while (true) {
            PageAccessor page = _manager.get_page(page_id);
            if (page->get_type() != Page::Type::FREE_LIST) {
                throw Exception(ErrorCode::CORRUPTED_FILE);
            }

            UniquePageLock page_lock(page);
            // CASE 1: Free List has entry with sufficient data space
            if (page->reserve_free_list_entry(val, data_page_id)) {
                _manager.write_page(page.instance());
                page_lock.unlock(); // We've already reserved the entry and queued up a write, we can unlock
                PageAccessor data_page = _manager.get_page(data_page_id);
                UniquePageLock data_page_lock(data_page);
                data_page_index = data_page->insert_data_entry(val);
                _manager.write_page(data_page.instance());
                break;
            }

            page_id = page->get_next_free_list_page();
            if (page_id != Page::INVALID_ID) continue;

            PageAccessor new_data_page = _manager.create_page(Page::Type::DATA);
            UniquePageLock new_data_page_lock(new_data_page);
            data_page_id = new_data_page->get_id();
            data_page_index = new_data_page->insert_data_entry(val);

            // CASE 2: Free List did not have an entry with sufficient space, create a new data page
            if (page->can_insert_free_list_entry()) {
                page->insert_free_list_entry(
                    data_page_id,
                    new_data_page->get_remaining_space());
                _manager.write_page(new_data_page.instance());
                _manager.write_page(page.instance());
                break;
            }

            // CASE 3: Free List does not have enough room for an entry, create a new one
            PageAccessor new_free_list_page = _manager.create_page(Page::Type::FREE_LIST);
            UniquePageLock new_free_list_page_lock(new_free_list_page);
            new_free_list_page->insert_free_list_entry(
                data_page_id,
                new_data_page->get_remaining_space());
            page->set_next_free_list_page(new_free_list_page->get_id());

            _manager.write_page(new_data_page.instance());
            _manager.write_page(new_free_list_page.instance());
            _manager.write_page(page.instance());
            break;
        }
    }

    bool StorageEngine::get_root_node_id(const Buffer& id, Page::ID& root_node_id) {
        Page::ID page_id = 1;
        while (page_id != Page::INVALID_ID) {
            PageAccessor page = _manager.get_page(page_id);
            SharedPageLock page_lock(page);
            if (page->get_root_node_id(id, root_node_id)) {
                return true;
            }

            page_id = page->get_next_roots_page();
        }

        return false;
    }

    PageAccessor StorageEngine::create_root_node_page(const Buffer& id) {
        Page::ID page_id = 1;
        while (true) {
            PageAccessor page = _manager.get_page(page_id);
            UniquePageLock page_lock(page);
            page_id = page->get_next_roots_page();
            if (page_id != Page::INVALID_ID) continue;
            PageAccessor root_page = _manager.create_page(Page::Type::LEAF_NODE);
            SharedPageLock root_page_lock(root_page);
            if (page->can_insert_root_node_id(id)) {
                page->set_root_node_id(id, root_page->get_id());
                _manager.write_page(page.instance());
                return root_page;
            }

            PageAccessor new_roots_page = _manager.create_page(Page::Type::ROOTS);
            UniquePageLock new_roots_page_lock(new_roots_page);
            new_roots_page->set_root_node_id(id, root_page->get_id());
            page->set_next_roots_page(new_roots_page->get_id());
            _manager.write_page(new_roots_page.instance());
            _manager.write_page(page.instance());

            return root_page;
        }
    }

    StorageEngine::Iterator::~Iterator() {
        if (_leaf_page_iterator) {
            delete _leaf_page_iterator;
        }
    }

    void StorageEngine::Iterator::next() {
        if (++_leaf_page_iterator->iter != 
                _leaf_page_iterator->page->leaf_node_entries_end()) return;
        if (_leaf_page_iterator->page->get_next_leaf_node_page() == Page::INVALID_ID) {
            delete _leaf_page_iterator;
            _leaf_page_iterator = nullptr;
            return;
        }
        PageAccessor next_page = _manager.get_page(
            _leaf_page_iterator->page->get_next_leaf_node_page());
        if (next_page->get_type() != Page::Type::LEAF_NODE) {
            throw Exception(ErrorCode::CORRUPTED_FILE);
        }
        SharedPageLock next_page_lock(next_page);
        LeafPageIterator* new_leaf_page_iterator = new LeafPageIterator{
            .page = std::move(next_page),
            .iter = next_page->leaf_node_entries_begin()
        };
        delete _leaf_page_iterator;
        _leaf_page_iterator = new_leaf_page_iterator;
    }

    Buffer StorageEngine::Iterator::key() {
        Page::LeafNodeEntry entry = *(_leaf_page_iterator->iter);
        return entry.key();
    }

    Buffer StorageEngine::Iterator::val() {
        Page::LeafNodeEntry entry = *(_leaf_page_iterator->iter);
        PageAccessor data_page = _manager.get_page(entry.data_id());
        SharedPageLock data_page_lock(data_page);
        return data_page->get_data_entry(entry.data_index()).data();
    }

    bool StorageEngine::Iterator::end() const {
        return _leaf_page_iterator == nullptr;
    }

    StorageEngine::Iterator::Iterator(PageManager& manager, PageAccessor page) 
        : _manager(manager),
        _leaf_page_iterator(new LeafPageIterator{
            .page = std::move(page),
            .iter = page->leaf_node_entries_begin()
        }) {}

} // namespace diamond
