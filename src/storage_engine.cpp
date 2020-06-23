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

namespace diamond {

    /* Static */
    int StorageEngine::default_compare(const Buffer& b0, const Buffer& b1) {
        size_t b0_n = b0.size();
        size_t b1_n = b1.size();
        size_t n = (b0_n <= b1_n) ? b0_n : b1_n;
        int r = std::memcmp(b0.buffer(), b1.buffer(), n);
        if (r != 0 || b0_n == b1_n) {
            return r;
        }
        return (b0_n < b1_n) ? -1 : 1;
    }

    StorageEngine::StorageEngine(PageManager& page_manager)
            : _manager(page_manager) {
        if (_manager.storage().size() == 0) {
            _manager.create_page(Page::Type::COLLECTIONS);
        }
    }

    uint64_t StorageEngine::count(const Buffer& collection_name) {
        Collection collection = get_or_create_collection(collection_name);
        Page::ID page_id = collection.root_node_id;
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

    bool StorageEngine::exists(const Buffer& collection_name, const Buffer& key, Compare compare_func) {
        Collection collection = get_or_create_collection(collection_name);
        PageAccessor page = get_leaf_page(
            collection.root_node_id,
            key,
            compare_func);
        SharedPageLock page_lock(page);
        Page::LeafNodeEntryListIterator iter = find_leaf_node_entry(page, key, compare_func);
        return iter != page->leaf_node_entries_end();
    }

    Buffer StorageEngine::get(const Buffer& collection_name, const Buffer& key, Compare compare_func) {
        Collection collection = get_or_create_collection(collection_name);
        PageAccessor page = get_leaf_page(
            collection.root_node_id,
            key,
            compare_func);
        SharedPageLock page_lock(page);
        Page::LeafNodeEntryListIterator iter = find_leaf_node_entry(page, key, compare_func);
        if (iter == page->leaf_node_entries_end()) {
            throw Exception(ErrorCode::ENTRY_NOT_FOUND);
        }
        const Page::LeafNodeEntry& entry = *iter;
        PageAccessor data_page = _manager.get_page(entry.val_data_id());
        SharedPageLock data_page_lock(data_page);
        return Buffer(data_page->get_data_entry(entry.val_data_index()).data());      
    }

    void StorageEngine::put(const Buffer& collection_name, Buffer key, Buffer val, Compare compare_func) {
        Collection collection = get_or_create_collection(collection_name);
        {
            // Make optimisitic descent
            PageAccessor page = get_leaf_page(collection.root_node_id, key, compare_func);
            UniquePageLock page_lock(page);
            Page::LeafNodeEntryListIterator iter = find_leaf_node_entry(page, key, compare_func);
            if (iter != page->leaf_node_entries_end()) {
                // CASE 1: Entry with key exists, update the value
                auto [val_data_id, val_data_index] = insert_value_into_data_page(
                    collection.free_list_id, std::move(val));
                (*iter).set_val_data_ptr(val_data_id, val_data_index);
                _manager.write_page(page.instance());
                return;
            } else if (page->can_insert_leaf_node_entry()) {
                // CASE 2: Leaf Page is safe, insert
                auto [key_data_id, key_data_index] = insert_value_into_data_page(
                    collection.free_list_id, std::move(key));
                auto [val_data_id, val_data_index] = insert_value_into_data_page(
                    collection.free_list_id, std::move(val));
                page->insert_leaf_node_entry(
                    iter,
                    key_data_id,
                    key_data_index,
                    val_data_id,
                    val_data_index);
                _manager.write_page(page.instance());
                return;
            }
        }

        // TODO: Make descent using Update Locks, unlocking only when we know
        // a node is safe
    }

    StorageEngine::Iterator StorageEngine::get_iterator(const Buffer& collection_name) {
        Collection collection = get_or_create_collection(collection_name);
        Page::ID page_id = collection.root_node_id;
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

    StorageEngine::Collection StorageEngine::create_collection(const Buffer& name) {
        Page::ID page_id = 1;
        while (true) {
            PageAccessor page = _manager.get_page(page_id);
            if (page->get_type() != Page::Type::COLLECTIONS) {
                throw Exception(ErrorCode::CORRUPTED_FILE);
            }

            UpgradePageLock page_lock(page);
            if (page->has_collection(name)) {
                // CASE 1: Some other thread has already created it, we can downgrade the lock.
                page_lock.downgrade();
                const Page::Collection& collection = page->get_collection(name);
                return Collection{
                    .root_node_id = collection.root_node_id(),
                    .free_list_id = collection.free_list_id()
                };
            }

            if (page->can_insert_collection(name)) {
                // CASE 2: There is space in the current COLLECTIONS page.
                page_lock.upgrade();
                PageAccessor root_page = _manager.create_page(Page::Type::LEAF_NODE);
                PageAccessor free_list_page = _manager.create_page(Page::Type::FREE_LIST);
                page->add_collection(name, root_page->get_id(), free_list_page->get_id());
                _manager.write_page(page.instance());
                return Collection{
                    .root_node_id = root_page->get_id(),
                    .free_list_id = free_list_page->get_id()
                };
            }

            page_id = page->get_next_collections_page();
            if (page_id != Page::INVALID_ID) continue;

            // CASE 3: There is no space in the last COLLECTIONS page, create a new one.
            PageAccessor root_page = _manager.create_page(Page::Type::LEAF_NODE);
            PageAccessor free_list_page = _manager.create_page(Page::Type::FREE_LIST);
            PageAccessor new_collections_page = _manager.create_page(Page::Type::COLLECTIONS);
            UniquePageLock new_collections_page_lock(new_collections_page);
            new_collections_page->add_collection(name, root_page->get_id(), free_list_page->get_id());
            page->set_next_collections_page(new_collections_page->get_id());
            _manager.write_page(new_collections_page.instance());
            _manager.write_page(page.instance());

            return Collection{
                .root_node_id = root_page->get_id(),
                .free_list_id = free_list_page->get_id()
            };
        }
    }

    StorageEngine::Collection StorageEngine::get_or_create_collection(const Buffer& name) {
        bool created;
        return get_or_create_collection(name, created);
    }

    StorageEngine::Collection StorageEngine::get_or_create_collection(const Buffer& name, bool& created) {
        Page::ID page_id = 1;
        while (page_id != Page::INVALID_ID) {
            // CASE 1: Iterate over the collections list with a shared lock to avoid contention.
            PageAccessor page = _manager.get_page(page_id);
            SharedPageLock page_lock(page);
            if (page->has_collection(name)) {
                const Page::Collection& collection = page->get_collection(name);
                created = false;
                return Collection{
                    .root_node_id = collection.root_node_id(),
                    .free_list_id = collection.free_list_id()
                };
            }

            page_id = page->get_next_collections_page();
        }

        /* 
            CASE 2: Second iteration using upgradeable locks, it will either create
            the collection entry or get the collection another thread created.
        */
        created = true;
        return create_collection(name);
    }

    // NOTE: This method must be called with a lock on page.
    Page::InternalNodeEntryListIterator StorageEngine::search_internal_node_entries(
            PageAccessor& page,
            const Buffer& key,
            Compare compare_func) {
        Page::InternalNodeEntryListIterator prev = page->internal_node_entries_end();
        Page::InternalNodeEntryListIterator iter = page->internal_node_entries_begin();
        Page::InternalNodeEntryListIterator end = page->internal_node_entries_end();
        while (iter != end) {
            const Page::InternalNodeEntry& entry = *iter;
            PageAccessor data_page = _manager.get_page(entry.key_data_id());
            if (data_page->get_type() != Page::Type::DATA) {
                throw Exception(ErrorCode::CORRUPTED_FILE);
            }

            SharedPageLock data_page_lock(data_page);
            const Page::DataEntry data_entry = data_page->get_data_entry(entry.key_data_index());
            if (compare_func(data_entry.data(), key) >= 0) {
                return iter;
            }

            prev = iter++;
        }

        return prev;
    }

    // NOTE: This method must be called with a lock on page.
    Page::LeafNodeEntryListIterator StorageEngine::find_leaf_node_entry(
            PageAccessor& page,
            const Buffer& key,
            Compare compare_func) {
        Page::LeafNodeEntryListIterator iter = page->leaf_node_entries_begin();
        Page::LeafNodeEntryListIterator end = page->leaf_node_entries_end();
        while (iter != end) {
            const Page::LeafNodeEntry& entry = *iter;
            PageAccessor data_page = _manager.get_page(entry.key_data_id());
            if (data_page->get_type() != Page::Type::DATA) {
                throw Exception(ErrorCode::CORRUPTED_FILE);
            }

            SharedPageLock data_page_lock(data_page);
            const Page::DataEntry data_entry = data_page->get_data_entry(entry.key_data_index());
            if (compare_func(data_entry.data(), key) == 0) {
                return iter;
            }

            iter++;
        }

        return iter;
    }

    PageAccessor StorageEngine::get_leaf_page(
            Page::ID root_node_id,
            const Buffer& key,
            Compare compare_func) {
        Page::ID page_id = root_node_id;
        while (true) {
            PageAccessor page = _manager.get_page(page_id);
            Page::Type type = page->get_type();
            switch (type) {
            case Page::Type::INTERNAL_NODE: {
                SharedPageLock page_lock(page);
                page_id = (*search_internal_node_entries(page, key, compare_func)).next_node_id();
                break;
            }
            case Page::Type::LEAF_NODE:
                return page;
            default:
                throw Exception(ErrorCode::CORRUPTED_FILE);
            }
        }
    }

    std::tuple<Page::ID, size_t> StorageEngine::insert_value_into_data_page(
            Page::ID free_list_id,
            const Buffer& val) {
        Page::ID data_page_id;
        size_t data_page_index;
        Page::ID page_id = free_list_id;
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
                if (data_page->get_type() != Page::Type::DATA) {
                    throw Exception(ErrorCode::CORRUPTED_FILE);
                }
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

        return std::make_tuple(data_page_id, data_page_index);
    }

    StorageEngine::Iterator::~Iterator() {
        if (_leaf_page_iterator != nullptr) {
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
        LeafPageIterator* new_leaf_page_iterator = new LeafPageIterator(std::move(next_page));
        delete _leaf_page_iterator;
        _leaf_page_iterator = new_leaf_page_iterator;
    }

    Buffer StorageEngine::Iterator::key() {
        Page::LeafNodeEntry entry = *(_leaf_page_iterator->iter);
        PageAccessor data_page = _manager.get_page(entry.key_data_id());
        if (data_page->get_type() != Page::Type::DATA) {
            throw Exception(ErrorCode::CORRUPTED_FILE);
        }
        SharedPageLock data_page_lock(data_page);
        return data_page->get_data_entry(entry.key_data_index()).data();
    }

    Buffer StorageEngine::Iterator::val() {
        Page::LeafNodeEntry entry = *(_leaf_page_iterator->iter);
        PageAccessor data_page = _manager.get_page(entry.val_data_id());
        if (data_page->get_type() != Page::Type::DATA) {
            throw Exception(ErrorCode::CORRUPTED_FILE);
        }
        SharedPageLock data_page_lock(data_page);
        return data_page->get_data_entry(entry.val_data_index()).data();
    }

    bool StorageEngine::Iterator::end() const {
        return _leaf_page_iterator == nullptr;
    }

    StorageEngine::Iterator::Iterator(PageManager& manager, PageAccessor page) 
        : _manager(manager),
        _leaf_page_iterator(new LeafPageIterator(std::move(page))) {}

    StorageEngine::Iterator::LeafPageIterator::LeafPageIterator(PageAccessor _page)
        : page(std::move(_page)),
        lock(page),
        iter(page->leaf_node_entries_begin()) {}

} // namespace diamond
