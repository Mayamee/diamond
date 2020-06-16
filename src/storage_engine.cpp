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

    StorageEngine::StorageEngine(PageManager& page_manager, Page::Compare compare_func)
            : _manager(page_manager),
            _compare_func(compare_func) {
        if (_manager.storage().size() == 0) {
            _manager.create_page(Page::Type::ROOTS, PageAccessor::Mode::SHARED);
            _manager.create_page(Page::Type::FREE_LIST, PageAccessor::Mode::SHARED);
        }
    }

    Buffer StorageEngine::get(const Buffer& id, const Buffer& key) {
        PageAccessor page = get_leaf_page(id, key);
        Page::LeafNodeEntryListIterator iter = page->find_leaf_node_entry(key, _compare_func);
        if (iter == page->leaf_node_entries_end()) {
            throw Exception(ErrorCode::ENTRY_NOT_FOUND);
        }
        const Page::LeafNodeEntry& entry = *iter;
        PageAccessor data_page = _manager.get_page(entry.data_id(), PageAccessor::Mode::SHARED);
        return Buffer(data_page->get_data_entry(entry.data_index()).data());      
    }

    void StorageEngine::insert(const Buffer& id, Buffer key, Buffer val) {
        // TODO: Figure out locking
        PageAccessor page = get_leaf_page(id, key);
        Page::LeafNodeEntryListIterator iter = page->find_leaf_node_entry(key, _compare_func);
        if (iter != page->leaf_node_entries_end()) {
            throw Exception(ErrorCode::DUPLICATE_ENTRY_KEY);
        }

        Page::ID data_page_id;
        size_t data_page_index;
        {
            PageAccessor data_page = get_free_data_page(val);
            data_page_id = data_page->get_id();
            data_page_index = data_page->insert_data_entry(val);
            _manager.write_page(data_page.instance());
        }

        if (page->can_insert_leaf_node_entry(key)) {
            page->insert_leaf_node_entry(key, data_page_id, data_page_index);
            _manager.write_page(page.instance());
            return;
        }

        // PageAccessor leaf_accessor = _manager.create_page(Page::Type::LEAF_NODE);
        // page->split_leaf_node_entries(leaf_accessor.instance());
    }

    PageAccessor StorageEngine::get_leaf_page(const Buffer& id, const Buffer& key) {
        Page::ID page_id;
        if (!get_root_node_id(id, page_id)) {
            return create_root_node_page(id);
        }
        while (true) {
            PageAccessor page = _manager.get_page(page_id, PageAccessor::Mode::SHARED);
            Page::Type type = page->get_type();
            switch (type) {
            case Page::Type::INTERNAL_NODE: {
                size_t i = page->search_internal_node_entries(key, _compare_func);
                page_id = page->get_internal_node_entry(i).next_node_id();
                break;
            }
            case Page::Type::LEAF_NODE:
                return page;
            default:
                throw Exception(ErrorCode::CORRUPTED_FILE);
            }
        }
    }

    PageAccessor StorageEngine::get_free_data_page(const Buffer& val) {
        Page::ID data_page;
        Page::ID page_id = 2;
        while (true) {
            PageAccessor page = _manager.get_page(page_id, PageAccessor::Mode::EXCLUSIVE);
            if (page->get_type() != Page::Type::FREE_LIST) {
                throw Exception(ErrorCode::CORRUPTED_FILE);
            }

            if (page->reserve_free_list_entry(val, data_page)) {
                return _manager.get_page(data_page, PageAccessor::Mode::EXCLUSIVE);
            }

            page_id = page->get_next_free_list_page();
            if (page_id == Page::INVALID_ID) {
                PageAccessor new_data_page = _manager.create_page(
                    Page::Type::DATA,
                    PageAccessor::Mode::EXCLUSIVE);
                if (page->can_insert_free_list_entry()) {
                    page->insert_free_list_entry(
                        new_data_page->get_id(),
                        new_data_page->get_remaining_space());
                } else {
                    PageAccessor new_free_list_page = _manager.create_page(
                        Page::Type::FREE_LIST,
                        PageAccessor::Mode::EXCLUSIVE);
                    new_free_list_page->insert_free_list_entry(
                        new_data_page->get_id(),
                        new_data_page->get_remaining_space());
                    page->set_next_free_list_page(new_free_list_page->get_id());
                    _manager.write_page(new_free_list_page.instance());
                }
                _manager.write_page(page.instance());

                return new_data_page;
            }
        }
    }

    bool StorageEngine::get_root_node_id(const Buffer& id, Page::ID& root_node_id) {
        Page::ID page_id = 1;
        while (page_id != Page::INVALID_ID) {
            PageAccessor page = _manager.get_page(page_id, PageAccessor::Mode::SHARED);
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
            PageAccessor page = _manager.get_page(page_id, PageAccessor::Mode::EXCLUSIVE);
            page_id = page->get_next_roots_page();
            if (page_id != Page::INVALID_ID) continue;
            PageAccessor root_page = _manager.create_page(
                Page::Type::LEAF_NODE,
                PageAccessor::Mode::SHARED);
            if (page->can_insert_root_node_id(id)) {
                page->set_root_node_id(id, root_page->get_id());
                _manager.write_page(page.instance());
                return root_page;
            }

            PageAccessor new_roots_page = _manager.create_page(
                Page::Type::ROOTS,
                PageAccessor::Mode::EXCLUSIVE);
            new_roots_page->set_root_node_id(id, root_page->get_id());
            page->set_next_roots_page(new_roots_page->get_id());
            _manager.write_page(new_roots_page.instance());
            _manager.write_page(page.instance());

            return root_page;
        }
    }

} // namespace diamond
