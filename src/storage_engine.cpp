/*  Diamond - Embedded Relational Database
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
#include <iostream>
#include "diamond/storage_engine.h"
#include "diamond/exception.h"

namespace diamond {

    StorageEngine::StorageEngine(PageManager& page_manager, PageCompare compare_func)
            : _manager(page_manager),
            _compare_func(compare_func) {
        if (_manager.storage().size() == 0) {
            _manager.create_page(PageType::LEAF_NODE);
            _manager.create_page(PageType::FREE_LIST);
        }
    }

    Buffer StorageEngine::get(const Buffer& key) {
        PageID page_id = get_leaf_page_id(key);
        PageAccessor accessor = _manager.get_page_accessor(
            page_id,
            PageAccessorMode::SHARED);
        const Page& page = accessor.page();
        size_t i;
        if (!page->find_leaf_node_entry(key, _compare_func, i)) return Buffer();
        const LeafNodeEntry& entry = page->get_leaf_node_entry(i);
        PageAccessor data_accessor = _manager.get_page_accessor(
            entry.data_id(),
            PageAccessorMode::SHARED);
        return Buffer(data_accessor.page()->get_data_entry(entry.data_index()).data());      
    }

    void StorageEngine::insert(const Buffer& key, const Buffer& val) {
        PageID page_id = get_leaf_page_id(key);
        PageAccessor accessor = _manager.get_page_accessor(page_id, PageAccessorMode::UPGRADE);
        const Page& page = accessor.page();
        if (page->can_insert_leaf_node_entry(key)) {
            PageID data_page_id;
            size_t data_page_index;
            {
                PageAccessor data_accessor = get_free_data_page(val);
                const Page& data_page = data_accessor.page();
                data_page_id = data_page->get_id();
                data_page_index = data_page->insert_data_entry(val);
                _manager.write_page(data_page);
            }
            accessor.upgrade_lock();
            page->insert_leaf_node_entry(key, data_page_id, data_page_index);
            _manager.write_page(page);
            return;
        }
        // split here
    }

    PageID StorageEngine::get_leaf_page_id(const Buffer& key) {
        PageID page_id = 1;
        while (true) {
            PageAccessor accessor = _manager.get_page_accessor(page_id, PageAccessorMode::SHARED);
            const Page& page = accessor.page();
            PageType type = page->get_type();
            switch (type) {
            case PageType::INTERNAL_NODE: {
                size_t i = page->search_internal_node_entries(key, _compare_func);
                page_id = page->get_internal_node_entry(i).next_node_id();
                break;
            }
            case PageType::LEAF_NODE:
                return page_id;
            default:
                throw Exception(ErrorCode::CORRUPTED_FILE);
            }
        }
    }

    PageAccessor StorageEngine::get_free_data_page(const Buffer& val) {
        PageID data_page;
        PageID page_id = 2;
        while (page_id) {
            PageAccessor accessor = _manager.get_page_accessor(page_id, PageAccessorMode::EXCLUSIVE);
            const Page& page = accessor.page();
            if (page->get_type() != PageType::FREE_LIST) {
                throw Exception(ErrorCode::CORRUPTED_FILE);
            }

            if (page->reserve_free_list_entry(val, data_page)) {
                return _manager.get_page_accessor(data_page, PageAccessorMode::EXCLUSIVE);
            }

            page_id = page->get_next_free_list_page();
            if (page_id) continue;

            PageAccessor new_data_accessor = _manager.create_page(PageType::DATA);
            const Page& new_data_page = new_data_accessor.page();
            if (page->can_insert_free_list_entry()) {
                page->insert_free_list_entry(
                    new_data_page->get_id(),
                    new_data_page->get_remaining_space());
            } else {
                PageAccessor new_free_list_accessor = _manager.create_page(PageType::FREE_LIST);
                const Page& new_free_list_page = new_data_accessor.page();
                new_free_list_page->insert_free_list_entry(
                    new_data_page->get_id(),
                    new_data_page->get_remaining_space());
                page->set_next_free_list_page(new_free_list_page->get_id());
                _manager.write_page(new_free_list_page);
            }
            _manager.write_page(page);

            return new_data_accessor;
        }
    }

} // namespace diamond
