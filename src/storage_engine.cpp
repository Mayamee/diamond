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

#include "diamond/storage_engine.h"
#include "diamond/exception.h"

namespace diamond {

    StorageEngine::StorageEngine(PageManager& page_manager, PageCompare compare_func)
            : _manager(page_manager),
            _compare_func(compare_func) {
        if (_manager.storage().size() == 0) {
            _manager.create_page(PageType::LEAF_NODE);
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
        return Buffer(accessor.page()->get_data_entry(entry.data_index()).data());      
    }

    void StorageEngine::insert(const Buffer& key, const Buffer& val) {
        PageID page_id = get_leaf_page_id(key);
        PageAccessor accessor = _manager.get_page_accessor(page_id, PageAccessorMode::SHARED);
        const Page& page = accessor.page();
        if (page->can_insert_leaf_node_entry(key)) {
            // insert new data entry
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

} // namespace diamond
