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

#include <cstring>

#include "diamond/storage_engine.h"
#include "diamond/exception.h"

namespace diamond {

    StorageEngine::StorageEngine(std::iostream& data_stream, const Options& options)
        : _options(options), 
        _manager(data_stream, options.page_manager_options) {
        if (data_stream.rdbuf()->in_avail() == 0) {
            _manager.write_page(Page::new_leaf_node_page(1));
        }
    }

    Buffer StorageEngine::get(const Buffer& key) {
        Page::ID page_id = 1;
        while (true) {
            PageManager::SharedAccessor accessor = _manager.get_shared_accessor(page_id);
            const std::shared_ptr<const Page>& page = accessor.page();
            Page::Type type = page->get_type();
            switch (type) {
            case Page::INTERNAL_NODE: {
                size_t i = page->search_internal_node_entries(key, _options.compare_func);
                const Page::InternalNodeEntry& entry = page->get_internal_node_entry(i);
                page_id = entry.next_node_id();
                break;
            }
            case Page::LEAF_NODE: {
                size_t i;
                if (!page->find_leaf_node_entry(key, _options.compare_func, i)) return Buffer();
                const Page::LeafNodeEntry& entry = page->get_leaf_node_entry(i);
                Page::ID data_page_id = entry.data_id();
                size_t data_entry_index = entry.data_index();
                accessor.unlock();
                PageManager::SharedAccessor data_accessor = _manager.get_shared_accessor(data_page_id);
                return Buffer(accessor.page()->get_data_entry(data_entry_index).data());
            }
            default:
                throw Exception(Exception::Reason::CORRUPTED_FILE);
            }
        }        
    }

    void StorageEngine::insert(const Buffer& key, const Buffer& val) {}

} // namespace diamond
