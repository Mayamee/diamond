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

#include "diamond/exception.h"
#include "diamond/storage_engine.h"

namespace diamond {

    StorageEngine::StorageEngine(std::iostream& data_stream, const Options& options)
        : _manager(data_stream, options.page_manager_options) {
        if (data_stream.rdbuf()->in_avail() == 0) {
            _manager.write_page(Page::new_leaf_node_page(0));
        }
    }

    void StorageEngine::get(const Buffer& key, Buffer& val) {
        Page::ID page_id = 0;
        // Page::Key data_page_key;
        // size_t data_entry_index;
        while (true) {
            PageManager::SharedAccessor accessor = _manager.get_shared_accessor(page_id);
            const std::shared_ptr<const Page>& page = accessor.page();
            Page::Type type = page->get_type();
            if (type == Page::INTERNAL_NODE) {

            } else if (type == Page::LEAF_NODE) {

            } else { /* TODO: Add corruption exception type */ }
        }

        // PageManager::SharedAccessor accessor = _manager.get_shared_accessor(data_page_key);
        // val = accessor.page()->get_data_entry(data_entry_index);
    }

    void StorageEngine::insert(const Buffer& key, const Buffer& val) {}

} // namespace diamond
