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

    void StorageEngine::get(const Buffer& key, Buffer& val) {
        Page::Key page_key = Page::make_key(Page::NODE, 0);
        Page::Key data_page_key;
        size_t data_entry_index;
        while (true) {
            PageManager::SharedAccessor accessor = _manager.get_shared_accessor(page_key);
            const std::shared_ptr<const Page>& page = accessor.page();
            const Page::NodeEntry& entry = page->search_node_entries(key);
            if (page->is_leaf_node()) {
                data_page_key = entry.next_page_key();
                data_entry_index = entry.next_data_index();
                break;
            } else {
                page_key = entry.next_page_key();
            }
        }

        PageManager::SharedAccessor accessor = _manager.get_shared_accessor(data_page_key);
        val = accessor.page()->get_data_entry(data_entry_index);
    }

    void StorageEngine::insert(const Buffer& key, const Buffer& val) {}

} // namespace diamond
