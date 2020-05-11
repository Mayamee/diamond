/*  Diamond - Relational Database
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

namespace diamond {

    StorageEngine::Status StorageEngine::get(const Buffer& key, Buffer& value) {
        std::shared_ptr<Page>& root = _manager.get_root_data_page();
        return search(root, key, value);
    }

    StorageEngine::Status StorageEngine::insert(const Buffer& key, const Buffer& val) {
        return OK;
    }

    StorageEngine::Status StorageEngine::search(std::shared_ptr<Page>& page, const Buffer& key, Buffer& value) {
        if (page == nullptr) {
            return NOT_FOUND;
        }

        if (page->is_leaf_node()) {
            const Page::NodeEntry& entry = page->search_node_entries(key);
            value = _manager.get_page(entry.next_page_key())->get_data_entry(entry.next_data_index());
            return OK;
        }

        const Page::NodeEntry& entry = page->search_node_entries(key);
        return search(_manager.get_page(entry.next_page_key()), key, value);
    }

} // namespace diamond
