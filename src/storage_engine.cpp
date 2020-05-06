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

    void* StorageEngine::get(const Key& key) {
        std::shared_ptr<Page>& root = _manager.get_root_data_page();
        return search(root, key);
    }

    void* StorageEngine::search(std::shared_ptr<Page>& page, const Key& key) {
        if (page == nullptr) {
            return nullptr;
        }

        if (page->is_leaf_node()) {
            // const Page::NodeEntry& entry = page->search_node_entries(key.val(), key.size());
            // return _manager.get_page(entry.next())->
            return nullptr;
        }

        const Page::NodeEntry& entry = page->search_node_entries(key.val(), key.size());
        return search(_manager.get_page(entry.next()), key);
    }

} // namespace diamond
