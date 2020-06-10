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

#include "diamond/eviction_strategy.h"

namespace diamond {

    Page::ID EvictionStrategy::evict() {
        Page::ID to_evict = next();
        while (to_evict != Page::INVALID_ID) {
            if (_tracked_pages.at(to_evict)->usage_count() == 0) {
                _tracked_pages.erase(to_evict);
                remove(to_evict);
                return to_evict;
            }

            to_evict = next(to_evict);
        }

        return to_evict;
    }

    void EvictionStrategy::track(const Page* page) {
        Page::ID id = page->get_id();
        if (_tracked_pages.find(id) != _tracked_pages.end()) {
            throw std::logic_error("already tracking this page");
        }
        _tracked_pages.emplace(id, page);
        add(id);
    }

} // namespace diamond
