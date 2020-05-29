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

#include "diamond/lru_eviction_strategy.h"

namespace diamond {

    void LRUEvictionStrategy::update(PageID id) {
        _list.splice(
            _list.begin(),
            _list,
            _iters.at(id));
    }

    void LRUEvictionStrategy::add(PageID id) {
        _list.push_front(id);
        _iters[id] = _list.begin();
    }

    PageID LRUEvictionStrategy::next(PageID after) {
        if (after != INVALID_PAGE) {
            auto iter = _iters.at(after);
            if (iter != _list.end()) {
                return *iter;
            }
            return INVALID_PAGE;
        } else {
            return *_list.begin();
        }
    }

    void LRUEvictionStrategy::remove(PageID id) {
        _list.erase(_iters[id]);
        _iters.erase(id);
    }

    std::shared_ptr<EvictionStrategy> LRUEvictionStrategyFactory::create() const {
        return std::make_shared<LRUEvictionStrategy>();
    }

} // namespace diamond
