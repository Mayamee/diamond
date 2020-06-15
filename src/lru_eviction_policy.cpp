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

#include "diamond/lru_eviction_policy.h"

namespace diamond {

    void LRUEvictionPolicy::update(Page::ID id) {
        _list.splice(
            _list.begin(),
            _list,
            _iters.at(id));
    }

    void LRUEvictionPolicy::add(Page::ID id) {
        _list.push_front(id);
        _iters[id] = _list.begin();
    }

    Page::ID LRUEvictionPolicy::next(Page::ID after) {
        if (after != Page::INVALID_ID) {
            auto iter = _iters.at(after);
            if (iter != _list.end()) {
                return *iter;
            }
            return Page::INVALID_ID;
        } else {
            return *_list.begin();
        }
    }

    void LRUEvictionPolicy::remove(Page::ID id) {
        _list.erase(_iters[id]);
        _iters.erase(id);
    }

    std::shared_ptr<EvictionPolicy> LRUEvictionPolicyFactory::create() const {
        return std::make_shared<LRUEvictionPolicy>();
    }

} // namespace diamond
