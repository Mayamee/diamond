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

#ifndef _DIAMOND_LRU_EVICTION_POLICY_H
#define _DIAMOND_LRU_EVICTION_POLICY_H

#include <list>
#include <unordered_map>

#include "diamond/eviction_policy.h"

namespace diamond {

    class LRUEvictionPolicy final : public EvictionPolicy {
    public:
        void update(Page::ID id) override;

    private:
        std::list<Page::ID> _list;
        std::unordered_map<
            Page::ID,
            std::list<Page::ID>::iterator
        > _iters;

        void add(Page::ID id) override;
        Page::ID next(Page::ID after = 0) override;
        void remove(Page::ID id) override;
    };

    class LRUEvictionPolicyFactory final : public EvictionPolicyFactory {
    public:
        std::shared_ptr<EvictionPolicy> create() const override;
    };

} // namespace diamond

#endif // _DIAMOND_LRU_EVICTION_POLICY_H
