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

#ifndef _DIAMOND_LRU_EVICTION_STRATEGY_H
#define _DIAMOND_LRU_EVICTION_STRATEGY_H

#include <list>
#include <unordered_map>

#include "diamond/eviction_strategy.h"

namespace diamond {

    class LRUEvictionStrategy final : public EvictionStrategy {
    public:
        void update(PageID id) override;

    private:
        std::list<PageID> _list;
        std::unordered_map<
            PageID,
            std::list<PageID>::iterator
        > _iters;

        void add(PageID id) override;
        PageID next(PageID after = 0) override;
        void remove(PageID id) override;
    };

    class LRUEvictionStrategyFactory final : public EvictionStrategyFactory {
    public:
        std::shared_ptr<EvictionStrategy> create() const override;
    };

} // namespace diamond

#endif // _DIAMOND_LRU_EVICTION_STRATEGY_H
