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

#ifndef _DIAMOND_EVICTION_STRATEGY_H
#define _DIAMOND_EVICTION_STRATEGY_H

#include <exception>
#include <unordered_map>

#include "diamond/page.h"

namespace diamond {

    class EvictionStrategy {
    public:
        PageID evict();
        virtual void update(PageID id) = 0;
        void track(const Page& page);

    protected:
        virtual void add(PageID id) = 0;
        virtual PageID next(PageID after = INVALID_PAGE) = 0;
        virtual void remove(PageID id) = 0;

    private:
        std::unordered_map<
            PageID,
            Page
        > _tracked_pages;
    };

    class EvictionStrategyFactory {
    public:
        virtual std::shared_ptr<EvictionStrategy> create() const = 0;
    };

} // namespace diamond

#endif // _DIAMOND_EVICTION_STRATEGY_H
