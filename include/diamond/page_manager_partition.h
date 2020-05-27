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

#ifndef _DIAMOND_PAGE_MANAGER_PARTITION_H
#define _DIAMOND_PAGE_MANAGER_PARTITION_H

#include <unordered_map>

#include <boost/thread.hpp>
#include <boost/utility.hpp>

#include "diamond/eviction_strategy.h"
#include "diamond/page_accessors.h"
#include "diamond/page_writer.h"

namespace diamond {

    class PageManagerPartition : boost::noncopyable {
    public:
        PageManagerPartition(
            Storage& storage,
            std::shared_ptr<PageWriter> page_writer,
            std::shared_ptr<EvictionStrategy> eviction_strategy);

        ExclusivePageAccessor get_exclusive_accessor(Page::ID id);
        SharedPageAccessor get_shared_accessor(Page::ID id);

        void write_page(const std::shared_ptr<Page>& page);

        bool is_page_managed(Page::ID id);

    private:
        Storage& _storage;
        std::shared_ptr<PageWriter> _page_writer;
        std::shared_ptr<EvictionStrategy> _eviction_strategy;

        struct ManagedPage {
            ManagedPage(std::shared_ptr<Page> _page);

            std::shared_ptr<Page> page;
            std::shared_ptr<boost::shared_mutex> mutex;
        };

        std::unordered_map<
            Page::ID,
            ManagedPage
        > _pages;

        boost::shared_mutex _mutex;

        std::tuple<
            std::shared_ptr<Page>,
            std::shared_ptr<boost::shared_mutex>
        >
        get_page(Page::ID id);
    };

} // namespace diamond

#endif // _DIAMOND_PAGE_MANAGER_PARTITION_H
