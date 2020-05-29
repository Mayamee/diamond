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

#include <boost/thread.hpp>
#include <boost/utility.hpp>

#include "diamond/eviction_strategy.h"
#include "diamond/page_accessor.h"
#include "diamond/page_writer.h"

namespace diamond {

    class PageManagerPartition : boost::noncopyable {
    public:
        PageManagerPartition(
            Storage& storage,
            std::shared_ptr<PageWriter> page_writer,
            std::shared_ptr<EvictionStrategy> eviction_strategy,
            size_t max_num_pages);

        PageAccessor create_page(PageID id, PageType type);

        PageAccessor get_page_accessor(PageID id, PageAccessorMode access_mode);

        void write_page(const Page& page);

        bool is_page_managed(PageID id);

    private:
        Storage& _storage;
        std::shared_ptr<PageWriter> _page_writer;
        std::shared_ptr<EvictionStrategy> _eviction_strategy;
        size_t _max_num_pages;

        struct ManagedPage {
            ManagedPage(const Page& _page);

            Page page;
            std::shared_ptr<boost::shared_mutex> mutex;
        };

        std::unordered_map<
            PageID,
            ManagedPage
        > _pages;

        boost::mutex _mutex;

        void add_page(const Page& page);
    };

} // namespace diamond

#endif // _DIAMOND_PAGE_MANAGER_PARTITION_H
