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

#ifndef _DIAMOND_STORAGE_PAGE_MANAGER_H
#define _DIAMOND_STORAGE_PAGE_MANAGER_H

#include <array>
#include <atomic>
#include <iostream>
#include <list>
#include <tuple>
#include <unordered_map>

#include <boost/thread.hpp>

#include "diamond/page.h"
#include "diamond/page_writer.h"

namespace diamond {

    class PageManager {
    public:
        static const uint8_t NUM_PARTITIONS = 128;

        class ExclusiveAccessor {
        public:
            ~ExclusiveAccessor();

            const std::shared_ptr<Page>& page() const;

            void unlock();

        private:
            bool _locked;
            std::shared_ptr<Page> _page;
            std::shared_ptr<boost::shared_mutex> _mutex;

            friend class PageManager;

            ExclusiveAccessor(
                std::shared_ptr<Page>& page,
                std::shared_ptr<boost::shared_mutex>& mutex);
        };

        class SharedAccessor {
        public:
            ~SharedAccessor();

            const std::shared_ptr<const Page>& page() const;

            void unlock();

        private:
            bool _locked;
            std::shared_ptr<const Page> _page;
            std::shared_ptr<boost::shared_mutex> _mutex;

            friend class PageManager;

            SharedAccessor(
                std::shared_ptr<Page>& page,
                std::shared_ptr<boost::shared_mutex>& mutex);
        };

        PageManager(std::iostream& stream, PageWriterFactory& page_writer_factory);

        ExclusiveAccessor get_exclusive_accessor(Page::ID id);
        SharedAccessor get_shared_accessor(Page::ID id);

        void write_page(const std::shared_ptr<Page>& page);
        void write_pages(const std::vector<std::shared_ptr<Page>>& pages);

        bool is_page_managed(Page::ID id);

        size_t evictions() const;

    private:
        std::iostream& _stream;

        struct Partition {
            struct PageInfo {
                PageInfo(std::shared_ptr<Page> _page);

                std::shared_ptr<Page> page;
                std::shared_ptr<boost::shared_mutex> mutex;
                std::atomic<bool> marked;
            };

            boost::shared_mutex mutex;
            std::list<Page::ID> page_order;
            std::unordered_map<Page::ID, PageInfo> pages;
            std::shared_ptr<PageWriter> page_writer;
        };

        std::array<Partition, NUM_PARTITIONS> _partitions;

        size_t _evictions;

        Partition& get_partition(Page::ID id);

        std::tuple<
            std::shared_ptr<Page>,
            std::shared_ptr<boost::shared_mutex>
        >
        get_page(Page::ID id);
    };

} // namespace diamond

#endif // _DIAMOND_STORAGE_PAGE_MANAGER_H
