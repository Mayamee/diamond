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
#include <ctime>
#include <iostream>
#include <list>
#include <thread>
#include <tuple>
#include <unordered_map>

#include <boost/thread.hpp>

#include "diamond/page.h"

namespace diamond {

    class PageManager {
    public:
        struct Options {
            Options() = default;

            // Delay between page writing.
            uint32_t background_writer_delay = 200;

            // The maximum number of pages the background
            // writer will flush.
            uint32_t background_writer_max_pages = 100;

            // The memory usage at which the manager will
            // begin to evict pages.
            float eviction_max_memory_usage = 95;
        };

        class ExclusiveAccessor {
        public:
            ~ExclusiveAccessor();

            const std::shared_ptr<Page>& page() const;

        private:
            std::shared_ptr<Page> _page;
            std::shared_ptr<boost::shared_mutex> _mutex;

            friend class PageManager;

            ExclusiveAccessor(
                std::shared_ptr<Page> page,
                std::shared_ptr<boost::shared_mutex> mutex);
        };

        class SharedAccessor {
        public:
            ~SharedAccessor();

            const std::shared_ptr<const Page>& page() const;

        private:
            std::shared_ptr<const Page> _page;
            std::shared_ptr<boost::shared_mutex> _mutex;

            friend class PageManager;

            SharedAccessor(
                std::shared_ptr<const Page> page,
                std::shared_ptr<boost::shared_mutex> mutex);
        };

        PageManager(std::iostream& stream, const Options& options);

        ExclusiveAccessor get_exclusive_accessor(Page::Key key);
        SharedAccessor get_shared_accessor(Page::Key key);

        size_t evictions() const;

    private:
        static const uint8_t NUM_PARTITIONS = 128;

        std::iostream& _stream;
        Options _options;

        struct Partition {
            Partition() = default;

            boost::shared_mutex mutex;

            struct PageInfo {
                PageInfo(std::shared_ptr<Page> _page);

                std::shared_ptr<Page> page;
                std::shared_ptr<boost::shared_mutex> mutex;
                std::atomic<bool> marked;
                std::atomic<bool> is_dirty;
            };

            std::list<Page::Key> page_order;
            std::unordered_map<
                Page::Key,
                PageInfo,
                Page::KeyHash,
                Page::KeyEqual
            > pages;
        };

        std::array<Partition, NUM_PARTITIONS> _partitions;

        std::thread _background_writer;

        size_t _evictions;

        Partition& get_partition(Page::Key key);

        std::shared_ptr<Page> load_page(Page::Key key);

        std::tuple<
            std::shared_ptr<Page>,
            std::shared_ptr<boost::shared_mutex>
        >
        get_page(Page::Key key);

        void background_writer_task();
    };

} // namespace diamond

#endif // _DIAMOND_STORAGE_PAGE_MANAGER_H
