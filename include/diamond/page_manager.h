/*  Diamond - Relational Database
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

#include <fstream>
#include <list>
#include <string>
#include <thread>
#include <tuple>
#include <unordered_map>
#include <vector>

#include "diamond/page.h"

namespace diamond {

    class PageManager {
    public:
        struct Options {
            Options() = default;

            // Delay between page writing.
            size_t background_writer_delay = 200;

            // The maximum number of pages the background
            // writer will flush.
            size_t background_writer_max_pages = 100;

            // The threshold for eviction in bytes.
            size_t eviction_threshold = 1000000;
        };

        PageManager(const std::string& file_name, const Options& options);

        std::shared_ptr<Page>& get_page(Page::Key key);
        std::shared_ptr<Page>& get_root_data_page();

        void write_page(std::shared_ptr<Page> page);

        size_t memory_usage() const;
        size_t evictions() const;

    private:
        std::fstream _stream;
        Options _options;
        std::unordered_map<
            std::tuple<Page::Type, size_t>,
            std::shared_ptr<Page>,
            Page::KeyHash
        > _pages;

        std::unordered_map<
            Page::Key,
            std::list<Page::Key>::iterator,
            Page::KeyHash
        > _lru_node_map;
        std::list<Page::Key> _lru;

        std::thread _background_writer;

        size_t _memory_usage;
        size_t _evictions;

        void add_page(std::shared_ptr<Page>& page);
        void update_last_used(std::shared_ptr<Page>& page);

        void background_writer_task();
    };

} // namespace diamond

#endif // _DIAMOND_STORAGE_PAGE_MANAGER_H
