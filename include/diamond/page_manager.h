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

#include <tuple>
#include <unordered_map>
#include <vector>

#include <boost/thread.hpp>
#include <boost/utility.hpp>

#include "diamond/page_manager_partition.h"

namespace diamond {

    class PageManager : boost::noncopyable {
    public:
        static const size_t DEFAULT_NUM_PARTITIONS = 128;

        PageManager(
            Storage& storage,
            PageWriterFactory& page_writer_factory,
            EvictionStrategyFactory& eviction_strategy_factory,
            size_t num_partitions = DEFAULT_NUM_PARTITIONS);

        ExclusivePageAccessor create_page(Page::Type type);

        ExclusivePageAccessor get_exclusive_accessor(Page::ID id);
        SharedPageAccessor get_shared_accessor(Page::ID id);

        void write_page(const std::shared_ptr<Page>& page);
        void write_pages(const std::vector<std::shared_ptr<Page>>& pages);

        bool is_page_managed(Page::ID id);

        Storage& storage();
        const Storage& storage() const;

    private:
        Storage& _storage;
        size_t _num_partitions;

        std::vector<std::unique_ptr<PageManagerPartition>> _partitions;

        std::unique_ptr<PageManagerPartition>& get_partition(Page::ID id) {
            return _partitions.at(id % _num_partitions);
        }
    };

} // namespace diamond

#endif // _DIAMOND_STORAGE_PAGE_MANAGER_H
