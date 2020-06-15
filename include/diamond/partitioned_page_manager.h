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

#ifndef _DIAMOND_STORAGE_PARTITIONED_PAGE_MANAGER_H
#define _DIAMOND_STORAGE_PARTITIONED_PAGE_MANAGER_H

#include <tuple>
#include <unordered_map>
#include <vector>

#include <boost/thread.hpp>

#include "diamond/eviction_policy.h"
#include "diamond/page_manager.h"
#include "diamond/page_writer.h"
#include "diamond/storage.h"

namespace diamond {

    class PartitionedPageManager final : public PageManager {
    public:
        static const size_t DEFAULT_NUM_PARTITIONS = 128;
        static const size_t MAX_NUM_PAGES_IN_PARTITION = 100;

        PartitionedPageManager(
            Storage& storage,
            PageWriterFactory& page_writer_factory,
            EvictionPolicyFactory& eviction_policy_factory,
            size_t num_partitions = DEFAULT_NUM_PARTITIONS,
            size_t max_num_pages_in_partition = MAX_NUM_PAGES_IN_PARTITION);

        PageAccessor create_page(Page::Type type, PageAccessor::Mode access_mode) override;
        PageAccessor get_page(Page::ID id, PageAccessor::Mode access_mode) override;
        void write_page(const Page* page) override;
        bool is_page_managed(Page::ID id) const override;

    private:
        size_t _num_partitions;

        class Partition : boost::noncopyable {
        public:
            Partition(
                Storage& storage,
                std::shared_ptr<PageWriter> page_writer,
                std::shared_ptr<EvictionPolicy> eviction_policy,
                size_t max_num_pages);
            ~Partition();

            PageAccessor create_page(
                Page::ID id,
                Page::Type type,
                PageAccessor::Mode access_mode = PageAccessor::Mode::EXCLUSIVE);

            PageAccessor get_page(Page::ID id, PageAccessor::Mode access_mode);

            void write_page(const Page* page);

            bool is_page_managed(Page::ID id) const;

        private:
            Storage& _storage;
            std::shared_ptr<PageWriter> _page_writer;
            std::shared_ptr<EvictionPolicy> _eviction_policy;
            size_t _max_num_pages;

            std::unordered_map<
                Page::ID,
                Page*
            > _pages;

            mutable boost::mutex _mutex;

            void add_page(Page* page);
        };

        std::vector<std::unique_ptr<Partition>> _partitions;

        const std::unique_ptr<Partition>& get_partition(Page::ID id) const {
            return _partitions.at(id % _num_partitions);
        }

        std::unique_ptr<Partition>& get_partition(Page::ID id) {
            return _partitions.at(id % _num_partitions);
        }
    };

} // namespace diamond

#endif // _DIAMOND_STORAGE_PARTITIONED_PAGE_MANAGER_H
