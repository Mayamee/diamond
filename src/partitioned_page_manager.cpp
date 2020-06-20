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

#include "diamond/exception.h"
#include "diamond/partitioned_page_manager.h"

namespace diamond {

    PartitionedPageManager::PartitionedPageManager(
            Storage& storage,
            PageWriterFactory& page_writer_factory,
            EvictionPolicyFactory& eviction_policy_factory,
            size_t num_partitions,
            size_t max_num_pages_in_partition)
            : PageManager(storage),
            _num_partitions(num_partitions) {
        for (size_t i = 0; i < _num_partitions; i++) {
            _partitions.push_back(
                std::make_unique<Partition>(
                    storage,
                    page_writer_factory.create(),
                    eviction_policy_factory.create(),
                    max_num_pages_in_partition
                ));
        }
    }

    PageAccessor PartitionedPageManager::create_page(Page::Type type) {
        Page::ID id = _next_page_id++;
        return get_partition(id)->create_page(id, type);
    }

    PageAccessor PartitionedPageManager::get_page(Page::ID id) {
        return get_partition(id)->get_page(id);
    }

    void PartitionedPageManager::write_page(const Page* page) {
        get_partition(page->get_id())->write_page(page);
    }

    bool PartitionedPageManager::is_page_managed(Page::ID id) const {
        return get_partition(id)->is_page_managed(id);
    }

    PartitionedPageManager::Partition::Partition(
        Storage& storage,
        std::shared_ptr<PageWriter> page_writer,
        std::shared_ptr<EvictionPolicy> eviction_policy,
        size_t max_num_pages)
        : _storage(storage),
        _page_writer(page_writer),
        _eviction_policy(eviction_policy),
        _max_num_pages(max_num_pages) {}

    PartitionedPageManager::Partition::~Partition() {
        for (auto [_, page] : _pages) {
            delete page;
        }
    }

    PageAccessor PartitionedPageManager::Partition::create_page(Page::ID id, Page::Type type) {
        boost::lock_guard<boost::mutex> lock(_mutex);
        if (_pages.find(id) != _pages.end()) {
            throw std::logic_error("page with the provided id already exists");
        }

        Page* page = Page::new_page(id, type);
        add_page(page);
        _page_writer->write(page);

        return PageAccessor(page);
    }

    PageAccessor PartitionedPageManager::Partition::get_page(Page::ID id) {
        Page* page;
        boost::lock_guard<boost::mutex> lock(_mutex);
        if (_pages.find(id) != _pages.end()) {
            page = _pages.at(id);
            _eviction_policy->update(id);
        } else if ((page = Page::from_storage(id, _storage)) != nullptr) {
            add_page(page);
        } else {
            throw Exception(ErrorCode::PAGE_DOES_NOT_EXIST);
        }

        return PageAccessor(page);
    }

    void PartitionedPageManager::Partition::write_page(const Page* page) {
        boost::lock_guard<boost::mutex> lock(_mutex);
        if (_pages.find(page->get_id()) == _pages.end()) {
            throw std::logic_error("trying to write unmanaged page");
        }

        _page_writer->write(page);
    }

    bool PartitionedPageManager::Partition::is_page_managed(Page::ID id) const {
        boost::lock_guard<boost::mutex> lock(_mutex);
        return _pages.find(id) != _pages.end();
    }

    void PartitionedPageManager::Partition::add_page(Page* page) {
        if (_pages.size() == _max_num_pages) {
            Page::ID to_evict = _eviction_policy->evict();
            if (to_evict == Page::INVALID_ID) {
                throw Exception(ErrorCode::NO_PAGE_SPACE_AVAILABLE);
            }
            delete _pages[to_evict];
            _pages.erase(to_evict);
        }

        _pages[page->get_id()] = page;
        _eviction_policy->track(page);
    }

} // namespace diamond
