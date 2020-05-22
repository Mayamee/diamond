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

#include "diamond/page_manager.h"
#include "diamond/bg_page_writer.h"
#include "diamond/exception.h"

namespace diamond {

    PageManager::PageManager(std::iostream& stream, PageWriterFactory& page_writer_factory)
        : _stream(stream),
        _evictions(0) {
        for (size_t i = 0; i < NUM_PARTITIONS; i++) {
            _partitions[i].page_writer = page_writer_factory.create(stream);
        }
    }

    PageManager::ExclusiveAccessor PageManager::get_exclusive_accessor(Page::ID id) {
        auto [ page, mutex ] = get_page(id);
        return ExclusiveAccessor(page, mutex);
    }

    PageManager::SharedAccessor PageManager::get_shared_accessor(Page::ID id) {
        auto [ page, mutex ] = get_page(id);
        return SharedAccessor(page, mutex);
    }

    void PageManager::write_page(const std::shared_ptr<Page>& page) {
        Page::ID id = page->get_id();
        Partition& partition = get_partition(id);
        boost::upgrade_lock<boost::shared_mutex> lock(partition.mutex);
        if (partition.pages.find(id) != partition.pages.end()) {
            partition.page_writer->write(page);
        } else {
            boost::upgrade_to_unique_lock<boost::shared_mutex> upgraded_lock(lock);
            partition.pages.emplace(id, page);
            partition.page_order.push_back(page->get_id());
            partition.page_writer->write(page);
        }
    }

    void PageManager::write_pages(const std::vector<std::shared_ptr<Page>>& pages) {
        for (const std::shared_ptr<Page>& page : pages) {
            write_page(page);
        }
    }

    bool PageManager::is_page_managed(Page::ID id) {
        Partition& partition = get_partition(id);
        boost::shared_lock<boost::shared_mutex> lock(partition.mutex);
        return partition.pages.find(id) != partition.pages.end();
    }

    size_t PageManager::evictions() const {
        return _evictions;
    }

    PageManager::Partition& PageManager::get_partition(Page::ID id) {
        return _partitions.at(id % NUM_PARTITIONS);
    }

    std::tuple<
        std::shared_ptr<Page>,
        std::shared_ptr<boost::shared_mutex>
    >
    PageManager::get_page(Page::ID id) {
        std::shared_ptr<Page> page;
        Partition& partition = get_partition(id);
        {
            boost::shared_lock<boost::shared_mutex> lock(partition.mutex);
            if (partition.pages.find(id) != partition.pages.end()) {
                Partition::PageInfo& info = partition.pages.at(id);
                info.marked.store(true, std::memory_order::memory_order_release);
                return { info.page, info.mutex };
            }
        }

        page = Page::new_page_from_stream(id, _stream);
        if (page) {
            boost::unique_lock<boost::shared_mutex> lock(partition.mutex);
            partition.pages.emplace(page->get_id(), page);
            partition.page_order.push_back(page->get_id());
            Partition::PageInfo& info = partition.pages.at(id);
            info.marked.store(true);
            return { info.page, info.mutex };
        }

        throw Exception(Exception::Reason::NO_SUCH_PAGE);
    }

    // void PageManager::background_writer_task() {
    //     bool advance = false;
    //     size_t partition_index = 0;
    //     std::list<Page::ID>::reverse_iterator iter =
    //         _partitions[partition_index].page_order.rbegin();
    //     while (true) {
    //         std::this_thread::sleep_for(
    //             std::chrono::milliseconds(_options.background_writer_delay));

    //         size_t pages_written = 0;
    //         for (size_t i = 0; i < NUM_PARTITIONS; i++) {
    //             Partition& partition = _partitions[partition_index];
    //             boost::shared_lock<boost::shared_mutex> lock(partition.mutex);
    //             if (advance) {
    //                 iter = partition.page_order.rbegin();
    //                 advance = false;
    //             }
    //             while (iter != partition.page_order.rend() &&
    //                     pages_written < _options.background_writer_max_pages) {
    //                 Page::ID id = *iter;
    //                 Partition::PageInfo& info = partition.pages.at(id);
    //                 if (info.is_dirty.load(std::memory_order::memory_order_acquire)) {
    //                     {
    //                         boost::unique_lock<boost::shared_mutex> lock(*info.mutex);
    //                         info.page->write_to_stream(_stream);
    //                     }
    //                     info.is_dirty.store(false, std::memory_order::memory_order_release);
    //                     pages_written++;
    //                 }
    //                 iter++;
    //             }
    //             if (iter == partition.page_order.rend()) {
    //                 partition_index++;
    //                 advance = true;
    //             }
    //             if (pages_written == _options.background_writer_max_pages) break;
    //         }
    //     }
    // }

    PageManager::ExclusiveAccessor::~ExclusiveAccessor() {
        if (_locked) _mutex->unlock();
    }

    const std::shared_ptr<Page>& PageManager::ExclusiveAccessor::page() const {
        return _page;
    }

    void PageManager::ExclusiveAccessor::unlock() {
        if (!_locked) return;
        _mutex->unlock();
        _locked = false;
    }

    PageManager::ExclusiveAccessor::ExclusiveAccessor(
        std::shared_ptr<Page>& page,
        std::shared_ptr<boost::shared_mutex>& mutex)
        : _page(page),
        _mutex(mutex) {
        _mutex->lock();
        _locked = true;
    }

    PageManager::SharedAccessor::~SharedAccessor() {
        if (_locked) _mutex->unlock_shared();
    }

    const std::shared_ptr<const Page>& PageManager::SharedAccessor::page() const {
        return _page;
    }

    void PageManager::SharedAccessor::unlock() {
        if (!_locked) return;
        _mutex->unlock_shared();
        _locked = false;
    }

    PageManager::SharedAccessor::SharedAccessor(
        std::shared_ptr<Page>& page,
        std::shared_ptr<boost::shared_mutex>& mutex)
        : _page(page),
        _mutex(mutex) {
        _mutex->lock_shared();
        _locked = true;
    }

    PageManager::Partition::PageInfo::PageInfo(std::shared_ptr<Page> _page)
        : page(_page),
        mutex(std::make_shared<boost::shared_mutex>()),
        marked(false) {}

} // namespace diamond
