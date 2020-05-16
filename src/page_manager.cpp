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
#include "diamond/exception.h"

namespace diamond {

    PageManager::PageManager(std::iostream& stream, const Options& options)
        : _stream(stream),
        _options(options),
        _background_writer(&PageManager::background_writer_task, this),
        _evictions(0) {}

    PageManager::ExclusiveAccessor PageManager::get_exclusive_accessor(Page::Key key) {
        auto [ page, mutex ] = get_page(key);
        return ExclusiveAccessor(page, mutex);
    }

    PageManager::SharedAccessor PageManager::get_shared_accessor(Page::Key key) {
        auto [ page, mutex ] = get_page(key);
        return SharedAccessor(page, mutex);
    }

    size_t PageManager::evictions() const {
        return _evictions;
    }

    PageManager::Partition& PageManager::get_partition(Page::Key key) {
        return _partitions.at(Page::KeyHash()(key) % NUM_PARTITIONS);
    }

    std::shared_ptr<Page> PageManager::load_page(Page::Key key) {
        std::shared_ptr<Page> page;
        Page::Type type = std::get<0>(key);
        size_t id = std::get<1>(key);
        switch (type) {
        case Page::Type::DATA:
        case Page::Type::NODE: {
            size_t n = 0;
            size_t oid = 0;
            Page::Type offsets_type = Page::get_offsets_type(type);
            while (true) {
                size_t offset;
                {
                    SharedAccessor accessor = get_shared_accessor(Page::make_key(offsets_type, oid));
                    const std::shared_ptr<const Page>& offsets = accessor.page();
                    n += offsets->get_num_offsets();
                    if (id > n) { 
                        oid++;
                        continue;
                    }
                    offset = offsets->get_offset(id % offsets->get_num_offsets());
                }
                _stream.seekg(offset);
                page = Page::new_page_from_stream(_stream);
                break;
            }
            break;
        }
        case Page::Type::DATA_OFFSETS:
        case Page::Type::NODE_OFFSETS: {
            size_t pid = 0;
            size_t offset;
            while (pid < id) {
                _stream.seekg(offset);
                {
                    SharedAccessor accessor = get_shared_accessor(Page::make_key(type, pid));
                    offset = accessor.page()->get_next_offsets();
                }
                pid++;
            }
            _stream.seekg(offset);
            page = Page::new_page_from_stream(_stream);
            break;
        }
        default:
            break;
        }

        return page;
    }

    std::tuple<
        std::shared_ptr<Page>,
        std::shared_ptr<boost::shared_mutex>
    >
    PageManager::get_page(Page::Key key) {
        std::shared_ptr<Page> page;
        Partition& partition = get_partition(key);
        {
            boost::shared_lock<boost::shared_mutex> lock(partition.mutex);
            if (partition.pages.find(key) != partition.pages.end()) {
                Partition::PageInfo& info = partition.pages.at(key);
                info.marked.store(true, std::memory_order::memory_order_release);
                return { info.page, info.mutex };
            }
        }

        page = load_page(key);
        if (page) {
            boost::unique_lock<boost::shared_mutex> lock(partition.mutex);
            partition.pages.emplace(page->get_key(), page);
            partition.page_order.push_back(page->get_key());
            Partition::PageInfo& info = partition.pages.at(key);
            info.marked.store(true, std::memory_order::memory_order_release);
            return { info.page, info.mutex };
        }

        throw Exception(Exception::Reason::NO_SUCH_PAGE);
    }

    void PageManager::background_writer_task() {
        bool advance = false;
        size_t partition_index = 0;
        std::list<Page::Key>::reverse_iterator iter =
            _partitions[partition_index].page_order.rbegin();
        while (true) {
            std::this_thread::sleep_for(
                std::chrono::milliseconds(_options.background_writer_delay));

            size_t pages_written = 0;
            for (size_t i = 0; i < NUM_PARTITIONS; i++) {
                Partition& partition = _partitions[partition_index];
                boost::shared_lock<boost::shared_mutex> lock(partition.mutex);
                if (advance) {
                    iter = partition.page_order.rbegin();
                    advance = false;
                }
                while (iter != partition.page_order.rend() &&
                        pages_written < _options.background_writer_max_pages) {
                    Page::Key key = *iter;
                    Partition::PageInfo& info = partition.pages.at(key);
                    if (info.is_dirty.load(std::memory_order::memory_order_acquire)) {
                        {
                            boost::unique_lock<boost::shared_mutex> lock(*info.mutex);
                            info.page->write_to_stream(_stream);
                        }
                        info.is_dirty.store(false, std::memory_order::memory_order_release);
                        pages_written++;
                    }
                    iter++;
                }
                if (iter == partition.page_order.rend()) {
                    partition_index++;
                    advance = true;
                }
                if (pages_written == _options.background_writer_max_pages) break;
            }
        }
    }

    PageManager::ExclusiveAccessor::~ExclusiveAccessor() {
        _mutex->unlock();
    }

    const std::shared_ptr<Page>& PageManager::ExclusiveAccessor::page() const {
        return _page;
    }

    PageManager::ExclusiveAccessor::ExclusiveAccessor(
        std::shared_ptr<Page> page,
        std::shared_ptr<boost::shared_mutex> mutex)
        : _page(page),
        _mutex(mutex) {
        _mutex->lock();
    }

    PageManager::SharedAccessor::~SharedAccessor() {
        _mutex->unlock_shared();
    }

    const std::shared_ptr<const Page>& PageManager::SharedAccessor::page() const {
        return _page;
    }

    PageManager::SharedAccessor::SharedAccessor(
        std::shared_ptr<const Page> page,
        std::shared_ptr<boost::shared_mutex> mutex)
        : _page(page),
        _mutex(mutex) {
        _mutex->lock_shared();
    }

    PageManager::Partition::PageInfo::PageInfo(std::shared_ptr<Page> _page)
        : page(_page),
        mutex(std::make_shared<boost::shared_mutex>()),
        marked(false),
        is_dirty(false) {}

} // namespace diamond
