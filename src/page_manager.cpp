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

    void PageManager::add_page_to_partition(std::shared_ptr<Page>& page, Partition& partition) {
        // if (_memory_usage > _options.eviction_threshold) {
        //     Page::Key key = _lru.back();
        //     std::shared_ptr<Page>& page = _pages[key];
        //     if (is_page_dirty(page)) {
        //         _stream.seekg(page->get_offset());
        //         page->write_to_stream(_stream);
        //     }
        //     _pages.erase(key);
        //     _lru.pop_back();
        // }

        partition.locks[page->get_key()] = std::make_shared<std::shared_mutex>();
        // update_last_used(page);
        partition.pages[page->get_key()] = page;
    }

    std::shared_ptr<Page> PageManager::get_page_in_partition(Page::Key key, Partition& partition) {
        if (partition.pages.find(key) != partition.pages.end()) {
            return partition.pages.at(key);
        }
        return nullptr;
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
        std::shared_ptr<std::shared_mutex>
    >
    PageManager::get_page(Page::Key key) {
        std::shared_ptr<Page> page;
        Partition& partition = get_partition(key);
        {
            std::shared_lock<std::shared_mutex> lock(partition.mutex);
            if (page = get_page_in_partition(key, partition)) {
                return { page, partition.locks.at(key) };
            }
        }

        page = load_page(key);
        if (page) {
            std::unique_lock<std::shared_mutex> lock(partition.mutex);
            add_page_to_partition(page, partition);
            return { page, partition.locks.at(key) };
        }

        throw Exception(Exception::Reason::NO_SUCH_PAGE);
    }

    void PageManager::background_writer_task() {
        while (true) {
            std::this_thread::sleep_for(
                std::chrono::milliseconds(_options.background_writer_delay));
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
        std::shared_ptr<std::shared_mutex> mutex)
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
        std::shared_ptr<std::shared_mutex> mutex)
        : _page(page),
        _mutex(mutex) {
        _mutex->lock_shared();
    }

} // namespace diamond
