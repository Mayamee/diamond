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

#include "diamond/page_manager_partition.h"
#include "diamond/exception.h"

namespace diamond {

    PageManagerPartition::PageManagerPartition(
        Storage& storage,
        std::shared_ptr<PageWriter> page_writer,
        std::shared_ptr<EvictionStrategy> eviction_strategy,
        size_t max_num_pages)
        : _storage(storage),
        _page_writer(page_writer),
        _eviction_strategy(eviction_strategy),
        _max_num_pages(max_num_pages) {}

    PageAccessor PageManagerPartition::create_page(PageID id, PageType type) {
        boost::lock_guard<boost::mutex> lock(_mutex);
        if (_pages.find(id) != _pages.end()) {
            throw std::logic_error("page with the provided id already exists");
        }

        Page page(id, type);
        add_page(page);
        _page_writer->write(page);
        ManagedPage& managed_page = _pages.at(id);
        return PageAccessor(
            managed_page.page,
            managed_page.mutex,
            PageAccessorMode::EXCLUSIVE);
    }

    PageAccessor PageManagerPartition::get_page_accessor(PageID id, PageAccessorMode access_mode) {
        boost::lock_guard<boost::mutex> lock(_mutex);
        if (_pages.find(id) != _pages.end()) {
            ManagedPage& managed_page = _pages.at(id);
            _eviction_strategy->update(id);
            return PageAccessor(
                managed_page.page,
                managed_page.mutex,
                access_mode);
        }

        Page page = Page::from_storage(id, _storage);
        if (page) {
            add_page(page);
            ManagedPage& managed_page = _pages.at(id);
            return PageAccessor(
                managed_page.page,
                managed_page.mutex,
                access_mode);
        }

        throw Exception(ErrorCode::PAGE_DOES_NOT_EXIST);
    }

    void PageManagerPartition::write_page(const Page& page) {
        boost::lock_guard<boost::mutex> lock(_mutex);
        if (_pages.find(page->get_id()) == _pages.end()) {
            throw std::logic_error("trying to write unmanaged page");
        }

        _page_writer->write(page);
    }

    bool PageManagerPartition::is_page_managed(PageID id) {
        boost::lock_guard<boost::mutex> lock(_mutex);
        return _pages.find(id) != _pages.end();
    }

    void PageManagerPartition::add_page(const Page& page) {
        if (_pages.size() == _max_num_pages) {
            PageID to_evict = _eviction_strategy->evict();
            if (to_evict == INVALID_PAGE) {
                throw Exception(ErrorCode::NO_PAGE_SPACE_AVAILABLE);
            }
            _pages.erase(to_evict);
        }

        _pages.emplace(page->get_id(), page);
        _eviction_strategy->track(page);
    }

    PageManagerPartition::ManagedPage::ManagedPage(const Page& _page)
        : page(_page),
        mutex(std::make_shared<boost::shared_mutex>()) {}

} // namespace diamond
