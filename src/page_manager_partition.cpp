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
        std::shared_ptr<EvictionStrategy> eviction_strategy)
        : _storage(storage),
        _page_writer(page_writer),
        _eviction_strategy(eviction_strategy) {}

    PageAccessor PageManagerPartition::create_page(Page::ID id, Page::Type type) {
        boost::lock_guard<boost::mutex> lock(_mutex);
        if (_pages.find(id) != _pages.end()) {
            throw std::logic_error("page with the provided id already exists");
        }
        std::shared_ptr<Page> page = Page::new_page(id, type);
        _pages.emplace(id, page);
        _eviction_strategy->track(id);
        _page_writer->write(page);
        ManagedPage& managed_page = _pages.at(id);
        return PageAccessor(
            managed_page.page,
            managed_page.mutex,
            PageAccessor::EXCLUSIVE);
    }

    PageAccessor PageManagerPartition::get_page_accessor(Page::ID id, PageAccessor::Mode access_mode) {
        boost::lock_guard<boost::mutex> lock(_mutex);
        if (_pages.find(id) != _pages.end()) {
            ManagedPage& managed_page = _pages.at(id);
            _eviction_strategy->use(id);
            return PageAccessor(
                managed_page.page,
                managed_page.mutex,
                access_mode);
        }

        std::shared_ptr<Page> page = Page::new_page_from_storage(id, _storage);
        if (page) {
            _pages.emplace(page->get_id(), page);
            _eviction_strategy->track(id);
            ManagedPage& managed_page = _pages.at(id);
            return PageAccessor(
                managed_page.page,
                managed_page.mutex,
                access_mode);
        }

        throw Exception(Exception::Reason::NO_SUCH_PAGE);
    }

    void PageManagerPartition::write_page(const std::shared_ptr<Page>& page) {
        boost::lock_guard<boost::mutex> lock(_mutex);
        if (_pages.find(page->get_id()) == _pages.end()) {
            throw std::logic_error("trying to write unmanaged page");
        }
        _page_writer->write(page);
    }

    bool PageManagerPartition::is_page_managed(Page::ID id) {
        boost::lock_guard<boost::mutex> lock(_mutex);
        return _pages.find(id) != _pages.end();
    }

    PageManagerPartition::ManagedPage::ManagedPage(std::shared_ptr<Page> _page)
        : page(_page),
        mutex(std::make_shared<boost::shared_mutex>()) {}

} // namespace diamond
