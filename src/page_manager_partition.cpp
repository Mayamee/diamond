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

    ExclusivePageAccessor PageManagerPartition::get_exclusive_accessor(Page::ID id) {
        auto [page, mutex] = get_page(id);
        return ExclusivePageAccessor(page, mutex);
    }

    SharedPageAccessor PageManagerPartition::get_shared_accessor(Page::ID id) {
        auto [page, mutex] = get_page(id);
        return SharedPageAccessor(page, mutex);
    }

    void PageManagerPartition::write_page(const std::shared_ptr<Page>& page) {
        {
            boost::shared_lock<boost::shared_mutex> lock(_mutex);
            if (_pages.find(page->get_id()) == _pages.end()) {
                throw std::logic_error("trying to write unmanaged page");
            }
        }
        _page_writer->write(page);
    }

    bool PageManagerPartition::is_page_managed(Page::ID id) {
        boost::shared_lock<boost::shared_mutex> lock(_mutex);
        return _pages.find(id) != _pages.end();
    }

    std::tuple<
        std::shared_ptr<Page>,
        std::shared_ptr<boost::shared_mutex>
    >
    PageManagerPartition::get_page(Page::ID id) {
        std::shared_ptr<Page> page;
        {
            boost::unique_lock<boost::shared_mutex> lock(_mutex);
            if (_pages.find(id) != _pages.end()) {
                ManagedPage& managed_page = _pages.at(id);
                _eviction_strategy->use(id);
                return { managed_page.page, managed_page.mutex };
            }
        }

        page = Page::new_page_from_storage(id, _storage);
        if (page) {
            boost::unique_lock<boost::shared_mutex> lock(_mutex);
            _pages.emplace(page->get_id(), page);
            _eviction_strategy->track(id);
            ManagedPage& managed_page = _pages.at(id);
            return { managed_page.page, managed_page.mutex };
        }

        throw Exception(Exception::Reason::NO_SUCH_PAGE);
    }

    PageManagerPartition::ManagedPage::ManagedPage(std::shared_ptr<Page> _page)
        : page(_page) {}

} // namespace diamond
