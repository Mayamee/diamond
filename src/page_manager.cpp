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

namespace diamond {

    PageManager::PageManager(
            Storage& storage,
            PageWriterFactory& page_writer_factory,
            EvictionStrategyFactory& eviction_strategy_factory,
            size_t num_partitions)
            : _storage(storage),
            _num_partitions(num_partitions) {
        for (size_t i = 0; i < _num_partitions; i++) {
            _partitions.push_back(
                std::make_unique<PageManagerPartition>(
                    storage,
                    page_writer_factory.create(storage),
                    eviction_strategy_factory.create()
                ));
        }
    }

    ExclusivePageAccessor PageManager::get_exclusive_accessor(Page::ID id) {
        return get_partition(id)->get_exclusive_accessor(id);
    }

    SharedPageAccessor PageManager::get_shared_accessor(Page::ID id) {
        return get_partition(id)->get_shared_accessor(id);
    }

    void PageManager::write_page(const std::shared_ptr<Page>& page) {
        get_partition(page->get_id())->write_page(page);
    }

    void PageManager::write_pages(const std::vector<std::shared_ptr<Page>>& pages) {
        for (const std::shared_ptr<Page>& page : pages) {
            write_page(page);
        }
    }

    bool PageManager::is_page_managed(Page::ID id) {
        return get_partition(id)->is_page_managed(id);
    }

    Storage& PageManager::storage() {
        return _storage;
    }

    const Storage& PageManager::storage() const {
        return _storage;
    }

} // namespace diamond
