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
            size_t num_partitions,
            size_t max_num_pages_in_partition)
            : _storage(storage),
            _num_partitions(num_partitions),
            _next_page_id(storage.size() / PAGE_SIZE + 1) {
        for (size_t i = 0; i < _num_partitions; i++) {
            _partitions.push_back(
                std::make_unique<PageManagerPartition>(
                    storage,
                    page_writer_factory.create(),
                    eviction_strategy_factory.create(),
                    max_num_pages_in_partition
                ));
        }
    }

    PageAccessor PageManager::create_page(PageType type) {
        PageID id = next_page_id();
        return get_partition(id)->create_page(id, type);
    }

    PageAccessor PageManager::get_page_accessor(PageID id, PageAccessorMode access_mode) {
        return get_partition(id)->get_page_accessor(id, access_mode);
    }

    void PageManager::write_page(const Page& page) {
        get_partition(page->get_id())->write_page(page);
    }

    void PageManager::write_pages(const std::vector<Page>& pages) {
        for (const Page& page : pages) {
            write_page(page);
        }
    }

    bool PageManager::is_page_managed(PageID id) {
        return get_partition(id)->is_page_managed(id);
    }

    Storage& PageManager::storage() {
        return _storage;
    }

    const Storage& PageManager::storage() const {
        return _storage;
    }

} // namespace diamond
