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

#include "gtest/gtest.h"

#include "diamond/exception.h"
#include "diamond/memory_storage.h"
#include "diamond/page_manager.h"

#include "mocks/eviction_strategy.h"
#include "mocks/page_writer.h"
#include "mocks/storage.h"

namespace {

    TEST(page_manager_tests, ensure_page_is_managed_after_creation) {
        MockStorage mock_storage;
        MockPageWriterFactory mock_page_writer_factory;
        std::shared_ptr<MockPageWriter> mock_page_writer =
            std::make_shared<MockPageWriter>(mock_storage);
        EXPECT_CALL(mock_page_writer_factory, create)
            .WillRepeatedly(::testing::Return(mock_page_writer));

        MockEvictionStrategyFactory mock_eviction_strategy_factory;
        std::shared_ptr<MockEvictionStrategy> mock_eviction_strategy =
            std::make_shared<MockEvictionStrategy>();
        EXPECT_CALL(mock_eviction_strategy_factory, create)
            .WillRepeatedly(::testing::Return(mock_eviction_strategy));

        diamond::PageManager manager(
            mock_storage,
            mock_page_writer_factory,
            mock_eviction_strategy_factory);
        diamond::PageAccessor accessor =
            manager.create_page(diamond::Page::Type::LEAF_NODE);
        EXPECT_TRUE(manager.is_page_managed(accessor.page()->get_id()));
    }

    TEST(page_manager_tests, ensure_unmanaged_page_is_read_from_storage) {
        diamond::MemoryStorage storage;

        diamond::Page::ID id = 1;
        std::shared_ptr<diamond::Page> page =
            diamond::Page::new_leaf_node_page(id);
        page->write_to_storage(storage);

        MockPageWriterFactory mock_page_writer_factory;
        std::shared_ptr<MockPageWriter> mock_page_writer =
            std::make_shared<MockPageWriter>(storage);
        EXPECT_CALL(mock_page_writer_factory, create)
            .WillRepeatedly(::testing::Return(mock_page_writer));

        MockEvictionStrategyFactory mock_eviction_strategy_factory;
        std::shared_ptr<MockEvictionStrategy> mock_eviction_strategy =
            std::make_shared<MockEvictionStrategy>();
        EXPECT_CALL(mock_eviction_strategy_factory, create)
            .WillRepeatedly(::testing::Return(mock_eviction_strategy));

        diamond::PageManager manager(
            storage,
            mock_page_writer_factory,
            mock_eviction_strategy_factory);

        EXPECT_FALSE(manager.is_page_managed(id));
        diamond::PageAccessor accessor =
            manager.get_page_accessor(id, diamond::PageAccessor::SHARED);
        EXPECT_TRUE(manager.is_page_managed(id));
    }

    TEST(page_manager_tests, throws_when_page_does_not_exist) {
        MockStorage mock_storage;
        MockPageWriterFactory mock_page_writer_factory;
        std::shared_ptr<MockPageWriter> mock_page_writer =
            std::make_shared<MockPageWriter>(mock_storage);
        EXPECT_CALL(mock_page_writer_factory, create)
            .WillRepeatedly(::testing::Return(mock_page_writer));

        MockEvictionStrategyFactory mock_eviction_strategy_factory;
        std::shared_ptr<MockEvictionStrategy> mock_eviction_strategy =
            std::make_shared<MockEvictionStrategy>();
        EXPECT_CALL(mock_eviction_strategy_factory, create)
            .WillRepeatedly(::testing::Return(mock_eviction_strategy));

        diamond::PageManager manager(
            mock_storage,
            mock_page_writer_factory,
            mock_eviction_strategy_factory);
        EXPECT_THROW(
            manager.get_page_accessor(1, diamond::PageAccessor::SHARED),
            diamond::Exception);
    }

} // namespace
