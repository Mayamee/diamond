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

#include "mocks/page_writer.h"
#include "mocks/storage.h"

namespace {

    TEST(page_manager_tests, write_page_ensure_writer_is_called) {
        MockStorage storage;
        MockPageWriterFactory mock_page_writer_factory;
        std::shared_ptr<MockPageWriter> mock_page_writer =
            std::make_shared<MockPageWriter>(storage);
        EXPECT_CALL(mock_page_writer_factory, create)
            .WillRepeatedly(::testing::Return(mock_page_writer));
        EXPECT_CALL(*mock_page_writer, write)
            .Times(2);

        diamond::PageManager manager(storage, mock_page_writer_factory);
        diamond::Page::ID id = 1;
        std::shared_ptr<diamond::Page> page =
            diamond::Page::new_leaf_node_page(id);

        EXPECT_FALSE(manager.is_page_managed(id));
        EXPECT_EQ(manager.pages_managed(), 0);
        manager.write_page(page);
        EXPECT_TRUE(manager.is_page_managed(id));
        EXPECT_EQ(manager.pages_managed(), 1);

        page->insert_leaf_node_entry(diamond::Buffer("key1"), 2, 0);
        manager.write_page(page);
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

        diamond::PageManager manager(storage, mock_page_writer_factory);

        EXPECT_FALSE(manager.is_page_managed(id));
        EXPECT_EQ(manager.pages_managed(), 0);
        diamond::PageManager::SharedAccessor accessor =
            manager.get_shared_accessor(id);
        EXPECT_TRUE(manager.is_page_managed(id));
        EXPECT_EQ(manager.pages_managed(), 1);
    }

    TEST(page_manager_tests, throws_when_page_does_not_exist) {
        MockStorage storage;
        MockPageWriterFactory mock_page_writer_factory;
        std::shared_ptr<MockPageWriter> mock_page_writer =
            std::make_shared<MockPageWriter>(storage);
        EXPECT_CALL(mock_page_writer_factory, create)
            .WillRepeatedly(::testing::Return(mock_page_writer));

        diamond::PageManager manager(storage, mock_page_writer_factory);
        EXPECT_THROW(manager.get_shared_accessor(1), diamond::Exception);
    }

} // namespace
