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

#include <sstream>

#include "gtest/gtest.h"

#include "diamond/page_manager.h"
#include "mocks/page_writer.h"

namespace {

    TEST(page_manager_tests, write_page_ensure_writer_is_called) {
        std::stringstream ss;

        MockPageWriterFactory mock_page_writer_factory;
        std::shared_ptr<MockPageWriter> mock_page_writer =
            std::make_shared<MockPageWriter>(ss);
        EXPECT_CALL(mock_page_writer_factory, create)
            .WillRepeatedly(::testing::Return(mock_page_writer));
        EXPECT_CALL(*mock_page_writer, write)
            .Times(2);

        diamond::PageManager manager(ss, mock_page_writer_factory);
        diamond::Page::ID id = 1;
        std::shared_ptr<diamond::Page> page =
            diamond::Page::new_leaf_node_page(id);

        EXPECT_FALSE(manager.is_page_managed(id));
        manager.write_page(page);
        EXPECT_TRUE(manager.is_page_managed(id));

        page->insert_leaf_node_entry(diamond::Buffer("key1"), 2, 0);
        manager.write_page(page);
    }

} // namespace
