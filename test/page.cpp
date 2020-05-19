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

#include "diamond/page.h"

namespace {

    TEST(page_tests, write_and_read_data_page) {
        std::stringstream ss;

        std::shared_ptr<diamond::Page> page1 =
            diamond::Page::new_data_page(1);

        page1->write_to_stream(ss);

        std::shared_ptr<diamond::Page> page2 =
            diamond::Page::new_page_from_stream(1, ss);

        ASSERT_EQ(page1->get_type(), diamond::Page::DATA);
        ASSERT_EQ(page1->get_type(), page2->get_type());
        ASSERT_EQ(page1->get_num_data_entries(), page2->get_num_data_entries());
        for (size_t i = 0; i < page1->get_num_data_entries(); i++) {
            const diamond::Page::DataEntry& entry1 = page1->get_data_entry(i);
            const diamond::Page::DataEntry& entry2 = page2->get_data_entry(i);

            EXPECT_EQ(entry1.data(), entry2.data());
            EXPECT_EQ(entry1.overflow_id(), entry2.overflow_id());
            EXPECT_EQ(entry1.overflow_index(), entry2.overflow_index());
        }
    }

    TEST(page_tests, write_and_read_internal_node_page) {
        std::stringstream ss;

        std::shared_ptr<diamond::Page> page1 =
            diamond::Page::new_internal_node_page(1);

        page1->write_to_stream(ss);

        std::shared_ptr<diamond::Page> page2 =
            diamond::Page::new_page_from_stream(1, ss);

        ASSERT_EQ(page1->get_type(), diamond::Page::INTERNAL_NODE);
        ASSERT_EQ(page1->get_type(), page2->get_type());
        ASSERT_EQ(page1->get_num_internal_node_entries(), page2->get_num_internal_node_entries());
        for (size_t i = 0; i < page1->get_num_internal_node_entries(); i++) {
            const diamond::Page::InternalNodeEntry& entry1 = page1->get_internal_node_entry(i);
            const diamond::Page::InternalNodeEntry& entry2 = page2->get_internal_node_entry(i);

            EXPECT_EQ(entry1.key(), entry2.key());
            EXPECT_EQ(entry1.next_node_id(), entry2.next_node_id());
        }
    }

} // namespace
