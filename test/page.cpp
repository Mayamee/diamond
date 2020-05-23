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

#include "diamond/memory_storage.h"
#include "diamond/page.h"

namespace {

    TEST(page_tests, write_and_read_data_page) {
        using TestInput = std::tuple<diamond::Buffer, diamond::Page::ID, size_t>;
        std::vector<TestInput> inputs = {
            { diamond::Buffer("wowowwowowoww"), 0, 0 },
            { diamond::Buffer("it is indeed a buffer"), 0, 0 },
            { diamond::Buffer("this is a buffer"), 0, 0 },
            { diamond::Buffer("hello world"), 0, 0 }
        };

        diamond::MemoryStorage storage;

        std::shared_ptr<diamond::Page> page1 =
            diamond::Page::new_data_page(1);
        size_t expected_size = page1->header_size();

        ASSERT_EQ(page1->get_type(), diamond::Page::DATA);

        for (size_t i = 0; i < inputs.size(); i++) {
            const TestInput& input = inputs.at(i);
            const diamond::Buffer& buffer = std::get<0>(input);
            diamond::Page::ID overflow_id = std::get<1>(input);
            size_t overflow_index = std::get<2>(input);
            page1->insert_data_entry(buffer, overflow_id, overflow_index);
            expected_size += buffer.size();
            if (overflow_id) expected_size += sizeof(overflow_id) + sizeof(overflow_index);
        }

        ASSERT_EQ(page1->get_num_data_entries(), inputs.size());
        EXPECT_EQ(page1->get_size(), expected_size);
        EXPECT_EQ(page1->get_remaining_space(), diamond::Page::SIZE - expected_size);

        page1->write_to_storage(storage);

        std::shared_ptr<diamond::Page> page2 =
            diamond::Page::new_page_from_storage(1, storage);

        ASSERT_EQ(page2->get_type(), diamond::Page::DATA);
        ASSERT_EQ(page2->get_num_data_entries(), inputs.size());
        EXPECT_EQ(page2->get_size(), expected_size);
        for (size_t i = 0; i < page1->get_num_data_entries(); i++) {
            const TestInput& input = inputs.at(i);
            const diamond::Page::DataEntry& entry = page2->get_data_entry(i);

            EXPECT_EQ(entry.data(), std::get<0>(input));
            EXPECT_EQ(entry.overflow_id(), std::get<1>(input));
            EXPECT_EQ(entry.overflow_index(), std::get<2>(input));
        }
    }

    TEST(page_tests, write_and_read_internal_node_page) {
        using TestInput = std::tuple<diamond::Buffer, diamond::Page::ID>;
        std::vector<TestInput> inputs = {
            { diamond::Buffer("key1"), 2 },
            { diamond::Buffer("key2"), 3 },
            { diamond::Buffer("key3"), 4 },
            { diamond::Buffer("key4"), 5 }
        };

        diamond::MemoryStorage storage;

        std::shared_ptr<diamond::Page> page1 =
            diamond::Page::new_internal_node_page(1);
        size_t expected_size = page1->header_size();

        ASSERT_EQ(page1->get_type(), diamond::Page::INTERNAL_NODE);

        for (size_t i = 0; i < inputs.size(); i++) {
            const TestInput& input = inputs.at(i);
            const diamond::Buffer& buffer = std::get<0>(input);
            diamond::Page::ID next_node_id = std::get<1>(input);
            page1->insert_internal_node_entry(buffer, next_node_id);
            expected_size += buffer.size() + sizeof(next_node_id);
        }

        ASSERT_EQ(page1->get_num_internal_node_entries(), inputs.size());
        EXPECT_EQ(page1->get_size(), expected_size);
        EXPECT_EQ(page1->get_remaining_space(), diamond::Page::SIZE - expected_size);

        page1->write_to_storage(storage);

        std::shared_ptr<diamond::Page> page2 =
            diamond::Page::new_page_from_storage(1, storage);

        ASSERT_EQ(page2->get_type(), diamond::Page::INTERNAL_NODE);
        ASSERT_EQ(page2->get_num_internal_node_entries(), inputs.size());
        EXPECT_EQ(page2->get_size(), expected_size);
        for (size_t i = 0; i < page1->get_num_internal_node_entries(); i++) {
            const TestInput& input = inputs.at(i);
            const diamond::Page::InternalNodeEntry& entry = page2->get_internal_node_entry(i);

            EXPECT_EQ(entry.key(), std::get<0>(input));
            EXPECT_EQ(entry.next_node_id(), std::get<1>(input));
        }
    }

    TEST(page_tests, write_and_read_leaf_node_page) {
        using TestInput = std::tuple<diamond::Buffer, diamond::Page::ID, size_t>;
        std::vector<TestInput> inputs = {
            { diamond::Buffer("key1"), 2, 0 },
            { diamond::Buffer("key2"), 2, 1 },
            { diamond::Buffer("key3"), 2, 2 },
            { diamond::Buffer("key4"), 2, 3 }
        };

        diamond::MemoryStorage storage;

        std::shared_ptr<diamond::Page> page1 =
            diamond::Page::new_leaf_node_page(1);
        size_t expected_size = page1->header_size();

        ASSERT_EQ(page1->get_type(), diamond::Page::LEAF_NODE);

        for (size_t i = 0; i < inputs.size(); i++) {
            const TestInput& input = inputs.at(i);
            const diamond::Buffer& buffer = std::get<0>(input);
            diamond::Page::ID data_id = std::get<1>(input);
            size_t data_index = std::get<2>(input);
            page1->insert_leaf_node_entry(buffer, data_id, data_index);
            expected_size += buffer.size() + sizeof(data_index) + sizeof(data_id);
        }

        ASSERT_EQ(page1->get_num_leaf_node_entries(), inputs.size());
        EXPECT_EQ(page1->get_size(), expected_size);
        EXPECT_EQ(page1->get_remaining_space(), diamond::Page::SIZE - expected_size);

        page1->write_to_storage(storage);

        std::shared_ptr<diamond::Page> page2 =
            diamond::Page::new_page_from_storage(1, storage);

        ASSERT_EQ(page2->get_type(), diamond::Page::LEAF_NODE);
        ASSERT_EQ(page2->get_num_leaf_node_entries(), inputs.size());
        EXPECT_EQ(page2->get_size(), expected_size);
        for (size_t i = 0; i < inputs.size(); i++) {
            const TestInput& input = inputs.at(i);
            const diamond::Page::LeafNodeEntry& entry = page2->get_leaf_node_entry(i);

            EXPECT_EQ(entry.key(), std::get<0>(input));
            EXPECT_EQ(entry.data_id(), std::get<1>(input));
            EXPECT_EQ(entry.data_index(), std::get<2>(input));
        }
    }

} // namespace
