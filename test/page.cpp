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
        std::vector<diamond::Buffer> data_entries = {
            diamond::Buffer("wowowwowowoww"),
            diamond::Buffer("it is indeed a buffer"),
            diamond::Buffer("this is a buffer"),
            diamond::Buffer("hello world")
        };

        diamond::MemoryStorage storage;

        diamond::Page page1(1, diamond::PageType::DATA);

        for (size_t i = 0; i < data_entries.size(); i++) {
            page1->insert_data_entry(data_entries.at(i));
        }

        page1->write_to_storage(storage);

        diamond::Page page2 = diamond::Page::from_storage(1, storage);

        ASSERT_EQ(page2->get_type(), diamond::PageType::DATA);
        EXPECT_EQ(page2->get_next_data_page(), diamond::INVALID_PAGE);
        ASSERT_EQ(page2->get_num_data_entries(), data_entries.size());
        for (size_t i = 0; i < page1->get_num_data_entries(); i++) {
            EXPECT_EQ(page2->get_data_entry(i).data(), data_entries.at(i));
        }
    }

    TEST(page_tests, write_and_read_internal_node_page) {
        using TestInput = std::tuple<diamond::Buffer, diamond::PageID>;
        std::vector<TestInput> internal_nodes = {
            { diamond::Buffer("key1"), 2 },
            { diamond::Buffer("key2"), 3 },
            { diamond::Buffer("key3"), 4 },
            { diamond::Buffer("key4"), 5 }
        };

        diamond::MemoryStorage storage;

        diamond::Page page1(1, diamond::PageType::INTERNAL_NODE);

        for (size_t i = 0; i < internal_nodes.size(); i++) {
            const TestInput& internal_node = internal_nodes.at(i);
            page1->insert_internal_node_entry(
                std::get<0>(internal_node),
                std::get<1>(internal_node));
        }

        page1->write_to_storage(storage);

        diamond::Page page2 = diamond::Page::from_storage(1, storage);

        ASSERT_EQ(page2->get_type(), diamond::PageType::INTERNAL_NODE);
        ASSERT_EQ(page2->get_num_internal_node_entries(), internal_nodes.size());
        for (size_t i = 0; i < page1->get_num_internal_node_entries(); i++) {
            const TestInput& internal_node = internal_nodes.at(i);
            const diamond::InternalNodeEntry& entry = page2->get_internal_node_entry(i);

            EXPECT_EQ(entry.key(), std::get<0>(internal_node));
            EXPECT_EQ(entry.next_node_id(), std::get<1>(internal_node));
        }
    }

    TEST(page_tests, write_and_read_leaf_node_page) {
        using TestInput = std::tuple<diamond::Buffer, diamond::PageID, size_t>;
        std::vector<TestInput> leaf_nodes = {
            { diamond::Buffer("key1"), 2, 0 },
            { diamond::Buffer("key2"), 2, 1 },
            { diamond::Buffer("key3"), 2, 2 },
            { diamond::Buffer("key4"), 2, 3 }
        };

        diamond::MemoryStorage storage;

        diamond::Page page1(1, diamond::PageType::LEAF_NODE);

        for (size_t i = 0; i < leaf_nodes.size(); i++) {
            const TestInput& leaf_node = leaf_nodes.at(i);
            page1->insert_leaf_node_entry(
                std::get<0>(leaf_node),
                std::get<1>(leaf_node),
                std::get<2>(leaf_node));
        }

        page1->write_to_storage(storage);

        diamond::Page page2 = diamond::Page::from_storage(1, storage);

        ASSERT_EQ(page2->get_type(), diamond::PageType::LEAF_NODE);
        EXPECT_EQ(page2->get_next_leaf_node_page(), diamond::INVALID_PAGE);
        ASSERT_EQ(page2->get_num_leaf_node_entries(), leaf_nodes.size());
        for (size_t i = 0; i < leaf_nodes.size(); i++) {
            const TestInput& leaf_node = leaf_nodes.at(i);
            const diamond::LeafNodeEntry& entry = page2->get_leaf_node_entry(i);

            EXPECT_EQ(entry.key(), std::get<0>(leaf_node));
            EXPECT_EQ(entry.data_id(), std::get<1>(leaf_node));
            EXPECT_EQ(entry.data_index(), std::get<2>(leaf_node));
        }
    }

    TEST(page_tests, throws_when_data_page_does_not_have_enough_space) {
        diamond::Page page(1, diamond::PageType::DATA);
        diamond::Buffer buffer(page->get_remaining_space() + 1);
        EXPECT_THROW(page->insert_data_entry(buffer), std::logic_error);
    }

    TEST(page_tests, throws_when_internal_node_page_does_not_have_enough_space) {
        diamond::Page page(1, diamond::PageType::INTERNAL_NODE);
        diamond::Buffer buffer(page->get_remaining_space() + 1);
        EXPECT_THROW(page->insert_internal_node_entry(buffer, 0), std::logic_error);
    }

    TEST(page_tests, throws_when_leaf_node_page_does_not_have_enough_space) {
        diamond::Page page(1, diamond::PageType::LEAF_NODE);
        diamond::Buffer buffer(page->get_remaining_space() + 1);
        EXPECT_THROW(page->insert_leaf_node_entry(buffer, 1, 0), std::logic_error);
    }

} // namespace
