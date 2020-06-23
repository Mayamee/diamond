/*  Diamond - Embedded NoSQL Database
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

        diamond::Page* page1 = diamond::Page::new_page(1, diamond::Page::Type::DATA);

        for (size_t i = 0; i < data_entries.size(); i++) {
            page1->insert_data_entry(data_entries.at(i));
        }

        page1->write_to_storage(storage);

        diamond::Page* page2 = diamond::Page::from_storage(1, storage);

        ASSERT_EQ(page2->get_type(), diamond::Page::Type::DATA);
        ASSERT_EQ(page2->get_num_data_entries(), data_entries.size());
        for (size_t i = 0; i < page1->get_num_data_entries(); i++) {
            EXPECT_EQ(page2->get_data_entry(i).data(), data_entries.at(i));
        }

        delete page1;
        delete page2;
    }

    TEST(page_tests, write_and_read_free_list_page) {
        using TestInput = std::tuple<diamond::Page::ID, uint16_t>;
        std::vector<TestInput> free_list_entries = {
            { 1, 100 },
            { 2, 300 },
            { 3, 588 },
            { 4, 1024 }
        };

        diamond::MemoryStorage storage;

        diamond::Page* page1 = diamond::Page::new_page(1, diamond::Page::Type::FREE_LIST);

        for (size_t i = 0; i < free_list_entries.size(); i++) {
            const TestInput& free_list_entry = free_list_entries.at(i);
            page1->insert_free_list_entry(
                std::get<0>(free_list_entry),
                std::get<1>(free_list_entry));
        }

        page1->write_to_storage(storage);

        diamond::Page* page2 = diamond::Page::from_storage(1, storage);

        ASSERT_EQ(page2->get_type(), diamond::Page::Type::FREE_LIST);
        ASSERT_EQ(page2->get_num_free_list_entries(), free_list_entries.size());
        for (size_t i = 0; i < page1->get_num_free_list_entries(); i++) {
            const TestInput& free_list_entry = free_list_entries.at(i);
            const diamond::Page::FreeListEntry& entry = page2->get_free_list_entry(i);

            EXPECT_EQ(entry.data_id(), std::get<0>(free_list_entry));
            EXPECT_EQ(entry.free_space(), std::get<1>(free_list_entry));
        }

        delete page1;
        delete page2;
    }

    TEST(page_tests, write_and_read_internal_node_page) {
        using TestInput = std::tuple<diamond::Page::ID, size_t, diamond::Page::ID>;
        std::vector<TestInput> internal_nodes = {
            { 2, 0, 2 },
            { 2, 1, 3 },
            { 2, 2, 4 },
            { 2, 3, 5 }
        };

        diamond::MemoryStorage storage;

        diamond::Page* page1 = diamond::Page::new_page(1, diamond::Page::Type::INTERNAL_NODE);

        for (size_t i = 0; i < internal_nodes.size(); i++) {
            const TestInput& internal_node = internal_nodes.at(i);
            page1->insert_internal_node_entry(
                page1->internal_node_entries_end(),
                std::get<0>(internal_node),
                std::get<1>(internal_node),
                std::get<2>(internal_node));
        }

        page1->write_to_storage(storage);

        diamond::Page* page2 = diamond::Page::from_storage(1, storage);

        ASSERT_EQ(page2->get_type(), diamond::Page::Type::INTERNAL_NODE);
        ASSERT_EQ(page2->get_num_internal_node_entries(), internal_nodes.size());
        diamond::Page::InternalNodeEntryListIterator iter = page2->internal_node_entries_begin();
        for (size_t i = 0; i < page1->get_num_internal_node_entries(); i++, iter++) {
            const TestInput& internal_node = internal_nodes.at(i);
            const diamond::Page::InternalNodeEntry& entry = *iter;

            EXPECT_EQ(entry.key_data_id(), std::get<0>(internal_node));
            EXPECT_EQ(entry.key_data_index(), std::get<1>(internal_node));
            EXPECT_EQ(entry.next_node_id(), std::get<2>(internal_node));
        }

        delete page1;
        delete page2;
    }

    TEST(page_tests, write_and_read_leaf_node_page) {
        using TestInput = std::tuple<diamond::Page::ID, size_t, diamond::Page::ID, size_t>;
        std::vector<TestInput> leaf_nodes = {
            { 2, 0, 2, 1 },
            { 2, 2, 2, 3 },
            { 2, 3, 2, 4 },
            { 2, 5, 2, 6 }
        };

        diamond::MemoryStorage storage;

        diamond::Page* page1 = diamond::Page::new_page(1, diamond::Page::Type::LEAF_NODE);

        for (size_t i = 0; i < leaf_nodes.size(); i++) {
            const TestInput& leaf_node = leaf_nodes.at(i);
            page1->insert_leaf_node_entry(
                page1->leaf_node_entries_end(),
                std::get<0>(leaf_node),
                std::get<1>(leaf_node),
                std::get<2>(leaf_node),
                std::get<3>(leaf_node));
        }

        page1->write_to_storage(storage);

        diamond::Page* page2 = diamond::Page::from_storage(1, storage);

        ASSERT_EQ(page2->get_type(), diamond::Page::Type::LEAF_NODE);
        EXPECT_EQ(page2->get_next_leaf_node_page(), diamond::Page::INVALID_ID);
        ASSERT_EQ(page2->get_num_leaf_node_entries(), leaf_nodes.size());
        diamond::Page::LeafNodeEntryListIterator iter = page2->leaf_node_entries_begin();
        for (size_t i = 0; i < leaf_nodes.size(); i++, iter++) {
            const TestInput& leaf_node = leaf_nodes.at(i);
            const diamond::Page::LeafNodeEntry& entry = *iter;

            EXPECT_EQ(entry.key_data_id(), std::get<0>(leaf_node));
            EXPECT_EQ(entry.key_data_index(), std::get<1>(leaf_node));
            EXPECT_EQ(entry.val_data_id(), std::get<2>(leaf_node));
            EXPECT_EQ(entry.val_data_index(), std::get<3>(leaf_node));
        }

        delete page1;
        delete page2;
    }

} // namespace
