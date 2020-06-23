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

#ifndef _DIAMOND_STORAGE_ENGINE_H
#define _DIAMOND_STORAGE_ENGINE_H

#include "diamond/buffer.h"
#include "diamond/exception.h"
#include "diamond/page_manager.h"
#include "diamond/utility.h"

namespace diamond {

    class StorageEngine {
    public:
        using Compare = std::function<int(const Buffer&, const Buffer&)>;

        class Iterator : noncopyable {
        public:
            ~Iterator();

            void next();

            Buffer key();
            Buffer val();

            bool end() const;

        private:
            friend class StorageEngine;

            PageManager& _manager;

            struct LeafPageIterator {
                LeafPageIterator(PageAccessor _page);

                PageAccessor page;
                SharedPageLock lock;
                Page::LeafNodeEntryListIterator iter;
            };
            LeafPageIterator* _leaf_page_iterator;

            Iterator(PageManager& manager, PageAccessor page);
        };

        static int default_compare(const Buffer& b0, const Buffer& b1);

        StorageEngine(PageManager& page_manager);

        uint64_t count(const Buffer& collection_name);
        bool exists(
            const Buffer& collection_name,
            const Buffer& key,
            Compare compare_func = &default_compare);
        Buffer get(
            const Buffer& collection_name,
            const Buffer& key,
            Compare compare_func = &default_compare);
        void put(
            const Buffer& collection_name,
            Buffer key,
            Buffer val,
            Compare compare_func = &default_compare);
        Iterator get_iterator(const Buffer& collection_name);

    private:
        PageManager& _manager;

        struct Collection {
            Page::ID root_node_id;
            Page::ID free_list_id;
        };

        Collection create_collection(const Buffer& name);
        Collection get_or_create_collection(const Buffer& name);
        Collection get_or_create_collection(const Buffer& name, bool& created);

        Page::InternalNodeEntryListIterator search_internal_node_entries(
            PageAccessor& page,
            const Buffer& key,
            Compare compare_func);
        Page::LeafNodeEntryListIterator find_leaf_node_entry(
            PageAccessor& page,
            const Buffer& key,
            Compare compare_func);
        PageAccessor get_leaf_page(
            Page::ID root_node_id,
            const Buffer& key,
            Compare compare_func);

        std::tuple<Page::ID, size_t> insert_value_into_data_page(
            Page::ID free_list_id,
            const Buffer& val);
    };

}

#endif // _DIAMOND_STORAGE_ENGINE_H
