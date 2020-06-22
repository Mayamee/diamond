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
#include "diamond/page_manager.h"
#include "diamond/utility.h"

namespace diamond {

    class StorageEngine {
    public:
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

        StorageEngine(PageManager& page_manager);

        uint64_t count(const Buffer& collection_name);
        bool exists(
            const Buffer& collection_name,
            const Buffer& key,
            Page::Compare compare_func = &Page::default_compare);
        Buffer get(
            const Buffer& collection_name,
            const Buffer& key,
            Page::Compare compare_func = &Page::default_compare);
        void insert(
            const Buffer& collection_name,
            Buffer key,
            Buffer val,
            Page::Compare compare_func = &Page::default_compare);
        Iterator get_iterator(const Buffer& collection_name);

    private:
        PageManager& _manager;

        struct Collection {
            Page::ID root_page_id;
            Page::ID free_list_id;
        };

        Collection create_collection(const Buffer& name);
        Collection get_or_create_collection(const Buffer& name);
        Collection get_or_create_collection(const Buffer& name, bool& created);

        PageAccessor get_leaf_page(
            Page::ID root_page_id,
            const Buffer& key,
            Page::Compare compare_func);
        void insert_into_data_page(
            Page::ID free_list_id,
            const Buffer& val,
            Page::ID& data_page_id,
            size_t& data_page_index);
    };

}

#endif // _DIAMOND_STORAGE_ENGINE_H
