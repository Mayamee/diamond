/*  Diamond - Relational Database
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

#include "diamond/page_manager.h"

namespace diamond {

    class StorageEngine {
    public:
        class Key {
        public:
            Key(size_t size, void* val);

            size_t size() const;
            const void* val() const;

        private:
            size_t _size;
            void* _val;
        };

        StorageEngine() = default;

        void* get(const Key& key);
        void insert(const Key& key, void* data);

    private:
        PageManager _manager;

        void* search(std::shared_ptr<Page>& page, const Key& key);
    };

}

#endif // _DIAMOND_STORAGE_ENGINE_H
