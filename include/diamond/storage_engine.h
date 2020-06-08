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

#ifndef _DIAMOND_STORAGE_ENGINE_H
#define _DIAMOND_STORAGE_ENGINE_H

#include "diamond/buffer.h"
#include "diamond/page_manager.h"

namespace diamond {

    class StorageEngine {
    public:
        StorageEngine(
            PageManager& page_manager,
            PageCompare compare_func = &page_default_compare);

        Buffer get(const Buffer& key);
        void insert(const Buffer& key, const Buffer& val);

    private:
        PageManager& _manager;
        PageCompare _compare_func;

        PageID get_leaf_page_id(const Buffer& key);
        PageAccessor get_free_data_page(const Buffer& val);
    };

}

#endif // _DIAMOND_STORAGE_ENGINE_H
