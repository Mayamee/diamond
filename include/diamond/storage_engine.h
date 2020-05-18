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
        struct Options {
            PageManager::Options page_manager_options;
        };

        StorageEngine(std::iostream& data_stream, const Options& options);

        void get(const Buffer& key, Buffer& val);
        void insert(const Buffer& key, const Buffer& val);

    private:
        PageManager _manager;
    };

}

#endif // _DIAMOND_STORAGE_ENGINE_H
