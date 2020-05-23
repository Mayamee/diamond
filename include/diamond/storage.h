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

#ifndef _DIAMOND_STORAGE_H
#define _DIAMOND_STORAGE_H

#include <boost/utility.hpp>

#include "diamond/buffer.h"

namespace diamond {

    class Storage : boost::noncopyable {
    public:
        Storage() = default;

        virtual void write(const char* buffer, size_t n) = 0;
        virtual void write(const Buffer& buffer) = 0;

        virtual void read(char* buffer, size_t n) = 0;
        virtual void read(Buffer& buffer, size_t n) = 0;

        virtual void seek(size_t n) = 0;

        virtual size_t size() = 0;
    };

} // namespace diamond

#endif // _DIAMOND_STORAGE_H
