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

#ifndef _DIAMOND_BINARY_ARCHIVE_H
#define _DIAMOND_BINARY_ARCHIVE_H

#include "diamond/buffer.h"
#include "diamond/serialization/default.h"

namespace diamond {

    class BinaryIArchive final {
    public:
        BinaryIArchive(const Buffer& buffer);

        template <class T>
        BinaryIArchive& operator&(T& val);

    private:
        BufferReader _reader;
    };

    class BinaryOArchive final {
    public:
        BinaryOArchive(Buffer& buffer);

        template <class T>
        BinaryOArchive& operator&(T& val);       

    private:
        BufferWriter _writer;
    };

    template <class T>
    BinaryIArchive& BinaryIArchive::operator&(T& val) {
        // serialization::load(*this, val);
        return *this;
    }

    template <class T>
    BinaryOArchive& BinaryOArchive::operator&(T& val) {
        // serialization::store(*this, val);
        return *this;
    }

} // namespace diamond

#endif // _DIAMOND_BINARY_ARCHIVE_H
