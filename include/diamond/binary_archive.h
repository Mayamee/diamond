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
#include "diamond/base_archive.h"

namespace diamond {

    class BinaryIArchive final : public BaseIArchive<BinaryIArchive> {
    public:
        BinaryIArchive(const Buffer& buffer);

    private:
        BufferReader _reader;

        friend class BaseIArchive<BinaryIArchive>;

        template <class T>
        void load_primitive(T& val);
    };

    class BinaryOArchive final : public BaseOArchive<BinaryOArchive> {
    public:
        BinaryOArchive(Buffer& buffer);

    private:
        BufferWriter _writer;

        friend class BaseOArchive<BinaryOArchive>;

        template <class T>
        void store_primitive(T& val);
    };

    template <class T>
    void BinaryIArchive::load_primitive(T& val) {
        _reader >> val;
    }

    template <>
    void BinaryIArchive::load_primitive(std::string& str);

    template <class T>
    void BinaryOArchive::store_primitive(T& val) {
        _writer << val;
    }

    template <>
    void BinaryOArchive::store_primitive(std::string& str);

} // namespace diamond

#endif // _DIAMOND_BINARY_ARCHIVE_H
