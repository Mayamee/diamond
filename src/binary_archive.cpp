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

#include "diamond/binary_archive.h"

namespace diamond {

    BinaryIArchive::BinaryIArchive(const Buffer& buffer)
        : _reader(buffer) {}

    template <>
    void BinaryIArchive::load_primitive(std::string& str) {
        size_t s;
        _reader >> s;
        char* c = new char[s + 1];
        _reader.read(c, s);
        c[s] = '\0';
        str = c;
        delete c;
    }

    BinaryOArchive::BinaryOArchive(Buffer& buffer)
        : _writer(buffer) {}

    template <>
    void BinaryOArchive::store_primitive(std::string& str) {
        _writer << str.size();
        _writer << str;
    }

} // namespace diamond
