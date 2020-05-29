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

#include "diamond/memory_storage.h"

namespace diamond {

    void MemoryStorage::write_impl(const char* buffer, size_t n) {
        _ss.write(buffer, n);
    }

    void MemoryStorage::read_impl(char* buffer, size_t n) {
        _ss.read(buffer, n);
    }

    void MemoryStorage::seek_impl(size_t n) {
        _ss.seekg(n);
    }

    uint64_t MemoryStorage::size_impl() {
        std::streampos pos = _ss.tellg();
        _ss.seekg(0, std::ios::end);
        std::streampos size = _ss.tellg() - pos;
        _ss.seekg(pos);
        return size;
    }

} // namespace diamond
