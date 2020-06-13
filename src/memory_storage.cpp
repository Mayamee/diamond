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

#include "diamond/memory_storage.h"

namespace diamond {

    MemoryStorage::MemoryStorage(size_t initial_size)
        : _buffer(new char[initial_size]),
        _pos(0),
        _size(initial_size) {}

    MemoryStorage::~MemoryStorage() {
        delete[] _buffer;
    }

    void MemoryStorage::write_impl(const char* buffer, size_t n) {
        if (_pos + n >= _size) {
            _size *= 2;
            char* new_buffer = new char[_size];
            std::memcpy(new_buffer, _buffer, _size);
            delete[] _buffer;
            _buffer = new_buffer;
        }
        std::memcpy(_buffer, buffer, n);
    }

    void MemoryStorage::read_impl(char* buffer, size_t n) {
        std::memcpy(buffer, _buffer, n);
    }

    void MemoryStorage::seek_impl(size_t n) {
        _pos = n;
    }

    uint64_t MemoryStorage::size_impl() {
        return _size;
    }

} // namespace diamond
