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

#include <cstring>

#include "diamond/buffer.h"
#include "diamond/endian.h"

namespace diamond {

    ReadableBuffer::ReadableBuffer(size_t size, const char* buffer)
        : _ptr(0), 
        _size(size),
        _buffer(buffer) {}

    size_t ReadableBuffer::bytes_read() const {
        return _ptr;
    }

    void ReadableBuffer::read(void* val, size_t size) {
        std::memcpy(val, _buffer + _ptr, size);
        _ptr += size;
    }

    WritableBuffer::WritableBuffer(size_t size)
        : _ptr(0),
        _size(size),
        _buffer(new char[_size]) {}

    size_t WritableBuffer::bytes_written() const {
        return _ptr;
    }

    template <>
    void WritableBuffer::write(int16_t val) {
        uint16_t be = htobe16((uint16_t)val);
        write(&be, sizeof(uint16_t));
    }

    template <>
    void WritableBuffer::write(int32_t val) {
        uint32_t be = htobe32((uint32_t)val);
        write(&be, sizeof(uint32_t));
    }

    template <>
    void WritableBuffer::write(int64_t val) {
        uint64_t be = htobe64((uint64_t)val);
        write(&be, sizeof(uint64_t));
    }

    template <>
    void WritableBuffer::write(uint16_t val) {
        uint16_t be = htobe16(val);
        write(&be, sizeof(uint16_t));
    }

    template <>
    void WritableBuffer::write(uint32_t val) {
        uint32_t be = htobe32(val);
        write(&be, sizeof(uint32_t));
    }

    template <>
    void WritableBuffer::write(uint64_t val) {
        uint64_t be = htobe64(val);
        write(&be, sizeof(uint64_t));
    }

    void WritableBuffer::write(const void* val, size_t size) {
        std::memcpy(_buffer + _ptr, val, size);
        _ptr += size;
    }

    void WritableBuffer::write_to_stream(const std::ostream& ostream) {}

} // namespace diamond
