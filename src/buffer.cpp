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

#include <cstring>

#include "diamond/buffer.h"

namespace diamond {

    Buffer::Buffer()
        : _size(0),
        _buffer(nullptr),
        _owns_buffer(true) {}

    Buffer::Buffer(size_t size)
        : _size(size),
        _buffer(new char[size]),
        _owns_buffer(true) {}

    Buffer::Buffer(size_t size, char* buffer)
        : _size(size),
        _buffer(buffer),
        _owns_buffer(false) {}

    Buffer::Buffer(size_t size, std::istream& stream)
        : _size(size),
        _buffer(new char[size]),
        _owns_buffer(true) {
        stream.read(_buffer, size);
    }

    Buffer::Buffer(const Buffer& other)
        : _size(other._size),
        _buffer(new char[other._size]),
        _owns_buffer(true) {
        std::memcpy(_buffer, other._buffer, _size);
    }

    Buffer::Buffer(Buffer&& other)
        : _size(other._size),
        _buffer(other._buffer),
        _owns_buffer(other._owns_buffer)  {
        if (_owns_buffer) {
            other._size = 0;
            other._buffer = nullptr;
        }
    }

    Buffer::~Buffer() {
        if (_owns_buffer && _buffer) {
            delete[] _buffer;
        }
    }

    size_t Buffer::size() const {
        return _size;
    }

    char* Buffer::buffer() {
        return _buffer;
    }

    const char* Buffer::buffer() const {
        return _buffer;
    }

    void Buffer::write_to_stream(std::ostream& ostream) const {
        ostream.write(_buffer, _size);
    }

    char Buffer::operator[](size_t i) const {
        return _buffer[i];
    }

    Buffer& Buffer::operator=(const Buffer& other) {
        if (this != &other) {
            if (_owns_buffer && _buffer) delete[] _buffer;
            _size = other._size;
            _buffer = new char[_size];
            _owns_buffer = true;
            std::memcpy(_buffer, other._buffer, _size);
        }

        return *this;
    }

    Buffer& Buffer::operator=(Buffer&& other) {
        if (this != &other) {
            if (_owns_buffer && _buffer) delete[] _buffer;
            _size = other._size;
            _buffer = new char[_size];
            _owns_buffer = other._owns_buffer;
            if (_owns_buffer) {
                other._size = 0;
                other._buffer = nullptr;
            }
        }
    }

    bool Buffer::operator==(const Buffer& other) {
        if (_size != other._size) return false;
        return std::memcmp(_buffer, other._buffer, _size) == 0;
    }

    bool Buffer::operator!=(const Buffer& other) {
        return !(*this == other);
    }

    BufferReader::BufferReader(const Buffer& buffer, endian::Endianness endianness)
        : _ptr(0),
        _buffer(buffer),
        _endianness(endianness) {}

    size_t BufferReader::bytes_read() const {
        return _ptr;
    }

    void BufferReader::read(Buffer& buffer) {
        read(buffer.buffer(), buffer.size());
    }

    void BufferReader::read(void* val, size_t size) {
        std::memcpy(val, _buffer.buffer() + _ptr, size);
        _ptr += size;
    }

    BufferWriter::BufferWriter(Buffer& buffer, endian::Endianness endianness)
        : _ptr(0),
        _buffer(buffer),
        _endianness(endianness) {}

    size_t BufferWriter::bytes_written() const {
        return _ptr;
    }

    void BufferWriter::write(const Buffer& buffer) {
        write(buffer.buffer(), buffer.size());
    }

    void BufferWriter::write(const void* val, size_t size) {
        std::memcpy(_buffer.buffer() + _ptr, val, size);
        _ptr += size;
    }

} // namespace diamond
