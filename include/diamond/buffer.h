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

#ifndef _DIAMOND_BUFFER_H
#define _DIAMOND_BUFFER_H

#include <iostream>
#include <type_traits>

#include "diamond/endian.h"

namespace diamond {

    class Buffer {
    public:
        Buffer();
        Buffer(size_t size);
        Buffer(size_t size, char* buffer);
        Buffer(size_t size, std::istream& stream);

        Buffer(const Buffer& other);
        Buffer(Buffer&& other);

        ~Buffer();

        size_t size() const;

        char* buffer();
        const char* buffer() const;

        void write_to_stream(std::ostream& ostream) const;

        char operator[](size_t i) const;

        Buffer& operator=(const Buffer& other);
        Buffer& operator=(Buffer&& other);

        bool operator==(const Buffer& other);
        bool operator!=(const Buffer& other);

    private:
        size_t _size;
        char* _buffer;
        bool _owns_buffer;
    };

    class BufferReader {
    public:
        BufferReader(
            const Buffer& buffer,
            endian::Endianness endianness = endian::Endianness::BIG);

        size_t bytes_read() const;

        template <class T>
        typename std::enable_if<
            !std::is_enum<T>::value &&
            !std::is_arithmetic<T>::value &&
            std::is_trivially_copyable<T>::value,
            T
        >::type
        read();

        template<class T>
        typename std::enable_if<std::is_enum<T>::value, T>::type
        read();

        template<class T>
        typename std::enable_if<std::is_arithmetic<T>::value, T>::type
        read();

        void read(Buffer& buffer);
        void read(void* val, size_t size);

    private:
        size_t _ptr;
        const Buffer& _buffer;
        endian::Endianness _endianness;
    };

    class BufferWriter {
    public:
        BufferWriter(
            Buffer& buffer,
            endian::Endianness endianness = endian::Endianness::BIG);

        size_t bytes_written() const;

        template <class T>
        typename std::enable_if<
            !std::is_enum<T>::value &&
            !std::is_arithmetic<T>::value &&
            std::is_trivially_copyable<T>::value,
            void
        >::type
        write(T val);

        template<class T>
        typename std::enable_if<std::is_enum<T>::value, void>::type
        write(T val);

        template<class T>
        typename std::enable_if<std::is_arithmetic<T>::value, void>::type
        write(T val);

        void write(const Buffer& buffer);
        void write(const void* val, size_t size);

    private:
        size_t _ptr;
        Buffer& _buffer;
        endian::Endianness _endianness;
    };

    template <class T>
    typename std::enable_if<
        !std::is_enum<T>::value &&
        !std::is_arithmetic<T>::value &&
        std::is_trivially_copyable<T>::value,
        T
    >::type
    BufferReader::read() {
        T val;
        read(&val, sizeof(T));
        return val;
    }

    template<class T>
    typename std::enable_if<std::is_enum<T>::value, T>::type
    BufferReader::read() {
        return static_cast<T>(read<typename std::underlying_type<T>::type>());
    }

    template<class T>
    typename std::enable_if<std::is_arithmetic<T>::value, T>::type
    BufferReader::read() {
        T val;
        read(&val, sizeof(T));
        if (_endianness != endian::HOST_ORDER) {
            endian::swap_endianness(val);
        }
        return val;
    }

    template <class T>
    typename std::enable_if<
        !std::is_enum<T>::value &&
        !std::is_arithmetic<T>::value &&
        std::is_trivially_copyable<T>::value,
        void
    >::type
    BufferWriter::write(T val) { 
        write(&val, sizeof(T));
    }

    template<class T>
    typename std::enable_if<std::is_enum<T>::value, void>::type
    BufferWriter::write(T val) {
        write(static_cast<typename std::underlying_type<T>::type>(val));
    }

    template<class T>
    typename std::enable_if<std::is_arithmetic<T>::value, void>::type
    BufferWriter::write(T val) {
        if (_endianness != endian::HOST_ORDER) {
            endian::swap_endianness(val);
        }
        return write(&val, sizeof(T));
    }

} // namespace diamond

#endif // _DIAMOND_BUFFER_H
