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

#include <ostream>
#include <type_traits>

namespace diamond {

    class ReadableBuffer {
    public:
        ReadableBuffer(size_t size, const char* buffer);

        size_t bytes_read() const;

        template <class T>
        void read(T* val);

        void read(void* val, size_t size);

    private:
        size_t _ptr;
        size_t _size;
        const char* _buffer;
    };

    class WritableBuffer {
    public:
        WritableBuffer(size_t size);

        size_t bytes_written() const;

        template <class T>
        typename std::enable_if<!std::is_enum<T>::value, void>::type
        write(T val);

        template<class T>
        typename std::enable_if<std::is_enum<T>::value, void>::type
        write(T val);

        void write(const void* val, size_t size);

        void write_to_stream(const std::ostream& ostream);

    private:
        size_t _ptr;
        size_t _size;
        char* _buffer;
    };

    template <class T>
    void ReadableBuffer::read(T* val) {
        return read(val, sizeof(T));
    }

    template <class T>
    typename std::enable_if<!std::is_enum<T>::value, void>::type
    WritableBuffer::write(T val) { 
        write(&val, sizeof(T));
    }

    template<class T>
    typename std::enable_if<std::is_enum<T>::value, void>::type
    WritableBuffer::write(T val) {
        write(static_cast<typename std::underlying_type<T>::type>(val));
    }

} // namespace diamond

#endif // _DIAMOND_BUFFER_H
