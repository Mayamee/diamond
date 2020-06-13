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

#ifndef _DIAMOND_VALUE_PROCESSORS_H
#define _DIAMOND_VALUE_PROCESSORS_H

#include "diamond/buffer.h"
#include "diamond/value.h"

namespace diamond {

    class SizeCalculator final {
    public:
        SizeCalculator();

        template <class T>
        SizeCalculator& operator&(Value<T>& val);

        size_t size() const;

    private:
        size_t _size;
    };

    class Deserializer final {
    public:
        Deserializer(Buffer& buffer);

        template <class T>
        Deserializer& operator&(Value<T>& val);

    private:
        BufferReader _reader;
    };

    class Serializer final {
    public:
        Serializer(Buffer& buffer);

        template <class T>
        Serializer& operator&(Value<T>& val);       

    private:
        BufferWriter _writer;
    };

    template <class T>
    SizeCalculator& SizeCalculator::operator&(Value<T>& val) {
        _size += val.size();
        return *this;
    }

    template <class T>
    Deserializer& Deserializer::operator&(Value<T>& val) {
        val.load(_reader);
        return *this;
    }

    template <class T>
    Serializer& Serializer::operator&(Value<T>& val) {
        val.store(_writer);
        return *this;
    }

} // namespace diamond

#endif // _DIAMOND_VALUE_PROCESSORS_H
