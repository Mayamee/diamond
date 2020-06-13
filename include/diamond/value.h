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

#ifndef _DIAMOND_VALUE_H
#define _DIAMOND_VALUE_H

#include <cstddef>

namespace diamond {

    template <class T>
    class Value {
    public:
        void load(BufferReader& reader);
        void store(BufferWriter& writer);
        size_t size() const;

        Value& operator=(const T& val);

    protected:
        T _val;
    };

    template <class T>
    void Value<T>::load(BufferReader& reader) {
        reader >> _val;
    }

    template <class T>
    void Value<T>::store(BufferWriter& writer) {
        writer << _val; 
    }

    template <class T>
    size_t Value<T>::size() const {
        return 0;
    }

    template <class T>
    Value<T>& Value<T>::operator=(const T& val) {
        _val = val;
        return *this;
    }

} // namespace diamond

#endif // _DIAMOND_VALUE_H
