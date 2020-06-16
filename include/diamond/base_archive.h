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

#ifndef _DIAMOND_BASE_ARCHIVE_H
#define _DIAMOND_BASE_ARCHIVE_H

#include <type_traits>

#include "diamond/serialization/serialization.h"

namespace diamond {

    template <class Derived>
    class BaseIArchive {
    public:
        template <
            class T,
            typename std::enable_if<
                (std::is_fundamental<T>::value ||
                std::is_same<T, std::string>::value),
            int>::type = 0
        >
        void load(T& val);

        template <
            class T,
            typename std::enable_if<
                (!std::is_fundamental<T>::value &&
                !std::is_same<T, std::string>::value),
            int>::type = 0
        >
        void load(T& val);

        template <class T>
        Derived& operator>>(T& val);

        template <class T>
        Derived& operator&(T& val);
    };

    template <class Derived>
    class BaseOArchive {
    public:
        template <
            class T,
            typename std::enable_if<
                (std::is_fundamental<T>::value ||
                std::is_same<T, std::string>::value),
            int>::type = 0
        >
        void store(T& val);

        template <
            class T,
            typename std::enable_if<
                (!std::is_fundamental<T>::value &&
                !std::is_same<T, std::string>::value),
            int>::type = 0
        >
        void store(T& val);

        template <class T>
        Derived& operator<<(T& val);

        template <class T>
        Derived& operator&(T& val);
    };

    template <class Derived>
    template <
        class T,
        typename std::enable_if<
            (std::is_fundamental<T>::value ||
            std::is_same<T, std::string>::value),
        int>::type
    >
    void BaseIArchive<Derived>::load(T& val) {
        static_cast<Derived*>(this)->load_primitive(val);
    }

    template <class Derived>
    template <
        class T,
        typename std::enable_if<
            (!std::is_fundamental<T>::value &&
            !std::is_same<T, std::string>::value),
        int>::type
    >
    void BaseIArchive<Derived>::load(T& val) {
        serialization::serialize(val, *this);
    }

    template <class Derived>
    template <class T>
    Derived& BaseIArchive<Derived>::operator>>(T& val) {
        load<T>(val);
        return static_cast<Derived&>(*this);
    }

    template <class Derived>
    template <class T>
    Derived& BaseIArchive<Derived>::operator&(T& val) {
        return *this >> val;
    }

    template <class Derived>
    template <
        class T,
        typename std::enable_if<
            (std::is_fundamental<T>::value ||
            std::is_same<T, std::string>::value),
        int>::type
    >
    void BaseOArchive<Derived>::store(T& val) {
        static_cast<Derived*>(this)->store_primitive(val);
    }

    template <class Derived>
    template <
        class T,
        typename std::enable_if<
            (!std::is_fundamental<T>::value &&
            !std::is_same<T, std::string>::value),
        int>::type
    >
    void BaseOArchive<Derived>::store(T& val) {
        serialization::serialize(val, *this);
    }

    template <class Derived>
    template <class T>
    Derived& BaseOArchive<Derived>::operator<<(T& val) {
        store<T>(val);
        return static_cast<Derived&>(*this);
    }

    template <class Derived>
    template <class T>
    Derived& BaseOArchive<Derived>::operator&(T& val) {
        return *this << val;
    }

} // namespace diamond

#endif // _DIAMOND_BASE_ARCHIVE_H
