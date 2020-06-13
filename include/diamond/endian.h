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

#ifndef _DIAMOND_ENDIAN_H
#define _DIAMOND_ENDIAN_H

#include <algorithm>
#include <array>
#include <type_traits>

namespace diamond {
namespace endian {

    enum class Endianness {
        BIG,
        LITTLE
    };

#ifdef _WIN32
    const Endianness HOST_ORDER = Endianness::LITTLE;
#else
    static constexpr Endianness get_host_order() {
        int n = 1;
        if (*(char*)&n == 1) {
            return Endianness::LITTLE;
        }
        
        return Endianness::BIG;
    }
    const Endianness HOST_ORDER = get_host_order();
#endif

    template <class T>
    void swap_endianness(
            T val,
            typename std::enable_if<std::is_arithmetic<T>::value, std::nullptr_t>::type = nullptr) {
        union U {
            T val;
            std::array<std::uint8_t, sizeof(T)> raw;
        } src, dst;

        src.val = val;
        std::reverse_copy(src.raw.begin(), src.raw.end(), dst.raw.begin());
        val = dst.val;
    }

} // namespace endian
} // namespace diamond

#endif // _DIAMOND_ENDIAN_H
