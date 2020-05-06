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

#ifndef _DIAMOND_ENDIAN_H
#define _DIAMOND_ENDIAN_H

#ifdef _WIN32
#include <winsock2.h>
#   define htobe16(x) htons(x)
#   define htole16(x) x
#   define be16toh(x) ntohs(x)
#   define le16toh(x) x
#   define htobe32(x) htonl(x)
#   define htole32(x) x
#   define be32toh(x) ntohl(x)
#   define le32toh(x) x
#   define htobe64(x) htonll(x)
#   define htole64(x) x
#   define be64toh(x) ntohll(x)
#   define le64toh(x) x
#else
#include <endian.h>
#endif

namespace diamond {
namespace endian {

    enum Endianness {
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

} // namespace endian
} // namespace diamond

#endif // _DIAMOND_ENDIAN_H
