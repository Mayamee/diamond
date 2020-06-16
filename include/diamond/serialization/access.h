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

#ifndef _DIAMOND_SERIALIZATION_ACCESS_H
#define _DIAMOND_SERIALIZATION_ACCESS_H

namespace diamond {
namespace serialization {

    class Access {
    public:
        template <class T, class Archive>
        static void serialize(T& val, Archive& archive);
    };

    template <class T, class Archive>
    void Access::serialize(T& val, Archive& archive) {
        val.serialize(archive);
    }

} // namespace serialization
} // namespace diamond

#endif // _DIAMOND_SERIALIZATION_ACCESS_H
