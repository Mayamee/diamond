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

#ifndef _DIAMOND_UTILITY_H
#define _DIAMOND_UTILITY_H

namespace diamond {

    class noncopyable {
    protected:
        noncopyable() {}
        noncopyable(noncopyable&&) {}
        ~noncopyable() {}

    private:
        noncopyable( const noncopyable& );
        const noncopyable& operator=( const noncopyable& );
    };

} // namespace diamond

#endif // _DIAMOND_UTILITY_H
