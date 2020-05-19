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

#ifndef _DIAMOND_EXCEPTION_H
#define _DIAMOND_EXCEPTION_H

#include <exception>
#include <string>

namespace diamond {

    class Exception : public std::exception {
    public:
        enum class Reason {
            CORRUPTED_FILE,
            NO_SUCH_PAGE
        };

        Exception(Reason reason);

        Reason reason() const;

    private:
        std::string get_msg_for_reason(Reason reason);

        Reason _reason;
    };

} // namespace diamond

#endif // _DIAMOND_EXCEPTION_H
