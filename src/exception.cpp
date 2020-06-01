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

#include "diamond/exception.h"

namespace diamond {

    Exception::Exception(ErrorCode code)
        : _code(code),
        _msg(get_msg_for_code(code)) {}

    ErrorCode Exception::code() const {
        return _code;
    }

    const char* Exception::what() const noexcept {
        return _msg.c_str();
    }

    std::string Exception::get_msg_for_code(ErrorCode code) {
        switch (code) {
        case ErrorCode::CORRUPTED_FILE:
            return "database file is corrupted.";
        case ErrorCode::PAGE_DOES_NOT_EXIST:
            return "the requested page does not exist.";
        case ErrorCode::NO_PAGE_SPACE_AVAILABLE:
            return "max page capacity has been reached and there are no unused pages available to evict";
        }
    }

} // namespace diamond
