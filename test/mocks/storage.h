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

#include "gmock/gmock.h"

#include "diamond/storage.h"

namespace {

    class MockStorage : public diamond::Storage {
    public:
        MockStorage() = default;

        MOCK_METHOD(void, write_impl, (const char* buffer, size_t n), (override));
        MOCK_METHOD(void, read_impl, (char* buffer, size_t n), (override));
        MOCK_METHOD(void, seek_impl, (size_t n), (override));
        MOCK_METHOD(uint64_t, size_impl, (), (override));
    };

} // namespace
