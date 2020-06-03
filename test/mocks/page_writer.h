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

#include "diamond/page_writer.h"

namespace {

    class MockPageWriter : public diamond::PageWriter {
    public:
        MOCK_METHOD(void, write, (const diamond::Page& page), (override));
    };

    class MockPageWriterFactory : public diamond::PageWriterFactory {
    public:
        MOCK_METHOD(std::shared_ptr<diamond::PageWriter>, create, (), (const override));
    };

} // namespace
