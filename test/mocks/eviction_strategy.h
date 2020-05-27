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

#include "diamond/eviction_strategy.h"

namespace {

    class MockEvictionStrategy : public diamond::EvictionStrategy {
    public:
        MOCK_METHOD(diamond::Page::ID, evict, (), (override));
        MOCK_METHOD(void, use, (diamond::Page::ID id), (override));
        MOCK_METHOD(void, track, (diamond::Page::ID id), (override));
    };

    class MockEvictionStrategyFactory : public diamond::EvictionStrategyFactory {
    public:
        MOCK_METHOD(std::shared_ptr<diamond::EvictionStrategy>, create, (), (const override));
    };

} // namespace 
