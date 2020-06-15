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

#ifndef _DIAMOND_PAGE_MANAGER_H
#define _DIAMOND_PAGE_MANAGER_H

#include <atomic>

#include "diamond/page_accessor.h"
#include "diamond/utility.h"

namespace diamond {

    class PageManager : noncopyable {
    public:
        PageManager(Storage& storage);

        virtual PageAccessor create_page(Page::Type type, PageAccessor::Mode access_mode) = 0;
        virtual PageAccessor get_page(Page::ID id, PageAccessor::Mode access_mode) = 0;
        virtual void write_page(const Page* page) = 0;
        virtual bool is_page_managed(Page::ID id) const = 0;

        Storage& storage() const;

    protected:
        Storage& _storage;
        std::atomic<Page::ID> _next_page_id;
    };

} // namespace diamond

#endif // _DIAMOND_PAGE_MANAGER_H
