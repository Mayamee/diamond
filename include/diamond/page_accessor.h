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

#ifndef _DIAMOND_PAGE_ACCESSORS_H
#define _DIAMOND_PAGE_ACCESSORS_H

#include <boost/thread.hpp>

#include "diamond/page.h"
#include "diamond/utility.h"

namespace diamond {

    class PageAccessor {
    public:
        PageAccessor(Page* page);
        PageAccessor(const PageAccessor& other);
        PageAccessor(PageAccessor&& other);
        ~PageAccessor();

        Page* instance() const;

        Page* operator->() const;

    private:
        Page* _page;
    };

    class SharedPageLock : noncopyable {
    public:
        SharedPageLock(PageAccessor& page);
        ~SharedPageLock();

        void lock();
        void unlock();

    private:
        PageAccessor& _page;
        bool _locked;
    };

    class UniquePageLock : noncopyable {
    public:
        UniquePageLock(PageAccessor& page);
        ~UniquePageLock();

        void lock();
        void unlock();

    private:
        PageAccessor& _page;
        bool _locked;
    };

    class UpgradePageLock : noncopyable {
    public:
        UpgradePageLock(PageAccessor& page);
        ~UpgradePageLock();

        void lock();
        void unlock();

        void downgrade();
        void upgrade();

    private:
        PageAccessor& _page;

        bool _locked;
        enum State {
            start,
            downgraded,
            upgraded
        } _state;
    };

} // namespace diamond

#endif // _DIAMOND_PAGE_ACCESSORS_H
