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

#ifndef _DIAMOND_PAGE_ACCESSORS_H
#define _DIAMOND_PAGE_ACCESSORS_H

#include <boost/thread.hpp>

#include "diamond/page.h"

namespace diamond {

    class PageManagerPartition;

    class PageAccessor {
    public:
        enum Mode {
            EXCLUSIVE,
            SHARED
        };

        ~PageAccessor();

        std::shared_ptr<Page>& page();

        void lock();
        void unlock();

        void lock_shared();
        void unlock_shared();

        bool locked() const;
        bool shared() const;

    private:
        bool _locked;
        bool _shared;
        std::shared_ptr<Page> _page;
        std::shared_ptr<boost::shared_mutex> _mutex;

        friend class PageManagerPartition;

        PageAccessor(
            std::shared_ptr<Page>& page,
            std::shared_ptr<boost::shared_mutex>& mutex,
            Mode mode);
    };

} // namespace diamond

#endif // _DIAMOND_PAGE_ACCESSORS_H
