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

    class ExclusivePageAccessor {
    public:
        ~ExclusivePageAccessor();

        const std::shared_ptr<Page>& page() const;

        void unlock();

    private:
        bool _locked;
        std::shared_ptr<Page> _page;
        std::shared_ptr<boost::shared_mutex> _mutex;

        friend class PageManagerPartition;

        ExclusivePageAccessor(
            std::shared_ptr<Page>& page,
            std::shared_ptr<boost::shared_mutex>& mutex);
    };

    class SharedPageAccessor {
    public:
        ~SharedPageAccessor();

        const std::shared_ptr<const Page>& page() const;

        void unlock();

    private:
        bool _locked;
        std::shared_ptr<const Page> _page;
        std::shared_ptr<boost::shared_mutex> _mutex;

        friend class PageManagerPartition;

        SharedPageAccessor(
            std::shared_ptr<Page>& page,
            std::shared_ptr<boost::shared_mutex>& mutex);
    };

} // namespace diamond

#endif // _DIAMOND_PAGE_ACCESSORS_H
