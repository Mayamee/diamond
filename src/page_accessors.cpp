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

#include "diamond/page_accessors.h"

namespace diamond {

    ExclusivePageAccessor::~ExclusivePageAccessor() {
        if (_locked) _mutex->unlock();
    }

    const std::shared_ptr<Page>& ExclusivePageAccessor::page() const {
        return _page;
    }

    void ExclusivePageAccessor::unlock() {
        if (!_locked) return;
        _mutex->unlock();
        _locked = false;
    }

    ExclusivePageAccessor::ExclusivePageAccessor(
            std::shared_ptr<Page>& page,
            std::shared_ptr<boost::shared_mutex>& mutex)
            : _page(page),
            _mutex(mutex) {
        _mutex->lock();
        _locked = true;
    }

    SharedPageAccessor::~SharedPageAccessor() {
        if (_locked) _mutex->unlock_shared();
    }

    const std::shared_ptr<const Page>& SharedPageAccessor::page() const {
        return _page;
    }

    void SharedPageAccessor::unlock() {
        if (!_locked) return;
        _mutex->unlock_shared();
        _locked = false;
    }

    SharedPageAccessor::SharedPageAccessor(
            std::shared_ptr<Page>& page,
            std::shared_ptr<boost::shared_mutex>& mutex)
            : _page(page),
            _mutex(mutex) {
        _mutex->lock_shared();
        _locked = true;
    }

} // namespace diamond
