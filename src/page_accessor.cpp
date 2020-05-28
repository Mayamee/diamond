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
#include <iostream>
#include "diamond/page_accessor.h"

namespace diamond {

    PageAccessor::~PageAccessor() {
        if (!_locked) return;
        if (_shared) {
            _mutex->unlock_shared();
        } else {
            _mutex->unlock();
        }
    }

    std::shared_ptr<Page>& PageAccessor::page() {
        return _page;
    }

    void PageAccessor::lock() {
        _mutex->lock();
        _shared = false;
        _locked = true;
    }

    void PageAccessor::unlock() {
        _mutex->unlock();
        _locked = false;
    }

    void PageAccessor::lock_shared() {
        _mutex->lock_shared();
        _shared = true;
        _locked = true;
    }

    void PageAccessor::unlock_shared() {
        _mutex->unlock_shared();
        _locked = false;
    }

    bool PageAccessor::locked() const {
        return _locked;
    }

    bool PageAccessor::shared() const {
        return _shared;
    }

    PageAccessor::PageAccessor(
            std::shared_ptr<Page>& page,
            std::shared_ptr<boost::shared_mutex>& mutex,
            Mode mode)
            : _page(page),
            _mutex(mutex) {
        switch (mode) {
        case EXCLUSIVE:
            lock();
            break;
        case SHARED:
            lock_shared();
            break;
        }
    }

} // namespace diamond
