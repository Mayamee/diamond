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
        switch (_mode) {
        case PageAccessorMode::EXCLUSIVE:
            _mutex->unlock();
            break;
        case PageAccessorMode::SHARED:
            _mutex->unlock_shared();
            break;
        case PageAccessorMode::UPGRADE:
            _mutex->unlock_upgrade();
            break;
        }
    }

    Page& PageAccessor::page() {
        return _page;
    }

    void PageAccessor::lock() {
        _mutex->lock();
        _mode = PageAccessorMode::EXCLUSIVE;
        _locked = true;
    }

    void PageAccessor::unlock() {
        _mutex->unlock();
        _locked = false;
    }

    void PageAccessor::lock_shared() {
        _mutex->lock_shared();
        _mode = PageAccessorMode::SHARED;
        _locked = true;
    }

    void PageAccessor::unlock_shared() {
        _mutex->unlock_shared();
        _locked = false;
    }

    void PageAccessor::lock_upgrade() {
        _mutex->lock_upgrade();
        _mode = PageAccessorMode::UPGRADE;
        _locked = true;
    }

    void PageAccessor::unlock_upgrade() {
        _mutex->unlock_upgrade();
        _locked = false;
    }

    void PageAccessor::upgrade_lock() {
        _mutex->unlock_upgrade_and_lock();
        _mode = PageAccessorMode::EXCLUSIVE;
    }

    bool PageAccessor::locked() const {
        return _locked;
    }

    PageAccessorMode PageAccessor::mode() const {
        return _mode;
    }

    PageAccessor::PageAccessor(
            Page& page,
            std::shared_ptr<boost::shared_mutex>& mutex,
            PageAccessorMode mode)
            : _page(page),
            _mutex(mutex) {
        switch (mode) {
        case PageAccessorMode::EXCLUSIVE:
            lock();
            break;
        case PageAccessorMode::SHARED:
            lock_shared();
            break;
        case PageAccessorMode::UPGRADE:
            lock_upgrade();
            break;
        }
    }

} // namespace diamond
