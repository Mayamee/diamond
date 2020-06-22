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

#include "diamond/page_accessor.h"

namespace diamond {

    PageAccessor::PageAccessor(Page* page)
            : _page(page) {
        _page->_usage_count++;
    }

    PageAccessor::PageAccessor(const PageAccessor& other)
            : _page(other._page) {
        _page->_usage_count++;
    }

    PageAccessor::PageAccessor(PageAccessor&& other)
            : _page(other._page) {
        other._page = nullptr;
    }

    PageAccessor::~PageAccessor() {
        if (!_page) return;
        _page->_usage_count--;
    }

    Page* PageAccessor::instance() const {
        return _page;
    }

    Page* PageAccessor::operator->() const {
        return _page;
    }

    SharedPageLock::SharedPageLock(PageAccessor& page)
            : _page(page) {
        lock();
    }

    SharedPageLock::~SharedPageLock() {
        unlock();
    }

    void SharedPageLock::lock() {
        if (_locked) return;
        _page->_mutex.lock_shared();
        _locked = true;
    }

    void SharedPageLock::unlock() {
        if (!_locked) return;
        _page->_mutex.unlock_shared();
        _locked = false;
    }

    UniquePageLock::UniquePageLock(PageAccessor& page)
            : _page(page) {
        lock();
    }

    UniquePageLock::~UniquePageLock() {
        unlock();
    }

    void UniquePageLock::lock() {
        if (_locked) return;
        _page->_mutex.lock();
        _locked = true;
    }

    void UniquePageLock::unlock() {
        if (!_locked) return;
        _page->_mutex.unlock();
        _locked = false;
    }

    UpgradePageLock::UpgradePageLock(PageAccessor& page)
            : _page(page) {
        lock();
    }

    UpgradePageLock::~UpgradePageLock() {
        unlock();
    }

    void UpgradePageLock::lock() {
        if (_locked) return;
        _page->_mutex.lock_upgrade();
        _locked = true;
        _state = State::start;
    }

    void UpgradePageLock::unlock() {
        if (!_locked) return;
        switch (_state) {
        case State::start:
            _page->_mutex.unlock_upgrade();
            break;
        case State::downgraded:
            _page->_mutex.unlock_shared();
            break;
        case State::upgraded:
            _page->_mutex.unlock();
            break;
        }
        _locked = false;
    }

    void UpgradePageLock::downgrade() {
        if (!_locked || _state != State::start) return;
        _page->_mutex.unlock_upgrade_and_lock_shared();
        _state = State::downgraded;
    }

    void UpgradePageLock::upgrade() {
        if (!_locked || _state != State::start) return;
        _page->_mutex.unlock_upgrade_and_lock();
        _state = State::upgraded;
    }

} // namespace diamond
