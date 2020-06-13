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

    PageAccessor::PageAccessor(PageAccessor&& other)
            : _locked(other._locked),
            _mode(other._mode),
            _page(other._page) {
        other._page = nullptr;
        other._locked = false;
    }

    PageAccessor::~PageAccessor() {
        if (!_page) return;
        if (_locked) {
            switch (_mode) {
            case Mode::EXCLUSIVE:
                _page->_mutex.unlock();
                break;
            case Mode::SHARED:
                _page->_mutex.unlock_shared();
                break;
            case Mode::UPGRADE:
                _page->_mutex.unlock_upgrade();
                break;
            }
        }
        _page->_usage_count--;
    }

    Page* PageAccessor::page() const {
        return _page;
    }

    void PageAccessor::lock() {
        _page->_mutex.lock();
        _mode = Mode::EXCLUSIVE;
        _locked = true;
    }

    void PageAccessor::unlock() {
        _page->_mutex.unlock();
        _locked = false;
    }

    void PageAccessor::lock_shared() {
        _page->_mutex.lock_shared();
        _mode = Mode::SHARED;
        _locked = true;
    }

    void PageAccessor::unlock_shared() {
        _page->_mutex.unlock_shared();
        _locked = false;
    }

    void PageAccessor::lock_upgrade() {
        _page->_mutex.lock_upgrade();
        _mode = Mode::UPGRADE;
        _locked = true;
    }

    void PageAccessor::unlock_upgrade() {
        _page->_mutex.unlock_upgrade();
        _locked = false;
    }

    void PageAccessor::upgrade_lock() {
        _page->_mutex.unlock_upgrade_and_lock();
        _mode = Mode::EXCLUSIVE;
    }

    bool PageAccessor::locked() const {
        return _locked;
    }

    PageAccessor::Mode PageAccessor::mode() const {
        return _mode;
    }

    Page* PageAccessor::operator->() const {
        return _page;
    }

    PageAccessor::PageAccessor(Page* page, Mode mode)
            : _page(page) {
        _page->_usage_count++;
        switch (mode) {
        case Mode::EXCLUSIVE:
            lock();
            break;
        case Mode::SHARED:
            lock_shared();
            break;
        case Mode::UPGRADE:
            lock_upgrade();
            break;
        }
    }

} // namespace diamond
