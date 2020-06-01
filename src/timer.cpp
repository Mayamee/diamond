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

#include "diamond/timer.h"

namespace diamond {

    Timer::Timer(
        ThreadPool& thread_pool,
        ThreadPool::Task task,
        const boost::posix_time::time_duration& delay)
        : _task(task),
        _delay(delay),
        _enabled(false),
        _timer(thread_pool.service()) {}

    void Timer::start() {
        _enabled.store(true, std::memory_order::memory_order_release);
        _timer.expires_from_now(_delay);
        _timer.async_wait(
            std::bind(
                &Timer::tick,
                this,
                std::placeholders::_1
            ));
    }

    void Timer::stop() {
        _enabled.store(false, std::memory_order::memory_order_release);
        _timer.cancel();
    }

    void Timer::tick(const boost::system::error_code& error) {
        if (error) return;
        if (!_enabled.load(std::memory_order::memory_order_acquire)) return;
        try {
            _task();
        } catch (...) {}
        if (_enabled.load(std::memory_order::memory_order_acquire)) {
            _timer.expires_from_now(_delay);
            _timer.async_wait(
                std::bind(
                    &Timer::tick,
                    this,
                    std::placeholders::_1
                ));
        }
    }

} // namespace diamond
