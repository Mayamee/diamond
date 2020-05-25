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

#ifndef _DIAMOND_TIMER_H
#define _DIAMOND_TIMER_H

#include <atomic>

#include <boost/utility.hpp>

#include "diamond/thread_pool.h"

namespace diamond {

    class Timer : boost::noncopyable {
    public:
        Timer(
            ThreadPool::Task task,
            const boost::posix_time::time_duration& delay);

        void start();
        void stop();

    private:
        ThreadPool::Task _task;
        boost::posix_time::time_duration _delay;
        std::atomic_bool _enabled;
        boost::asio::deadline_timer _timer;

        void tick();
    };

} // namespace diamond

#endif // _DIAMOND_TIMER_H
