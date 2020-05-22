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

#ifndef _DIAMOND_THREAD_POOL_H
#define _DIAMOND_THREAD_POOL_H

#include <functional>

#include <boost/asio.hpp>

namespace diamond {

    class ThreadPool {
    public:
        ThreadPool() = delete;

        template <class Task>
        static void queue(Task task);

        template <class Task>
        static void queue_repeated(Task task, const boost::posix_time::time_duration& delay);

    private:
        static boost::asio::thread_pool _pool;
        static boost::asio::io_service _service;
    };

    template <class Task>
    void ThreadPool::queue(Task task) {
        boost::asio::post(_pool, task);
    }

    template <class Task>
    void ThreadPool::queue_repeated(Task task, const boost::posix_time::time_duration& delay) {
        std::shared_ptr<boost::asio::deadline_timer> timer =
            std::make_shared<boost::asio::deadline_timer>(_service);
        timer->expires_from_now(delay);
        // TODO(zvp): finish this
    }

} // namespace diamond

#endif // _DIAMOND_THREAD_POOL_H
