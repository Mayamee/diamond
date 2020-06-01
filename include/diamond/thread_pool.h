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
#include <boost/thread.hpp>

namespace diamond {

    class ThreadPool {
    public:
        using Task = std::function<void()>;

        ThreadPool(size_t threads = boost::thread::hardware_concurrency());
        ~ThreadPool();

        void queue(Task task);

        boost::asio::io_service& service();

    private:
        boost::thread_group _thread_group;
        boost::asio::io_service _service;
        boost::asio::io_service::work _work;
    };

} // namespace diamond

#endif // _DIAMOND_THREAD_POOL_H
