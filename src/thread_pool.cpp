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

#include "diamond/thread_pool.h"

namespace diamond {

    ThreadPool::ThreadPool(size_t threads)
        : _work(_service) {
        for (size_t i = 0; i < threads; i++) {
            _thread_group.create_thread([&]() { _service.run(); });
        }
    }

    ThreadPool::~ThreadPool() {
        _service.stop();
        _thread_group.join_all();
    }

    void ThreadPool::queue(Task task) {
        _service.post(task);
    }

    boost::asio::io_service& ThreadPool::service() {
        return _service;
    }

} // namespace diamond
