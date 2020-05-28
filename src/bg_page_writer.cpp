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

#include <algorithm>

#include "diamond/bg_page_writer.h"
#include "diamond/thread_pool.h"

namespace diamond {

    BgPageWriter::BgPageWriter(Storage& storage, const Options& options)
        : PageWriter(storage),
        _timer_running(false),
        _timer(
            std::bind(&BgPageWriter::bg_task, this),
            boost::posix_time::milliseconds(options.delay)),
        _max_pages(options.max_pages) {}

    BgPageWriter::~BgPageWriter() {
        while (_queue.size()) {
            PendingWrite& pending_write = _queue.front();
            pending_write.buffer.write_to_storage(
                _storage,
                pending_write.pos);
            _queue.pop();
        }
    }

    void BgPageWriter::write(const std::shared_ptr<const Page>& page) {
        Buffer buffer(Page::SIZE);
        page->write_to_buffer(buffer);
        {
            boost::lock_guard<boost::mutex> lock(_mutex);
            _queue.emplace(std::move(buffer), page->file_pos());
            if (!_timer_running) {
                _timer.start();
                _timer_running = true;
            }
        }
    }

    void BgPageWriter::bg_task() {
        Buffer buffer;
        uint64_t pos;
        {
            boost::lock_guard<boost::mutex> lock(_mutex);
            PendingWrite& pending_write = _queue.front();
            buffer = std::move(pending_write.buffer);
            pos = pending_write.pos;
            _queue.pop();
            if (_queue.size() == 0) {
                _timer.stop();
                _timer_running = false;
            }
        }

        buffer.write_to_storage(_storage, pos);
    }

    BgPageWriter::PendingWrite::PendingWrite(Buffer _buffer, uint64_t _pos)
        : buffer(std::move(_buffer)), 
        pos(_pos) {}

    BgPageWriterFactory::BgPageWriterFactory(const BgPageWriter::Options& options)
        : _options(options) {}

    std::shared_ptr<PageWriter> BgPageWriterFactory::create(Storage& storage) const {
        return std::make_shared<BgPageWriter>(storage, _options);
    }

} // namespace diamond
