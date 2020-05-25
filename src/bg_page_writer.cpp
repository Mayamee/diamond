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

#include "diamond/bg_page_writer.h"
#include "diamond/storage.h"
#include "diamond/thread_pool.h"

namespace diamond {

    BgPageWriter::BgPageWriter(Storage& storage, const Options& options)
        : PageWriter(storage),
        _timer_running(false),
        _timer(
            std::bind(&BgPageWriter::bg_task, this),
            boost::posix_time::milliseconds(options.delay)),
        _max_pages(options.max_pages) {}

    void BgPageWriter::write(const std::shared_ptr<Page>& page) {
        Buffer buffer(Page::SIZE);
        page->write_to_buffer(buffer);
        {
            boost::lock_guard<boost::mutex> lock(_mutex);
            PendingWrite pending_write = PendingWrite(std::move(buffer), page->file_pos());
            _pending_writes.insert_or_assign(page->get_id(), std::move(pending_write));
            if (!_timer_running) {
                _timer.start();
                _timer_running = true;
            }
        }
    }

    void BgPageWriter::bg_task() {
        boost::lock_guard<boost::mutex> lock(_mutex);
        // TODO(zvp): group pending writes for vectored io
        size_t pages_written = 0;
        if (_pending_writes_iter == _pending_writes.end()) {
            _pending_writes_iter = _pending_writes.begin();
        }
        while (_pending_writes_iter != _pending_writes.end() &&
                pages_written < _max_pages) {
            PendingWrite& pending_write = (*_pending_writes_iter).second;
            _storage.seek(pending_write.pos);
            _storage.write(pending_write.data);
            _pending_writes_iter = _pending_writes.erase(_pending_writes_iter);
            pages_written++;
        }
        if (_pending_writes.size() == 0) _timer.stop();
    }

    BgPageWriter::PendingWrite::PendingWrite(Buffer _data, uint64_t _pos)
        : data(std::move(_data)),
        pos(_pos) {}

    BgPageWriterFactory::BgPageWriterFactory(const BgPageWriter::Options& options)
        : _options(options) {}

    std::shared_ptr<PageWriter> BgPageWriterFactory::create(Storage& storage) const {
        return std::make_shared<BgPageWriter>(storage, _options);
    }

} // namespace diamond
