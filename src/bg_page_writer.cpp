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

    void BgPageWriter::write(const std::shared_ptr<const Page>& page) {
        Buffer buffer(Page::SIZE);
        page->write_to_buffer(buffer);
        {
            boost::lock_guard<boost::mutex> lock(_mutex);
            _pending_writes.insert_or_assign(page->get_id(), std::move(buffer));
            if (!_timer_running) {
                _timer.start();
                _timer_running = true;
            }
        }
    }

    void BgPageWriter::bg_task() {
        using Write = std::tuple<Page::ID, Buffer>;
        std::vector<Write> writes_in_current_cycle;
        {
            boost::lock_guard<boost::mutex> lock(_mutex);
            size_t pages_written = 0;
            if (_pending_writes_iter == _pending_writes.end()) {
                _pending_writes_iter = _pending_writes.begin();
            }
            while (_pending_writes_iter != _pending_writes.end() &&
                    pages_written < _max_pages) {
                writes_in_current_cycle.emplace_back(
                    (*_pending_writes_iter).first,
                    std::move((*_pending_writes_iter).second));
                _pending_writes_iter = _pending_writes.erase(_pending_writes_iter);
                pages_written++;
            }
            if (_pending_writes.size() == 0) {
                _timer.stop();
                _timer_running = false;
            }
        }

        // TODO(zvp): group pending writes for vectored io
        size_t n = writes_in_current_cycle.size();
        for (size_t i = 0; i < n; i++) {
            const Write& write = writes_in_current_cycle.at(i);
            _storage.seek(Page::file_pos_for_id(std::get<0>(write)));
            _storage.write(std::get<1>(write));
        }
    }

    BgPageWriterFactory::BgPageWriterFactory(const BgPageWriter::Options& options)
        : _options(options) {}

    std::shared_ptr<PageWriter> BgPageWriterFactory::create(Storage& storage) const {
        return std::make_shared<BgPageWriter>(storage, _options);
    }

} // namespace diamond
