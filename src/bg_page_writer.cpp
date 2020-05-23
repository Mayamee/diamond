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
#include "diamond/thread_pool.h"

namespace diamond {

    BgPageWriter::BgPageWriter(Storage& storage, const Options& options)
        : PageWriter(storage),
        _options(options),
        _timer_running(false) {}

    void BgPageWriter::write(const std::shared_ptr<Page>& page) {
        Buffer buffer(Page::SIZE);
        page->write_to_buffer(buffer);
        {
            boost::lock_guard<boost::mutex> lock(_mutex);
            _pending_writes.push_back(std::move(buffer));
            if (!_timer_running) {
                ThreadPool::queue_repeated(
                    &BgPageWriter::bg_task,
                    boost::posix_time::milliseconds(_options.delay));
                _timer_running = true;
            }
        }
    }

    void BgPageWriter::bg_task() {}

    BgPageWriterFactory::BgPageWriterFactory(const BgPageWriter::Options& options)
        : _options(options) {}

    std::shared_ptr<PageWriter> BgPageWriterFactory::create(Storage& storage) const {
        return std::make_shared<BgPageWriter>(storage, _options);
    }

} // namespace diamond
