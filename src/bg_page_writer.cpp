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

namespace diamond {

    BgPageWriter::BgPageWriter(BgPageWriterQueue& queue)
        : _queue(queue) {}

    void BgPageWriter::write(const Page& page) {
        _queue.enqueue_write(page);
    }

    BgPageWriterQueue::BgPageWriterQueue(Storage& storage)
        : _storage(storage),
        _stop(false),
        _thread(std::bind(&BgPageWriterQueue::bg_task, this)),
        _current_batch(_batches.end()) {}

    BgPageWriterQueue::~BgPageWriterQueue() {
        _stop = true;
        _thread.join();
        for (const Batch& batch : _batches) {
            for (const auto& [_, batch_item] : batch) {
                batch_item.buffer.write_to_storage(
                    _storage,
                    batch_item.pos);
            }
        }
    }

    void BgPageWriterQueue::enqueue_write(const Page& page) {
        Buffer buffer(PAGE_SIZE);
        page->write_to_buffer(buffer);
        boost::unique_lock<boost::mutex> lock(_mutex);
        if (_current_batch == _batches.end()) {
            _current_batch = _batches.emplace(_batches.begin());
        }
        (*_current_batch).insert_or_assign(
            page->get_id(),
            std::move(BatchItem(std::move(buffer), page->file_pos())));
        if ((*_current_batch).size() >= BATCH_SIZE) {
            _current_batch++;
        }
    }

    void BgPageWriterQueue::bg_task() {
        while (!_stop) {
            boost::this_thread::sleep_for(
                boost::chrono::milliseconds(DELAY));
            if (_stop) break;
            Batch batch;
            {
                boost::unique_lock<boost::mutex> lock(_mutex);
                if (_current_batch == _batches.end()) continue;
                if ((*_current_batch).size() == 0) continue;
                batch = std::move(*_current_batch);
                _current_batch = _batches.erase(_current_batch);
            }
            for (const auto &[_, batch_item] : batch) {
                batch_item.buffer.write_to_storage(
                    _storage,
                    batch_item.pos);
            }
        }
    }

    BgPageWriterQueue::BatchItem::BatchItem(Buffer _buffer, uint64_t _pos)
        : buffer(std::move(_buffer)), 
        pos(_pos) {}

    BgPageWriterFactory::BgPageWriterFactory(BgPageWriterQueue& queue)
        : _queue(queue) {}

    std::shared_ptr<PageWriter> BgPageWriterFactory::create() const {
        return std::make_shared<BgPageWriter>(_queue);
    }

} // namespace diamond
