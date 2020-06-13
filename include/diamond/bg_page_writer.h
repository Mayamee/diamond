/*  Diamond - Embedded NoSQL Database
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

#ifndef _DIAMOND_BG_PAGE_WRITER_H
#define _DIAMOND_BG_PAGE_WRITER_H

#include <atomic>
#include <list>
#include <unordered_map>

#include <boost/thread.hpp>

#include "diamond/page_writer.h"

namespace diamond {

    class BgPageWriterQueue;

    class BgPageWriter : public PageWriter, boost::noncopyable  {
    public:
        BgPageWriter(BgPageWriterQueue& queue);

        virtual void write(const Page* page) override;

    private:
        BgPageWriterQueue& _queue;
    };

    class BgPageWriterQueue final : boost::noncopyable {
    public:
        static const uint64_t DELAY = 500;
        static const size_t BATCH_SIZE = 100;

        BgPageWriterQueue(Storage& storage);
        ~BgPageWriterQueue();

        void enqueue_write(const Page* page);

    private:
        Storage& _storage;

        std::atomic_bool _stop;

        boost::thread _thread;
        boost::mutex _mutex;

        struct BatchItem {
            BatchItem(Buffer _buffer, uint64_t _pos);

            Buffer buffer;
            uint64_t pos;
        };

        using Batch = std::unordered_map<Page::ID, BatchItem>;

        std::list<Batch> _batches;
        std::list<Batch>::iterator _current_batch;

        void bg_task();
    };

    class BgPageWriterFactory : public PageWriterFactory {
    public:
        BgPageWriterFactory(BgPageWriterQueue& queue);

        virtual std::shared_ptr<PageWriter> create() const override;

    private:
        BgPageWriterQueue& _queue;
    };

} // namespace diamond

#endif // _DIAMOND_BG_PAGE_WRITER_H
