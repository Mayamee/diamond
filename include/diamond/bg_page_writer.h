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

#ifndef _DIAMOND_BG_PAGE_WRITER_H
#define _DIAMOND_BG_PAGE_WRITER_H

#include <queue>

#include <boost/thread.hpp>
#include <boost/utility.hpp>

#include "diamond/page_writer.h"
#include "diamond/timer.h"

namespace diamond {

    class BgPageWriter : public PageWriter, boost::noncopyable  {
    public:
        struct Options {
            uint32_t delay = 200;
            uint32_t max_pages = 100;
        };

        BgPageWriter(Storage& storage, const Options& options);
        virtual ~BgPageWriter();

        virtual void write(const std::shared_ptr<const Page>& page) override;

    private:
        bool _timer_running;
        Timer _timer;
        boost::mutex _mutex;
        uint32_t _max_pages;

        struct PendingWrite {
            PendingWrite(Buffer _buffer, uint64_t _pos);

            Buffer buffer;
            uint64_t pos;
        };
        std::queue<PendingWrite> _queue;

        void bg_task();
    };

    class BgPageWriterFactory : public PageWriterFactory {
    public:
        BgPageWriterFactory(const BgPageWriter::Options& options);

        virtual std::shared_ptr<PageWriter> create(Storage& storage) const override;

    private:
        BgPageWriter::Options _options;
    };

} // namespace diamond

#endif // _DIAMOND_BG_PAGE_WRITER_H
