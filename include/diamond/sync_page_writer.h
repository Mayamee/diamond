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

#ifndef _DIAMOND_SYNC_WRITER_H
#define _DIAMOND_SYNC_WRITER_H

#include "diamond/page_writer.h"

namespace diamond {

    class SyncPageWriter : public PageWriter {
    public:
        SyncPageWriter(Storage& storage);

        virtual void write(const std::shared_ptr<Page>& page) override;
    };

    class SyncPageWriterFactory : public PageWriterFactory {
    public:
        virtual std::shared_ptr<PageWriter> create(Storage& storage) const override;
    };

} // namespace diamond

#endif // _DIAMOND_SYNC_WRITER_H
