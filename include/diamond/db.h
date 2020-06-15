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

#ifndef _DIAMOND_DB_H
#define _DIAMOND_DB_H

#include "diamond/binary_archive.h"
#include "diamond/storage_engine.h"

namespace diamond {

    template <
        class TIArchive = BinaryIArchive,
        class TOArchive = BinaryOArchive
    >
    class Db {
    public:
        Db(StorageEngine& storage_engine);

        template <class T, class TKey>
        T get(TKey key);

        template <class T>
        void insert(T& record);

    private:
        StorageEngine& _storage_engine;

        template <class T>
        static std::string collection_name();
    };

    template <class TIArchive, class TOArchive>
    Db<TIArchive, TOArchive>::Db(StorageEngine& storage_engine)
        : _storage_engine(storage_engine) {}

    template <class TIArchive, class TOArchive>
    template <class T, class TKey>
    T Db<TIArchive, TOArchive>::get(TKey /*key*/) {
        return T();
    }

    template <class TIArchive, class TOArchive>
    template <class T>
    void Db<TIArchive, TOArchive>::insert(T& record) {
        // Buffer key(record.key.size());
        // BufferWriter writer(key);
        // record.key.store(writer);

        Buffer value;
        TOArchive o_archive(value);
        record.serialize(o_archive);

        // _storage_engine.insert(collection_name<T>(), std::move(key), std::move(value));
    }

    template <class TIArchive, class TOArchive>
    template <class T>
    std::string Db<TIArchive, TOArchive>::collection_name() {
        return boost::typeindex::type_id<T>().pretty_name();
    }

} // namespace diamond

#endif // _DIAMOND_DB_H
