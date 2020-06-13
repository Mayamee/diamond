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

#include "diamond/storage_engine.h"
#include "diamond/value_processors.h"

namespace diamond {

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

    template <class T, class TKey>
    T Db::get(TKey /*key*/) {
        return T();
    }

    template <class T>
    void Db::insert(T& record) {
        Buffer key(record.key.size());
        BufferWriter writer(key);
        record.key.store(writer);

        SizeCalculator size_calculator;
        record.process_values(size_calculator);
        Buffer value(size_calculator.size());
        Serializer serializer(value);
        record.process_values(serializer);

        _storage_engine.insert(collection_name<T>(), std::move(key), std::move(value));
    }

    template <class T>
    std::string Db::collection_name() {
        return boost::typeindex::type_id<T>().pretty_name();
    }

} // namespace diamond

#endif // _DIAMOND_DB_H
