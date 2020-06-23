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
        template <class T>
        class Query {
        public:
            using Condition = std::function<bool(const T&)>;

            Query& where(Condition condition);
            Query& top(uint64_t n);

            std::vector<T> execute();

        private:
            friend class Db;

            StorageEngine& _storage_engine;

            Condition _condition;
            uint64_t _top;

            Query(StorageEngine& storage_engine);
        };

        Db(StorageEngine& storage_engine);

        template <class T>
        bool exists(const Buffer& key);

        template <class T>
        uint64_t count();

        template <class T>
        T get(const Buffer& key);

        template <class T>
        void put(Buffer key, T& record);

        template <class T>
        Query<T> query();

    private:
        StorageEngine& _storage_engine;

        template <class T>
        static std::string collection_name();
    };

    template <class TIArchive, class TOArchive>
    Db<TIArchive, TOArchive>::Db(StorageEngine& storage_engine)
        : _storage_engine(storage_engine) {}

    template <class TIArchive, class TOArchive>
    template <class T>
    uint64_t Db<TIArchive, TOArchive>::count() {
        return _storage_engine.count(collection_name<T>());
    }

    template <class TIArchive, class TOArchive>
    template <class T>
    bool Db<TIArchive, TOArchive>::exists(const Buffer& key) {
        return _storage_engine.exists(collection_name<T>(), key);
    }

    template <class TIArchive, class TOArchive>
    template <class T>
    T Db<TIArchive, TOArchive>::get(const Buffer& key) {
        Buffer value = _storage_engine.get(collection_name<T>(), key);
        T obj;
        TIArchive i_archive(value);
        i_archive >> obj;
        return obj;
    }

    template <class TIArchive, class TOArchive>
    template <class T>
    void Db<TIArchive, TOArchive>::put(Buffer key, T& record) {;
        Buffer value;
        TOArchive o_archive(value);
        o_archive << record;
        _storage_engine.put(collection_name<T>(), std::move(key), std::move(value));
    }

    template <class TIArchive, class TOArchive>
    template <class T>
    Db<TIArchive, TOArchive>::Query<T> Db<TIArchive, TOArchive>::query() {
        return Query<T>(_storage_engine);
    }

    template <class TIArchive, class TOArchive>
    template <class T>
    std::string Db<TIArchive, TOArchive>::collection_name() {
        return boost::typeindex::type_id<T>().pretty_name();
    }

    template <class TIArchive, class TOArchive>
    template <class T>
    Db<TIArchive, TOArchive>::Query<T>::Query(StorageEngine& storage_engine)
        : _storage_engine(storage_engine),
        _top(0) {}

    template <class TIArchive, class TOArchive>
    template <class T>
    Db<TIArchive, TOArchive>::Query<T>&
    Db<TIArchive, TOArchive>::Query<T>::where(Condition condition) {
        _condition = condition;
        return *this;
    }

    template <class TIArchive, class TOArchive>
    template <class T>
    Db<TIArchive, TOArchive>::Query<T>&
    Db<TIArchive, TOArchive>::Query<T>::top(uint64_t n) {
        if (n == 0) throw std::invalid_argument("top must be greater than 0");
        _top = n;
        return *this;
    }

    template <class TIArchive, class TOArchive>
    template <class T>
    std::vector<T>
    Db<TIArchive, TOArchive>::Query<T>::execute() {
        std::vector<T> result;
        StorageEngine::Iterator iter = _storage_engine.get_iterator(
            collection_name<T>());
        while (!iter.end()) {
            Buffer value = iter.val();
            T obj;
            TIArchive i_archive(value);
            i_archive >> obj;
            if (_condition(obj)) {
                result.push_back(obj);
                if (_top && result.size() == _top) break;
            }
            iter.next();
        }

        return result;
    }

} // namespace diamond

#endif // _DIAMOND_DB_H
