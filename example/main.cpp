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

#include <iostream>

#include "diamond/bg_page_writer.h"
#include "diamond/db.h"
#include "diamond/lru_eviction_strategy.h"
#include "diamond/file_storage.h"

struct Person {
    diamond::Value<std::string> key;
    diamond::Value<std::string> first_name;
    diamond::Value<std::string> last_name;
    diamond::Value<std::string> gender;
    diamond::Value<uint8_t> height;
    diamond::Value<uint16_t> weight;

    template <class ValueProcessor>
    void process_values(ValueProcessor processor) {
        processor & first_name;
        processor & last_name;
        processor & gender;
        processor & height;
        processor & weight;
    }
};

int main() {

    diamond::FileStorage storage("data");
    diamond::BgPageWriterQueue page_writer_queue(storage);
    diamond::BgPageWriterFactory page_writer_factory(page_writer_queue);
    diamond::LRUEvictionStrategyFactory eviction_strategy_factory;
    diamond::PageManager manager(
        storage,
        page_writer_factory,
        eviction_strategy_factory);
    diamond::StorageEngine engine(manager);
    diamond::Db db(engine);
    {
        Person me;
        me.key = "zach-perkitny";
        me.first_name = "zach";
        me.last_name = "perkitny";
        me.gender = "male";
        me.height = 70;
        me.weight = 155;

        db.insert<Person>(me);
    }

    {
        Person me = db.get<Person>("zach-perkitny");
    }

    return 0;
}
