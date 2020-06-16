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
#include "diamond/lru_eviction_policy.h"
#include "diamond/file_storage.h"
#include "diamond/partitioned_page_manager.h"

class Person {
public:
    std::string first_name;
    std::string last_name;
    std::string gender;
    uint8_t height;
    uint16_t weight;

private:
    friend class diamond::serialization::Access;

    template <class Archive>
    void serialize(Archive& archive) {
        archive & first_name;
        archive & last_name;
        archive & gender;
        archive & height;
        archive & weight;
    }
};

int main() {

    diamond::FileStorage storage("data");
    diamond::BgPageWriterQueue page_writer_queue(storage);
    diamond::BgPageWriterFactory page_writer_factory(page_writer_queue);
    diamond::LRUEvictionPolicyFactory eviction_policy_factory;
    diamond::PartitionedPageManager manager(
        storage,
        page_writer_factory,
        eviction_policy_factory);
    diamond::StorageEngine engine(manager);
    diamond::Db db(engine);
    std::string key = "zach-perkitny";
    {
        Person me;
        me.first_name = "zach";
        me.last_name = "perkitny";
        me.gender = "male";
        me.height = 70;
        me.weight = 155;

        db.insert<Person>(key, me);
    }

    {
        Person me = db.get<Person>(key);
    }

    return 0;
}
