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

std::vector<Person> people = {
    Person{.first_name="Zach", .last_name="Perkitny", .gender="male", .height=70, .weight=155},
    Person{.first_name="Bob", .last_name="Doe", .gender="male", .height=73, .weight=180},
    Person{.first_name="Jane", .last_name="Doe", .gender="female", .height=62, .weight=130}
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

    for (Person& person : people) {
        std::string key = person.first_name + " " + person.last_name;
        if (!db.exists<Person>(key)) {
            std::cout << key << " does not exist, inserting..." << std::endl;
            db.insert<Person>(key, person);
        } else {
            std::cout << key << " already exists." << std::endl;
        }
    }

    std::cout << "people count: " << db.count<Person>() << std::endl;

    // diamond::Db::Query query = db.query()
    //     .where()
    //     .top(5);
    // diamond::Db::QueryResult result = query.execute();

    return 0;
}
