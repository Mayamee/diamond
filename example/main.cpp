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
    uint8_t age;
    uint8_t height;
    uint16_t weight;

private:
    friend class diamond::serialization::Access;

    template <class Archive>
    void serialize(Archive& archive) {
        archive & first_name;
        archive & last_name;
        archive & gender;
        archive & age;
        archive & height;
        archive & weight;
    }
};

std::vector<Person> people = {
    Person{.first_name="Zach", .last_name="Perkitny", .gender="male", .age=22, .height=70, .weight=155},
    Person{.first_name="Bob", .last_name="Doe", .gender="male", .age=30, .height=73, .weight=180},
    Person{.first_name="Jane", .last_name="Doe", .gender="female", .age=29, .height=62, .weight=130},
    Person{.first_name="Zach", .last_name="Doe", .gender="male", .age=31, .height=74, .weight=195},
    Person{.first_name="Pop", .last_name="Culture Guy", .gender="male", .age=36, .height=76, .weight=215}
};

int main() {

    diamond::FileStorage storage("diamond");
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
        db.put<Person>(key, person);
    }

    std::cout << "people count: " << db.count<Person>() << std::endl;

    auto query = db.query<Person>()
        .where([](const Person& person) {
                return person.age >= 30 && person.last_name == "Doe";
        });
    for (const Person& person : query.execute()) {
        std::cout << person.first_name << " " << person.last_name << std::endl;
    }

    return 0;
}
