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

#include <algorithm>
#include <cstring>
#include <utility>

#include "diamond/buffer.h"
#include "diamond/page.h"

namespace diamond {

    const Page::ID Page::INVALID_ID = 0;

    const uint16_t Page::SIZE = 8192;
    const uint16_t Page::MAX_KEY_SIZE = SIZE / 4;

   /* Static */
    size_t Page::default_compare(const Buffer& b0, const Buffer& b1) {
        size_t b0_n = b0.size();
        size_t b1_n = b1.size();
        size_t n = (b0_n <= b1_n) ? b0_n : b1_n;
        size_t r = std::memcmp(b0.buffer(), b1.buffer(), n);
        if (r != 0 || b0_n == b1_n) {
            return r;
        }
        return (b0_n < b1_n) ? -1 : 1;
    }

    /* Static */
    uint64_t Page::file_pos_for_id(ID id) {
        return SIZE * (id - 1);
    }

    /* Static */
    Page* Page::from_storage(ID id, Storage& storage) {
        if (id == 0) throw std::invalid_argument("page ids must be greater than 0");

        if (storage.size() < SIZE * id) return nullptr;

        Buffer buffer(storage, SIZE, file_pos_for_id(id));
        BufferReader buffer_reader(buffer);

        Page* page = new Page(id, buffer_reader.read<Type>());
        switch (page->_type) {
        case Type::DATA: {
            size_t num_entries = buffer_reader.read<size_t>();
            page->_data_entries = new std::vector<DataEntry>();
            for (size_t i = 0; i < num_entries; i++) {
                size_t data_size = buffer_reader.read<size_t>();
                size_t rem = buffer_reader.bytes_remaining();
                if (rem < data_size) {
                    ID overflow_id;
                    size_t overflow_index;

                    size_t to_read = rem - (sizeof(overflow_id) + sizeof(overflow_index));
                    Buffer data(to_read);
                    buffer_reader.read(data);

                    overflow_id = buffer_reader.read<ID>();
                    overflow_index = buffer_reader.read<size_t>();

                    page->_data_entries->emplace_back(std::move(data), overflow_id, overflow_index);
                    page->_size += to_read + sizeof(overflow_id) + sizeof(overflow_index);
                } else {
                    Buffer data(data_size);
                    buffer_reader.read(data);
                    page->_data_entries->emplace_back(std::move(data));
                    page->_size += data_size;
                }
            }
            break;
        }
        case Type::FREE_LIST: {
            page->_free_list.next = buffer_reader.read<ID>();
            size_t num_entries = buffer_reader.read<size_t>();
            page->_free_list.entries = new std::vector<FreeListEntry>();
            for (size_t i = 0; i < num_entries; i++) {
                ID data_id = buffer_reader.read<ID>(); 
                uint16_t free_space = buffer_reader.read<uint16_t>();
                page->_free_list.entries->emplace_back(data_id, free_space);
                page->_size += sizeof(data_id) + sizeof(free_space);
            }
            break;
        }
        case Type::INTERNAL_NODE: {
            size_t num_entries = buffer_reader.read<size_t>();
            page->_internal_node_entries = new std::vector<InternalNodeEntry>();
            for (size_t i = 0; i < num_entries; i++) {
                ID next_node_id = buffer_reader.read<ID>();

                size_t key_size = buffer_reader.read<size_t>();
                Buffer key(key_size);
                buffer_reader.read(key);

                page->_internal_node_entries->emplace_back(std::move(key), next_node_id);
                page->_size += key_size + sizeof(next_node_id);
            }
            break;
        }
        case Type::LEAF_NODE: {
            page->_leaf.next = buffer_reader.read<ID>();
            size_t num_entries = buffer_reader.read<size_t>();
            page->_leaf.entries = new LeafNodeEntryList();
            for (size_t i = 0; i < num_entries; i++) {
                ID data_id = buffer_reader.read<ID>();
                size_t data_index = buffer_reader.read<size_t>();

                size_t key_size = buffer_reader.read<size_t>();
                Buffer key(key_size);
                buffer_reader.read(key);

                page->_leaf.entries->emplace_back(std::move(key), data_id, data_index);
                page->_size += key_size + sizeof(data_id) + sizeof(data_index);
            }
            break;
        }
        case Type::ROOTS: {
            page->_roots.next = buffer_reader.read<ID>();
            size_t num_elements = buffer_reader.read<size_t>();
            page->_roots.map = new RootsMap();
            for (size_t i = 0; i < num_elements; i++) {
                size_t id_size = buffer_reader.read<size_t>();
                Buffer id(id_size);
                buffer_reader.read(id);

                ID root_node_id = buffer_reader.read<ID>();

                page->_roots.map->emplace(std::move(id), root_node_id);
                page->_size += id_size + sizeof(root_node_id);
            }
            break;
        }
        }

        return page;
    }

    /* Static */
    Page* Page::new_page(ID id, Type type) {
        return new Page(id, type);
    }

    Page::~Page() {
        switch (_type) {
        case Type::DATA:
            delete _data_entries;
            break;
        case Type::FREE_LIST:
            delete _free_list.entries;
            break;
        case Type::INTERNAL_NODE:
            delete _internal_node_entries;
            break;
        case Type::LEAF_NODE:
            delete _leaf.entries;
            break;
        case Type::ROOTS:
            delete _roots.map;
            break;
        }
    }

    Page::Type Page::get_type() const {
        return _type;
    }

    Page::ID Page::get_id() const {
        return _id;
    }

    uint16_t Page::get_size() const {
        return _size;
    }

    uint16_t Page::get_remaining_space() const {
        return SIZE - _size;
    }

    uint16_t Page::header_size() const {
        uint16_t size = sizeof(Type);
        switch (_type) {
        case Type::DATA:
            return size + sizeof(size_t);
        case Type::FREE_LIST:
            return size + sizeof(ID) + sizeof(size_t);
        case Type::INTERNAL_NODE:
            return size + sizeof(size_t);
        case Type::LEAF_NODE:
            return size + sizeof(ID) + sizeof(size_t);
        case Type::ROOTS:
            return size + sizeof(ID) + sizeof(size_t);
        }
    }

    uint64_t Page::file_pos() const {
        return file_pos_for_id(_id);
    }

    uint64_t Page::usage_count() const {
        return _usage_count.load(std::memory_order::memory_order_acquire);
    }

    size_t Page::get_num_data_entries() const {
        ensure_type_is(Type::DATA);
        return _data_entries->size();
    }

    const std::vector<Page::DataEntry>* Page::get_data_entries() const {
        ensure_type_is(Type::DATA);
        return _data_entries;
    }

    const Page::DataEntry& Page::get_data_entry(size_t i) const {
        ensure_type_is(Type::DATA);
        return _data_entries->at(i);
    }

    size_t Page::insert_data_entry(Buffer data) {
        ensure_type_is(Type::DATA);
        // TODO: Handle overflows
        uint16_t space = data_entry_space_req(data);
        ensure_space_available(space);

        size_t i = _data_entries->size();
        _data_entries->emplace_back(std::move(data));
        _size += space;
        return i;
    }

    bool Page::can_insert_data_entry(const Buffer& data) {
        ensure_type_is(Type::DATA);
        // TODO: Handle overflows
        return get_remaining_space() >= data_entry_space_req(data);
    }

    Page::ID Page::get_next_free_list_page() const {
        ensure_type_is(Type::FREE_LIST);
        return _free_list.next;
    }

    void Page::set_next_free_list_page(ID next) {
        ensure_type_is(Type::FREE_LIST);
        _free_list.next = next;
    }

    size_t Page::get_num_free_list_entries() const {
        ensure_type_is(Type::FREE_LIST);
        return _free_list.entries->size();
    }

    const std::vector<Page::FreeListEntry>* Page::get_free_list_entries() const {
        ensure_type_is(Type::FREE_LIST);
        return _free_list.entries;
    }

    const Page::FreeListEntry& Page::get_free_list_entry(size_t i) const {
        ensure_type_is(Type::FREE_LIST);
        return _free_list.entries->at(i);
    }

    bool Page::reserve_free_list_entry(const Buffer& data, ID& data_id) {
        ensure_type_is(Type::FREE_LIST);
        uint16_t space_req = data_entry_space_req(data);
        size_t n = _free_list.entries->size();
        for (size_t i = 0; i < n; i++) {
            FreeListEntry& entry = _free_list.entries->at(i);
            uint16_t free_space = entry.free_space();
            if (free_space >= space_req) {
                entry.set_free_space(free_space - space_req);
                data_id = _free_list.entries->at(i).data_id();
                return true;
            }
        }
        return false;
    }

    size_t Page::insert_free_list_entry(ID data_id, uint16_t free_space) {
        ensure_type_is(Type::FREE_LIST);
        uint16_t space = free_list_entry_space_req();
        ensure_space_available(space);

        size_t i = _free_list.entries->size();
        _free_list.entries->emplace_back(data_id, free_space);
        _size += space;
        return i;
    }

    bool Page::can_insert_free_list_entry() {
        ensure_type_is(Type::FREE_LIST);
        return get_remaining_space() >= free_list_entry_space_req();
    }

    size_t Page::get_num_internal_node_entries() const {
        ensure_type_is(Type::INTERNAL_NODE);
        return _internal_node_entries->size();
    }

    const std::vector<Page::InternalNodeEntry>* Page::get_internal_node_entries() const {
        ensure_type_is(Type::INTERNAL_NODE);
        return _internal_node_entries;
    }

    const Page::InternalNodeEntry& Page::get_internal_node_entry(size_t i) const {
        ensure_type_is(Type::INTERNAL_NODE);
        return _internal_node_entries->at(i);
    }

    size_t Page::search_internal_node_entries(const Buffer& key, Compare compare) const {
        ensure_type_is(Type::INTERNAL_NODE);
        size_t n = _internal_node_entries->size();
        for (size_t i = 0; i < n - 1; i++) {
            if (compare(_internal_node_entries->at(i).key(), key) > 0) {
                return i;
            }
        }
        return n - 1;
    }

    size_t Page::insert_internal_node_entry(Buffer key, ID next_node_id) {
        ensure_type_is(Type::INTERNAL_NODE);
        uint16_t space = internal_node_entry_space_req(key);
        ensure_space_available(space);

        size_t i = _internal_node_entries->size();
        _internal_node_entries->emplace_back(
            std::move(key),
            next_node_id);
        _size += space;
        return i;
    }

    bool Page::can_insert_internal_node_entry(const Buffer& key) const {
        ensure_type_is(Type::INTERNAL_NODE);
        return get_remaining_space() >= internal_node_entry_space_req(key);
    }

    Page::ID Page::get_next_leaf_node_page() const {
        ensure_type_is(Type::LEAF_NODE);
        return _leaf.next;
    }

    size_t Page::get_num_leaf_node_entries() const {
        ensure_type_is(Type::LEAF_NODE);
        return _leaf.entries->size();
    }

    const Page::LeafNodeEntryList* Page::get_leaf_node_entries() const {
        ensure_type_is(Type::LEAF_NODE);
        return _leaf.entries;
    }

    Page::LeafNodeEntryListIterator Page::find_leaf_node_entry(const Buffer& key, Compare compare) const {
        ensure_type_is(Type::LEAF_NODE);
        LeafNodeEntryListIterator iter;
        for (iter = _leaf.entries->begin(); 
                iter != _leaf.entries->end();
                iter++) {
            if (compare((*iter).key(), key) == 0) {
                return iter;
            }
        }
        return iter;
    }

    Page::LeafNodeEntryListIterator Page::leaf_node_entries_begin() const {
        ensure_type_is(Type::LEAF_NODE);
        return _leaf.entries->begin();
    }

    Page::LeafNodeEntryListIterator Page::leaf_node_entries_end() const {
        ensure_type_is(Type::LEAF_NODE);
        return _leaf.entries->end();
    }

    size_t Page::insert_leaf_node_entry(Buffer key, ID data_id, size_t data_index) {
        ensure_type_is(Type::LEAF_NODE);
        uint16_t space = leaf_node_entry_space_req(key);
        ensure_space_available(space);

        size_t i = _leaf.entries->size();
        _leaf.entries->emplace_back(
            std::move(key),
            data_id,
            data_index);
        _size += space;
        return i;
    }

    bool Page::can_insert_leaf_node_entry(const Buffer& key) const {
        ensure_type_is(Type::LEAF_NODE);
        return get_remaining_space() >= leaf_node_entry_space_req(key);
    }

    void Page::split_leaf_node_entries(Page* other) {
        ensure_type_is(Type::LEAF_NODE);
        other->ensure_type_is(Type::LEAF_NODE);

        if (_leaf.entries->size() == 0) return;

        size_t n = 0;
        size_t t = _size / 2;
        other->ensure_space_available(t);
        while (true) {
            const LeafNodeEntry& entry = _leaf.entries->front();
            n += leaf_node_entry_space_req(entry);
            if (n > t) break;
            other->_leaf.entries->push_back(std::move(entry));
            _leaf.entries->pop_front();
        }
    }

    Page::ID Page::get_next_roots_page() const {
        ensure_type_is(Type::ROOTS);
        return _roots.next;
    }

    void Page::set_next_roots_page(ID next) {
        ensure_type_is(Type::ROOTS);
        _roots.next = next;
    }

    const Page::RootsMap* Page::get_roots_map() const {
        ensure_type_is(Type::ROOTS);
        return _roots.map;
    }

    bool Page::can_insert_root_node_id(const Buffer& id) const {
        ensure_type_is(Type::ROOTS);
        return get_remaining_space() >= roots_element_space_req(id);
    }

    bool Page::get_root_node_id(const Buffer& id, ID& root_node_id) const {
        ensure_type_is(Type::ROOTS);
        if (_roots.map->find(id) != _roots.map->end()) {
            root_node_id = _roots.map->at(id);
            return true;
        }
        return false;
    }

    void Page::set_root_node_id(Buffer id, ID root_node_id) {
        ensure_type_is(Type::ROOTS);
        if (_roots.map->find(id) == _roots.map->end()) {
            uint16_t space = roots_element_space_req(id);
            ensure_space_available(space);
            _roots.map->emplace(
                std::move(id),
                root_node_id);
            _size += space;
        } else {
            _roots.map->at(id) = root_node_id;
        }
    }

    void Page::write_to_storage(Storage& storage) const {
        Buffer buffer(SIZE);
        write_to_buffer(buffer);
        buffer.write_to_storage(storage, file_pos());
    }

    void Page::write_to_buffer(Buffer& buffer) const {
        if (buffer.size() < _size) throw std::logic_error("buffer is too small to fit entire page data.");
        BufferWriter buffer_writer(buffer);

        buffer_writer.write<Type>(_type);
        switch (_type) {
        case Type::DATA: {
            size_t num_entries = _data_entries->size();
            buffer_writer.write<size_t>(num_entries);
            for (size_t i = 0; i < num_entries; i++) {
                const DataEntry& entry = _data_entries->at(i);

                buffer_writer.write<size_t>(entry.data_size());
                buffer_writer.write(entry.data());

                if (entry.overflows()) {
                    buffer_writer.write<ID>(entry.overflow_id());
                    buffer_writer.write<size_t>(entry.overflow_index());
                }
            }
            break;
        }
        case Type::FREE_LIST: {
            buffer_writer.write<ID>(_free_list.next);
            size_t num_entries = _free_list.entries->size();
            buffer_writer.write<size_t>(num_entries);
            for (size_t i = 0; i < num_entries; i++) {
                const FreeListEntry& entry = _free_list.entries->at(i);

                buffer_writer.write<ID>(entry.data_id());
                buffer_writer.write<uint16_t>(entry.free_space());
            }
            break;
        }
        case Type::INTERNAL_NODE: {
            size_t num_entries = _internal_node_entries->size();
            buffer_writer.write<size_t>(num_entries);
            for (size_t i = 0; i < num_entries; i++) {
                const InternalNodeEntry& entry = _internal_node_entries->at(i);

                buffer_writer.write<ID>(entry.next_node_id());

                buffer_writer.write<size_t>(entry.key_size());
                buffer_writer.write(entry.key());
            }
            break;
        }
        case Type::LEAF_NODE: {
            buffer_writer.write<ID>(_leaf.next);
            size_t num_entries = _leaf.entries->size();
            buffer_writer.write<size_t>(num_entries);
            LeafNodeEntryListIterator iter;
            for (iter = _leaf.entries->begin();
                    iter != _leaf.entries->end();
                    iter++) {
                const LeafNodeEntry& entry = *iter;

                buffer_writer.write<ID>(entry.data_id());
                buffer_writer.write<size_t>(entry.data_index());

                buffer_writer.write<size_t>(entry.key_size());
                buffer_writer.write(entry.key());
            }
            break;
        }
        case Type::ROOTS: {
            buffer_writer.write<ID>(_roots.next);
            size_t num_elements = _roots.map->size();
            buffer_writer.write<size_t>(num_elements);
            RootsMapIterator iter;
            for (iter = _roots.map->begin();
                    iter != _roots.map->end();
                    iter++) {
                const std::pair<Buffer, ID>& pair = *iter;

                buffer_writer.write<size_t>(pair.first.size());
                buffer_writer.write(pair.first);

                buffer_writer.write<ID>(pair.second);
            }
            break;
        }
        }
    }

    Page::Page(ID id, Type type)
            : _id(id),
            _type(type),
            _size(header_size()),
            _usage_count(0) {
        switch (type) {
        case Type::DATA:
            _data_entries = new std::vector<DataEntry>();
            break;
        case Type::FREE_LIST:
            _free_list.next = 0;
            _free_list.entries = new std::vector<FreeListEntry>();
            break;
        case Type::INTERNAL_NODE:
            _internal_node_entries = new std::vector<InternalNodeEntry>();
            break;
        case Type::LEAF_NODE:
            _leaf.next = 0;
            _leaf.entries = new LeafNodeEntryList();
            break;
        case Type::ROOTS:
            _roots.next = 0;
            _roots.map = new RootsMap();
            break;
        }
    }

    Page::DataEntry::DataEntry(Buffer data, ID overflow_id, size_t overflow_index)
        : _data(std::move(data)),
        _overflow_id(overflow_id),
        _overflow_index(overflow_index) {
        if (_data.size() >= SIZE) throw std::invalid_argument("data is too large");
    }

    size_t Page::DataEntry::data_size() const {
        return _data.size();
    }

    const Buffer& Page::DataEntry::data() const {
        return _data;
    }

    bool Page::DataEntry::overflows() const {
        return _overflow_id != 0;
    }

    Page::ID Page::DataEntry::overflow_id() const {
        return _overflow_id;
    }

    size_t Page::DataEntry::overflow_index() const {
        return _overflow_index;
    }

    Page::FreeListEntry::FreeListEntry(ID data_id, uint16_t free_space)
        : _data_id(data_id),
        _free_space(free_space) {}

    Page::ID Page::FreeListEntry::data_id() const {
        return _data_id;
    }

    uint16_t Page::FreeListEntry::free_space() const {
        return _free_space;
    }

    void Page::FreeListEntry::set_free_space(uint16_t free_space) {
        _free_space = free_space;
    }

    Page::InternalNodeEntry::InternalNodeEntry(Buffer key, ID next_node_id)
        : _key(std::move(key)),
        _next_node_id(next_node_id) {
        if (_key.size() >= MAX_KEY_SIZE) throw std::invalid_argument("key is too large");
    }

    size_t Page::InternalNodeEntry::key_size() const {
        return _key.size();
    }

    const Buffer& Page::InternalNodeEntry::key() const {
        return _key;
    }

    Page::ID Page::InternalNodeEntry::next_node_id() const {
        return _next_node_id;
    }

    Page::LeafNodeEntry::LeafNodeEntry(Buffer key, ID data_id, ID data_index)
        : _key(std::move(key)),
        _data_id(data_id),
        _data_index(data_index) {
        if (_key.size() >= MAX_KEY_SIZE) throw std::invalid_argument("key is too large");
    }

    size_t Page::LeafNodeEntry::key_size() const {
        return _key.size();
    }

    const Buffer& Page::LeafNodeEntry::key() const {
        return _key;
    }

    Page::ID Page::LeafNodeEntry::data_id() const {
        return _data_id;
    }

    Page::ID Page::LeafNodeEntry::data_index() const {
        return _data_index;
    }

} // namespace diamond
