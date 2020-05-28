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

#include <algorithm>
#include <cstring>
#include <utility>

#include "diamond/buffer.h"
#include "diamond/page.h"

namespace diamond {

    std::shared_ptr<Page> Page::new_page(ID id, Type type) {
        if (id == 0) throw std::invalid_argument("page ids must be greater than 0");

        std::shared_ptr<Page> page(new Page);
        page->_id = id;
        page->_type = type;
        page->_size = page->header_size();
        
        switch (type) {
        case DATA:
            page->_data_entries = new std::vector<DataEntry>();
            break;
        case INTERNAL_NODE:
            page->_internal_node_entries = new std::vector<InternalNodeEntry>();
            break;
        case LEAF_NODE:
            page->_leaf.next = 0;
            page->_leaf.entries = new std::vector<LeafNodeEntry>();
            break;
        }

        return page;
    }

    std::shared_ptr<Page> Page::new_data_page(ID id) {
        return new_page(id, DATA);
    }

    std::shared_ptr<Page> Page::new_internal_node_page(ID id) {
        return new_page(id, INTERNAL_NODE);
    }

    std::shared_ptr<Page> Page::new_leaf_node_page(ID id) {
        return new_page(id, LEAF_NODE);
    }

    std::shared_ptr<Page> Page::new_page_from_storage(ID id, Storage& storage) {
        if (id == 0) throw std::invalid_argument("page ids must be greater than 0");

        if (storage.size() < SIZE * id) return nullptr;

        Buffer buffer(storage, SIZE, file_pos_for_id(id));
        BufferReader buffer_reader(buffer);

        std::shared_ptr<Page> page(new Page);

        page->_id = id;
        page->_type = buffer_reader.read<Type>();
        page->_size = page->header_size();

        switch (page->_type) {
        case DATA: {
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
        case INTERNAL_NODE: {
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
        case LEAF_NODE: {
            page->_leaf.next = buffer_reader.read<ID>();
            size_t num_entries = buffer_reader.read<size_t>();
            page->_leaf.entries = new std::vector<LeafNodeEntry>();
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
        }

        return page;
    }

    uint64_t Page::file_pos_for_id(Page::ID id) {
        return SIZE * (id - 1);
    }

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

    Page::~Page() {
        switch (_type) {
        case DATA:
            delete _data_entries;
            break;
        case INTERNAL_NODE:
            delete _internal_node_entries;
            break;
        case LEAF_NODE:
            delete _leaf.entries;
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

    size_t Page::header_size() const {
        size_t size = sizeof(Type);
        switch (_type) {
        case DATA:
            return size + sizeof(size_t);
        case INTERNAL_NODE:
            return size + sizeof(size_t);
        case LEAF_NODE:
            return size + sizeof(ID) + sizeof(size_t);
        }
    }

    uint64_t Page::file_pos() const {
        return file_pos_for_id(_id);
    }

    size_t Page::get_num_data_entries() const {
        ensure_type_is(DATA);
        return _data_entries->size();
    }

    const std::vector<Page::DataEntry>* Page::get_data_entries() const {
        ensure_type_is(DATA);
        return _data_entries;
    }

    const Page::DataEntry& Page::get_data_entry(size_t i) const {
        ensure_type_is(DATA);
        return _data_entries->at(i);
    }

    void Page::insert_data_entry(const Buffer& data, ID overflow_id, size_t overflow_index) {
        ensure_type_is(DATA);
        size_t space = data.size();
        if (overflow_id > 0) space += sizeof(overflow_id) + sizeof(overflow_index);
        ensure_space_available(space);

        _data_entries->emplace_back(data, overflow_id, overflow_index);
        _size += space;
    }

    size_t Page::get_num_internal_node_entries() const {
        ensure_type_is(INTERNAL_NODE);
        return _internal_node_entries->size();
    }

    const std::vector<Page::InternalNodeEntry>* Page::get_internal_node_entries() const {
        ensure_type_is(INTERNAL_NODE);
        return _internal_node_entries;
    }

    const Page::InternalNodeEntry& Page::get_internal_node_entry(size_t i) const {
        ensure_type_is(INTERNAL_NODE);
        return _internal_node_entries->at(i);
    }

    size_t Page::search_internal_node_entries(const Buffer& key, Compare compare) const {
        ensure_type_is(INTERNAL_NODE);
        size_t n = _internal_node_entries->size();
        for (size_t i = 0; i < n - 1; i++) {
            if (compare(_internal_node_entries->at(i).key(), key) > 0) {
                return i;
            }
        }
        return n - 1;
    }

    void Page::insert_internal_node_entry(const Buffer& key, ID next_node_id) {
        ensure_type_is(INTERNAL_NODE);
        size_t space = key.size() + sizeof(ID);
        ensure_space_available(space);

        _internal_node_entries->emplace_back(key, next_node_id);
        _size += space;
    }

    bool Page::can_insert_internal_node_entry(const Buffer& key) const {
        ensure_type_is(INTERNAL_NODE);
        size_t space = key.size() + sizeof(ID);
        return get_remaining_space() >= space;
    }

    size_t Page::get_num_leaf_node_entries() const {
        ensure_type_is(LEAF_NODE);
        return _leaf.entries->size();
    }

    const std::vector<Page::LeafNodeEntry>* Page::get_leaf_node_entries() const {
        ensure_type_is(LEAF_NODE);
        return _leaf.entries;
    }

    const Page::LeafNodeEntry& Page::get_leaf_node_entry(size_t i) const {
        ensure_type_is(LEAF_NODE);
        return _leaf.entries->at(i);
    }

    bool Page::find_leaf_node_entry(const Buffer& key, Compare compare, size_t& res) const {
        ensure_type_is(LEAF_NODE);
        size_t n = _leaf.entries->size();
        for (size_t i = 0; i < n; i++) {
            if (compare(_leaf.entries->at(i).key(), key) == 0) {
                res = i;   
                return true;
            }
        }
        return false;
    }

    void Page::insert_leaf_node_entry(const Buffer& key, ID data_id, size_t data_index) {
        ensure_type_is(LEAF_NODE);
        size_t space = key.size() + sizeof(ID) + sizeof(size_t);
        ensure_space_available(space);

        _leaf.entries->emplace_back(key, data_id, data_index);
        _size += space;
    }

    bool Page::can_insert_leaf_node_entry(const Buffer& key) const {
        ensure_type_is(LEAF_NODE);
        size_t space = key.size() + sizeof(ID) + sizeof(size_t);
        return get_remaining_space() >= space;
    }

    void Page::write_to_storage(Storage& storage) const {
        Buffer buffer(Page::SIZE);
        write_to_buffer(buffer);
        buffer.write_to_storage(storage, file_pos());
    }

    void Page::write_to_buffer(Buffer& buffer) const {
        if (buffer.size() < _size) throw std::logic_error("buffer is too small to fit entire page data.");
        BufferWriter buffer_writer(buffer);

        buffer_writer.write<Type>(_type);
        switch (_type) {
        case DATA: {
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
        case INTERNAL_NODE: {
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
        case LEAF_NODE: {
            buffer_writer.write<ID>(_leaf.next);
            size_t num_entries = _leaf.entries->size();
            buffer_writer.write<size_t>(num_entries);
            for (size_t i = 0; i < num_entries; i++) {
                const LeafNodeEntry& entry = _leaf.entries->at(i);

                buffer_writer.write<ID>(entry.data_id());
                buffer_writer.write<size_t>(entry.data_index());

                buffer_writer.write<size_t>(entry.key_size());
                buffer_writer.write(entry.key());
            }
            break;
        }
        }
    }

    Page::InternalNodeEntry::InternalNodeEntry(Buffer key, ID next_node_id)
        : _key(std::move(key)),
        _next_node_id(next_node_id) {
        if (_key.size() >= Page::MAX_KEY_SIZE) throw std::invalid_argument("key is too large");
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
        if (_key.size() >= Page::MAX_KEY_SIZE) throw std::invalid_argument("key is too large");
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

    Page::DataEntry::DataEntry(Buffer data, ID overflow_id, size_t overflow_index)
        : _data(std::move(data)),
        _overflow_id(overflow_id),
        _overflow_index(overflow_index) {
        if (_data.size() >= Page::SIZE) throw std::invalid_argument("data is too large");
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

} // namespace diamond
