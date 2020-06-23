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
        case Type::COLLECTIONS: {
            page->_collections.next = buffer_reader.read<ID>();
            size_t num_elements = buffer_reader.read<size_t>();
            page->_collections.map = new Collections();
            for (size_t i = 0; i < num_elements; i++) {
                size_t id_size = buffer_reader.read<size_t>();
                Buffer id(id_size);
                buffer_reader.read(id);

                ID root_node_id = buffer_reader.read<ID>();
                ID free_list_id = buffer_reader.read<ID>();

                page->_collections.map->try_emplace(
                    std::move(id),
                    root_node_id,
                    free_list_id);
                page->_size += collection_space_req(id);
            }
            break;
        }
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
                page->_size += free_list_entry_space_req();
            }
            break;
        }
        case Type::INTERNAL_NODE: {
            size_t num_entries = buffer_reader.read<size_t>();
            page->_internal_node_entries = new InternalNodeEntryList();
            for (size_t i = 0; i < num_entries; i++) {
                ID key_data_id = buffer_reader.read<ID>();
                size_t key_data_index = buffer_reader.read<size_t>();
                ID next_node_id = buffer_reader.read<ID>();

                page->_internal_node_entries->emplace_back(key_data_id, key_data_index, next_node_id);
                page->_size += internal_node_entry_space_req();
            }
            break;
        }
        case Type::LEAF_NODE: {
            page->_leaf.next = buffer_reader.read<ID>();
            size_t num_entries = buffer_reader.read<size_t>();
            page->_leaf.entries = new LeafNodeEntryList();
            for (size_t i = 0; i < num_entries; i++) {
                ID key_data_id = buffer_reader.read<ID>();
                size_t key_data_index = buffer_reader.read<size_t>();
                ID val_data_id = buffer_reader.read<ID>();
                size_t val_data_index = buffer_reader.read<size_t>();

                page->_leaf.entries->emplace_back(
                    key_data_id,
                    key_data_index,
                    val_data_id,
                    val_data_index);
                page->_size += leaf_node_entry_space_req();
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
        case Type::COLLECTIONS:
            delete _collections.map;
            break;
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
        case Type::COLLECTIONS:
            return size + sizeof(ID) + sizeof(size_t);
        case Type::DATA:
            return size + sizeof(size_t);
        case Type::FREE_LIST:
            return size + sizeof(ID) + sizeof(size_t);
        case Type::INTERNAL_NODE:
            return size + sizeof(size_t);
        case Type::LEAF_NODE:
            return size + sizeof(ID) + sizeof(size_t);
        }
    }

    uint64_t Page::file_pos() const {
        return file_pos_for_id(_id);
    }

    uint64_t Page::usage_count() const {
        return _usage_count.load(std::memory_order::memory_order_acquire);
    }

    Page::ID Page::get_next_collections_page() const {
        ensure_type_is(Type::COLLECTIONS);
        return _collections.next;
    }

    void Page::set_next_collections_page(ID next) {
        ensure_type_is(Type::COLLECTIONS);
        _collections.next = next;
    }

    const Page::Collections* Page::get_collections() const {
        ensure_type_is(Type::COLLECTIONS);
        return _collections.map;
    }

    bool Page::can_insert_collection(const Buffer& id) const {
        ensure_type_is(Type::COLLECTIONS);
        return get_remaining_space() >= collection_space_req(id);
    }

    bool Page::has_collection(const Buffer& name) const {
        ensure_type_is(Type::COLLECTIONS);
        return _collections.map->find(name) != _collections.map->end();
    }

    const Page::Collection& Page::get_collection(const Buffer& name) const {
        ensure_type_is(Type::COLLECTIONS);
        return _collections.map->at(name);
    }

    void Page::add_collection(Buffer name, ID root_node_id, ID free_list_id) {
        ensure_type_is(Type::COLLECTIONS);
        uint16_t space = collection_space_req(name);
        ensure_space_available(space);

        if (_collections.map->try_emplace(
                std::move(name),
                root_node_id,
                free_list_id).second) {
            _size += space;
        }
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

    const Page::InternalNodeEntryList* Page::get_internal_node_entries() const {
        ensure_type_is(Type::INTERNAL_NODE);
        return _internal_node_entries;
    }

    Page::InternalNodeEntryListIterator Page::internal_node_entries_begin() const {
        ensure_type_is(Type::INTERNAL_NODE);
        return _internal_node_entries->begin();
    }

    Page::InternalNodeEntryListIterator Page::internal_node_entries_end() const {
        ensure_type_is(Type::INTERNAL_NODE);
        return _internal_node_entries->end();
    }

    void Page::insert_internal_node_entry(
            InternalNodeEntryListIterator pos,
            ID key_data_id,
            size_t key_data_index,
            ID next_node_id) {
        ensure_type_is(Type::INTERNAL_NODE);
        uint16_t space = internal_node_entry_space_req();
        ensure_space_available(space);

        _internal_node_entries->emplace(
            pos,
            key_data_id,
            key_data_index,
            next_node_id);
        _size += space;
    }

    bool Page::can_insert_internal_node_entry() const {
        ensure_type_is(Type::INTERNAL_NODE);
        return get_remaining_space() >= internal_node_entry_space_req();
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

    Page::LeafNodeEntryListIterator Page::leaf_node_entries_begin() const {
        ensure_type_is(Type::LEAF_NODE);
        return _leaf.entries->begin();
    }

    Page::LeafNodeEntryListIterator Page::leaf_node_entries_end() const {
        ensure_type_is(Type::LEAF_NODE);
        return _leaf.entries->end();
    }

    void Page::insert_leaf_node_entry(
            LeafNodeEntryListIterator pos,
            ID key_data_id,
            size_t key_data_index,
            ID val_data_id,
            size_t val_data_index) {
        ensure_type_is(Type::LEAF_NODE);
        uint16_t space = leaf_node_entry_space_req();
        ensure_space_available(space);

        _leaf.entries->emplace(
            pos,
            key_data_id,
            key_data_index,
            val_data_id,
            val_data_index);
        _size += space;
    }

    bool Page::can_insert_leaf_node_entry() const {
        ensure_type_is(Type::LEAF_NODE);
        return get_remaining_space() >= leaf_node_entry_space_req();
    }

    void Page::split_leaf_node_entries(Page* other) {
        ensure_type_is(Type::LEAF_NODE);
        other->ensure_type_is(Type::LEAF_NODE);

        if (_leaf.entries->size() == 0) return;

        size_t i = 0;
        size_t n = _leaf.entries->size() / 2;
        other->ensure_space_available(n * leaf_node_entry_space_req());
        while (i < n) {
            const LeafNodeEntry& entry = _leaf.entries->front();
            other->_leaf.entries->push_back(entry);
            _leaf.entries->pop_front();
            i++;
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
        case Type::COLLECTIONS: {
            buffer_writer.write<ID>(_collections.next);
            size_t num_elements = _collections.map->size();
            buffer_writer.write<size_t>(num_elements);
            CollectionsIterator iter;
            for (iter = _collections.map->begin();
                    iter != _collections.map->end();
                    iter++) {
                const std::pair<Buffer, Collection>& pair = *iter;

                buffer_writer.write<size_t>(pair.first.size());
                buffer_writer.write(pair.first);

                buffer_writer.write<ID>(pair.second.root_node_id());
                buffer_writer.write<ID>(pair.second.free_list_id());
            }
            break;
        }
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
            InternalNodeEntryListIterator iter;
            for (iter = _internal_node_entries->begin();
                    iter != _internal_node_entries->end();
                    iter++) {
                const InternalNodeEntry& entry = *iter;

                buffer_writer.write<ID>(entry.key_data_id());
                buffer_writer.write<size_t>(entry.key_data_index());
                buffer_writer.write<ID>(entry.next_node_id());
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

                buffer_writer.write<ID>(entry.key_data_id());
                buffer_writer.write<size_t>(entry.key_data_index());
                buffer_writer.write<ID>(entry.val_data_id());
                buffer_writer.write<size_t>(entry.val_data_index());
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
        case Type::COLLECTIONS:
            _collections.next = 0;
            _collections.map = new Collections();
            break;
        case Type::DATA:
            _data_entries = new std::vector<DataEntry>();
            break;
        case Type::FREE_LIST:
            _free_list.next = 0;
            _free_list.entries = new std::vector<FreeListEntry>();
            break;
        case Type::INTERNAL_NODE:
            _internal_node_entries = new InternalNodeEntryList();
            break;
        case Type::LEAF_NODE:
            _leaf.next = 0;
            _leaf.entries = new LeafNodeEntryList();
            break;
        }
    }

    Page::Collection::Collection(ID root_node_id, ID free_list_id)
        : _root_node_id(root_node_id),
        _free_list_id(free_list_id) {}

    Page::ID Page::Collection::root_node_id() const {
        return _root_node_id;
    }

    Page::ID Page::Collection::free_list_id() const {
        return _free_list_id;
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

    Page::InternalNodeEntry::InternalNodeEntry(ID key_data_id, size_t key_data_index, ID next_node_id)
        : _key_data_id(key_data_id),
        _key_data_index(key_data_index),
        _next_node_id(next_node_id) {}

    Page::ID Page::InternalNodeEntry::key_data_id() const {
        return _key_data_id;
    }

    size_t Page::InternalNodeEntry::key_data_index() const {
        return _key_data_index;
    }

    Page::ID Page::InternalNodeEntry::next_node_id() const {
        return _next_node_id;
    }

    Page::LeafNodeEntry::LeafNodeEntry(ID key_data_id, size_t key_data_index, ID val_data_id, size_t val_data_index)
        : _key_data_id(key_data_id),
        _key_data_index(key_data_index),
        _val_data_id(val_data_id),
        _val_data_index(val_data_index) {}

    Page::ID Page::LeafNodeEntry::key_data_id() const {
        return _key_data_id;
    }

    size_t Page::LeafNodeEntry::key_data_index() const {
        return _key_data_index;
    }

    Page::ID Page::LeafNodeEntry::val_data_id() const {
        return _val_data_id;
    }

    Page::ID Page::LeafNodeEntry::val_data_index() const {
        return _val_data_index;
    }

    void Page::LeafNodeEntry::set_val_data_ptr(ID val_data_id, size_t val_data_index) {
        _val_data_id = val_data_id;
        _val_data_index = val_data_index;
    }

} // namespace diamond
