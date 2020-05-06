/*  Diamond - Relational Database
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
#include <iostream>

#include "diamond/buffer.h"
#include "diamond/page.h"

namespace diamond {

    Page::Key Page::make_key(Type type, size_t id) {
        return std::tie(type, id);
    }

    Page::Type Page::get_offsets_type(Type type) {
        switch (type) {
        case DATA:
            return DATA_OFFSETS;
        case NODE:
            return NODE_OFFSETS;
        default:
            return NONE;
        }
    }

    // std::shared_ptr<Page> Page::new_node_offsets_page(
    //         size_t id,
    //         size_t offset,
    //         std::vector<size_t>& offsets,
    //         size_t next) {
    //     return std::shared_ptr<Page>(new Page(id, offset, offsets, next));
    // }

    std::shared_ptr<Page> Page::new_page_from_stream(std::istream& stream) {
        // Type type;
        // stream >> type;
        // size_t id;
        // stream >> id;
        // size_t offset;
        // stream >> offset;

        std::shared_ptr<Page> page;
        // switch (type) {
        // case DATA: {
        //     size_t num;
        //     stream >> num;
        //     size_t next;
        //     stream >> next;
        //     std::vector<DataEntry> entries;
        //     for (size_t i = 0; i < num; i++) {
        //         size_t data_size;
        //         stream >> data_size;
        //         char data[data_size];
        //         stream.readsome(data, data_size);

        //         entries.emplace_back(data_size, (void*)&data);
        //     }
        //     page = std::shared_ptr<Page>(new Page(id, offset, entries, next));
        //     break;
        // }
        // case NODE: {
        //     size_t num_entries;
        //     stream >> num_entries;
        //     std::vector<NodeEntry> entries;
        //     bool is_leaf = true;
        //     for (size_t i = 0; i < num_entries; i++) {
        //         size_t key_size;
        //         stream >> key_size;
        //         char key[key_size];
        //         stream.readsome(key, key_size);

        //         NodeEntry::PtrType ptr_type;
        //         stream >> ptr_type;
        //         if (ptr_type == NodeEntry::NODE) is_leaf = false;
        //         size_t ptr;
        //         stream >> ptr;

        //         entries.emplace_back(key_size, (void*)&key, ptr_type, ptr);
        //     }
        //     page = std::shared_ptr<Page>(new Page(id, offset, entries, is_leaf));
        //     break;
        // }
        // case NODE_OFFSETS: {
        //     size_t num_offsets;
        //     stream >> num_offsets;
        //     size_t next;
        //     stream >> next;
        //     std::vector<size_t> offsets;
        //     offsets.reserve(num_offsets);
        //     for (size_t i = 0; i < num_offsets; i++) {
        //         stream >> offsets[i];
        //     }
        //     page = std::shared_ptr<Page>(new Page(id, offset, offsets, next));
        //     break;
        // }
        // }
        
        return page;
    }

    Page::~Page() {
        switch (_type) {
        case DATA:
            break;
        case NODE:
            delete _node.entries;
            break;
        case DATA_OFFSETS:
        case NODE_OFFSETS:
            delete _offsets.offsets;
            break;
        default:
            break;
        }
    }

    Page::Type Page::get_type() const {
        return _type;
    }

    size_t Page::get_id() const {
        return _id;
    }

    Page::Key Page::get_key() const {
        return std::tie(_type, _id);
    }

    size_t Page::get_offset() const {
        return _offset;
    }

    std::time_t Page::last_used() const {
        return _last_used;
    }

    void Page::set_last_used(std::time_t last_used) {
        _last_used = last_used;
    }

    bool Page::is_dirty() const {
        return _is_dirty;
    }
        
    void Page::set_dirty(bool dirty) {
        _is_dirty = dirty;
    }

    size_t Page::get_num_data_entries() const {
        ensure_type(DATA);
        return _data.entries->size();
    }

    const std::vector<Page::DataEntry>* Page::get_data_entries() const {
        ensure_type(DATA);
        return _data.entries;
    }

    const Page::DataEntry& Page::get_data_entry(size_t i) const {
        ensure_type(DATA);
        return _data.entries->at(i);
    }

    size_t Page::get_num_node_entries() const {
        ensure_type(NODE);
        return _node.entries->size();
    }

    const std::vector<Page::NodeEntry>* Page::get_node_entries() const {
        ensure_type(NODE);
        return _node.entries;
    }

    const Page::NodeEntry& Page::get_node_entry(size_t i) const {
        ensure_type(NODE);
        return _node.entries->at(i);
    }

    size_t Page::get_num_offsets() const {
        ensure_type_oneof({ DATA_OFFSETS, NODE_OFFSETS });
        return _offsets.offsets->size();
    }

    const std::vector<size_t>* Page::get_offsets() const {
        ensure_type_oneof({ DATA_OFFSETS, NODE_OFFSETS });
        return _offsets.offsets;
    }

    size_t Page::get_offset(size_t i) const {
        ensure_type_oneof({ DATA_OFFSETS, NODE_OFFSETS });
        return _offsets.offsets->at(i);
    }

    size_t Page::get_next_offsets() const {
        ensure_type_oneof({ DATA_OFFSETS, NODE_OFFSETS });
        return _offsets.next;
    }

    size_t Page::memory_usage() const {
        return 0;
    }

    void Page::write_to_stream(std::ostream& stream) const {
        WritableBuffer buffer(Page::PAGE_SIZE);
        switch (_type) {
        case DATA: {
            size_t n = _data.entries->size();
            buffer.write<size_t>(n);
            for (size_t i = 0; i < n; i++) {
                const DataEntry& entry = _data.entries->at(i);

                buffer.write<size_t>(entry.size());
                buffer.write(entry.val(), entry.size());
            }
            break;
        }
        case NODE: {
            size_t n = _node.entries->size();
            buffer.write<size_t>(n);
            for (size_t i = 0; i < n; i++) {
                const NodeEntry& entry = _node.entries->at(i);

                buffer.write<size_t>(entry.key_size());
                buffer.write(entry.key(), entry.key_size());

                Key key = entry.next();
                buffer.write<Type>(std::get<0>(key));
                buffer.write<size_t>(std::get<1>(key));
            }
            break;
        }
        case DATA_OFFSETS:
        case NODE_OFFSETS: {
            size_t n = _offsets.offsets->size();
            buffer.write<size_t>(n);
            break;
        }
        default:
            break;
        }

        buffer.write_to_stream(stream);
    }

    Page::Page(
            size_t id,
            size_t offset,
            std::vector<DataEntry>& entries)
        : _type(DATA), 
        _id(id),
        _offset(offset),
        _data({ .entries = new std::vector<DataEntry>(entries) }) {}

    Page::Page(
            size_t id,
            size_t offset,
            std::vector<NodeEntry>& entries,
            bool is_leaf)
        : _type(NODE),
        _id(id),
        _offset(offset),
        _node({ .entries = new std::vector<NodeEntry>(entries), .is_leaf = is_leaf }) {}

    Page::Page(
            size_t id,
            size_t offset,
            std::vector<size_t>& offsets,
            size_t next)
        : _type(NODE_OFFSETS), 
        _id(id),
        _offset(offset),
        _offsets({
            .offsets = new std::vector<size_t>(offsets),
            .next = next 
        }) {}

    Page::DataEntry::DataEntry(size_t size, void* val)
        : _size(size),
        _val(new char[_size]) {
        std::memcpy(_val, val, _size);
    }

    size_t Page::DataEntry::size() const {
        return _size;
    }

    const void* Page::DataEntry::val() const {
        return _val;
    }

    Page::NodeEntry::NodeEntry(size_t key_size, void* key, Page::Key next)
        : _key_size(key_size),
        _key(new char[_key_size]),
        _next(next) {
        std::memcpy(_key, key, _key_size);
    }

    size_t Page::NodeEntry::key_size() const {
        return _key_size;
    }

    const void* Page::NodeEntry::key() const {
        return _key;
    }

    Page::Key Page::NodeEntry::next() const {
        return _next;
    }

    size_t Page::NodeEntry::compare(const void* other, size_t size) const {
        if (size != _key_size) return false;
        return std::memcmp(_key, other, size);
    }

} // namespace diamond
