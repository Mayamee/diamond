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
#include <utility>

#include "diamond/buffer.h"
#include "diamond/logging.h"
#include "diamond/page.h"

namespace diamond {

    Page::Key Page::make_key(Type type, uint64_t id) {
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

    std::shared_ptr<Page> Page::new_page_from_stream(std::istream& stream) {
        Buffer buffer(Page::SIZE, stream);
        BufferReader buffer_reader(buffer);

        std::shared_ptr<Page> page(new Page);

        page->_type = buffer_reader.read<Type>();
        page->_id = buffer_reader.read<uint64_t>();
        page->_offset = buffer_reader.read<uint64_t>();

        switch (page->_type) {
        case DATA: {
            size_t num_entries = buffer_reader.read<size_t>();
            page->_data_entries = new std::vector<Buffer>();
            for (size_t i = 0; i < num_entries; i++) {
                size_t data_size = buffer_reader.read<size_t>();
                Buffer data(data_size);
                buffer_reader.read(data);

                page->_data_entries->emplace_back(std::move(data));
            }
            break;
        }
        case NODE: {
            size_t num_entries = buffer_reader.read<size_t>();
            page->_node.is_leaf = buffer_reader.read<bool>();
            if (page->_node.is_leaf) {
                page->_node.next = buffer_reader.read<uint64_t>();
            } else { 
                page->_node.next = 0;
            }
            page->_node.entries = new std::vector<NodeEntry>();
            for (size_t i = 0; i < num_entries; i++) {
                size_t key_size = buffer_reader.read<size_t>();
                Buffer key(key_size);
                buffer_reader.read(key);

                Type type = buffer_reader.read<Type>();
                uint64_t node_id = buffer_reader.read<uint64_t>();
                switch (type) {
                case DATA: {
                    size_t data_index = buffer_reader.read<size_t>();
                    page->_node.entries->emplace_back(std::move(key), node_id, data_index);
                    break;
                }
                case NODE:
                    page->_node.entries->emplace_back(std::move(key), node_id);
                    break;
                default:
                    break;
                }
            }
            break;
        }
        case DATA_OFFSETS:
        case NODE_OFFSETS: {
            size_t num_offsets = buffer_reader.read<size_t>();
            page->_offsets.next = buffer_reader.read<uint64_t>();
            page->_offsets.offsets = new std::vector<uint64_t>();
            for (size_t i = 0; i < num_offsets; i++) {
                page->_offsets.offsets->push_back(buffer_reader.read<uint64_t>());
            }
            break;
        }
        default:
            UNREACHABLE();
        }
        
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
            UNREACHABLE();
        }
    }

    Page::Type Page::get_type() const {
        return _type;
    }

    uint64_t Page::get_id() const {
        return _id;
    }

    Page::Key Page::get_key() const {
        return std::tie(_type, _id);
    }

    uint64_t Page::get_offset() const {
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
        CHECK_EQ(_type, DATA);
        return _data_entries->size();
    }

    const std::vector<Buffer>* Page::get_data_entries() const {
        CHECK_EQ(_type, DATA);
        return _data_entries;
    }

    const Buffer& Page::get_data_entry(size_t i) const {
        CHECK_EQ(_type, DATA);
        return _data_entries->at(i);
    }

    size_t Page::get_num_node_entries() const {
        CHECK_EQ(_type, NODE);
        return _node.entries->size();
    }

    const std::vector<Page::NodeEntry>* Page::get_node_entries() const {
        CHECK_EQ(_type, NODE);
        return _node.entries;
    }

    const Page::NodeEntry& Page::get_node_entry(size_t i) const {
        CHECK_EQ(_type, NODE);
        return _node.entries->at(i);
    }

    // const Page::NodeEntry& Page::search_node_entries(const Buffer& key) const {
        // return _node.entries.back();
    // }

    size_t Page::get_num_offsets() const {
        CHECK(_type == DATA_OFFSETS || _type == NODE_OFFSETS);
        return _offsets.offsets->size();
    }

    const std::vector<size_t>* Page::get_offsets() const {
        CHECK(_type == DATA_OFFSETS || _type == NODE_OFFSETS);
        return _offsets.offsets;
    }

    size_t Page::get_offset(size_t i) const {
        CHECK(_type == DATA_OFFSETS || _type == NODE_OFFSETS);
        return _offsets.offsets->at(i);
    }

    size_t Page::get_next_offsets() const {
        CHECK(_type == DATA_OFFSETS || _type == NODE_OFFSETS);
        return _offsets.next;
    }

    size_t Page::memory_usage() const {
        return 0;
    }

    void Page::write_to_stream(std::ostream& stream) const {
        Buffer buffer(Page::SIZE);
        BufferWriter buffer_writer(buffer);

        buffer_writer.write<Type>(_type);
        buffer_writer.write<uint64_t>(_id);
        buffer_writer.write<uint64_t>(_offset);

        switch (_type) {
        case DATA: {
            size_t num_entries = _data_entries->size();
            buffer_writer.write<size_t>(num_entries);
            for (size_t i = 0; i < num_entries; i++) {
                const Buffer& entry = _data_entries->at(i);

                buffer_writer.write<size_t>(entry.size());
                buffer_writer.write(entry);
            }
            break;
        }
        case NODE: {
            size_t num_entries = _node.entries->size();
            buffer_writer.write<size_t>(num_entries);
            buffer_writer.write<bool>(_node.is_leaf);
            if (_node.is_leaf) buffer_writer.write<uint64_t>(_node.next);
            for (size_t i = 0; i < num_entries; i++) {
                const NodeEntry& entry = _node.entries->at(i);

                buffer_writer.write<size_t>(entry.key_size());
                buffer_writer.write(entry.key());

                Key key = entry.next_page_key();
                buffer_writer.write<Type>(std::get<0>(key));
                buffer_writer.write<uint64_t>(std::get<1>(key));
            }
            break;
        }
        case DATA_OFFSETS:
        case NODE_OFFSETS: {
            size_t num_offsets = _offsets.offsets->size();
            buffer_writer.write<size_t>(num_offsets);
            for (size_t i = 0; i < num_offsets; i++) {
                buffer_writer.write<uint64_t>(_offsets.offsets->at(i));
            }
            break;
        }
        default:
            UNREACHABLE();
        }

        buffer.write_to_stream(stream);
    }

    Page::NodeEntry::NodeEntry(Buffer key, uint64_t data_id, size_t data_index)
        : _key(key),
        _next_page_type(Page::DATA),
        _next_data_page({ .id = data_id, .index = data_index }) {}

    Page::NodeEntry::NodeEntry(Buffer key, uint64_t node_id)
        : _key(key),
        _next_page_type(Page::NODE),
        _next_node_page_id(node_id) {}

    size_t Page::NodeEntry::key_size() const {
        return _key.size();
    }

    const Buffer& Page::NodeEntry::key() const {
        return _key;
    }

    uint64_t Page::NodeEntry::next_data_id() const {
        CHECK_EQ(_next_page_type, Page::DATA);
        return _next_data_page.id;
    }

    size_t Page::NodeEntry::next_data_index() const {
        CHECK_EQ(_next_page_type, Page::DATA);
        return _next_data_page.index;
    }

    uint64_t Page::NodeEntry::next_node_id() const {
        CHECK_EQ(_next_page_type, Page::NODE);
        return _next_node_page_id;
    }

    Page::Type Page::NodeEntry::next_type() const {
        return _next_page_type;
    }

    Page::Key Page::NodeEntry::next_page_key() const {
        switch (_next_page_type) {
        case DATA:
            return Page::make_key(_next_page_type, _next_data_page.id);
        case NODE:
            return Page::make_key(_next_page_type, _next_node_page_id);
        default:
            UNREACHABLE();
        }
    }

} // namespace diamond
