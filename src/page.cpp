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
#include <iostream>
#include <utility>

#include "diamond/buffer.h"
#include "diamond/logging.h"
#include "diamond/page.h"

namespace diamond {

    std::shared_ptr<Page> Page::new_data_page(ID id) {
        std::shared_ptr<Page> page(new Page);

        page->_type = DATA;
        page->_id = id;

        page->_data_entries = new std::vector<Buffer>();

        return page;
    }

    std::shared_ptr<Page> Page::new_internal_node_page(ID id) {
        std::shared_ptr<Page> page(new Page);

        page->_type = INTERNAL_NODE;
        page->_id = id;

        page->_internal_node_entries = new std::vector<InternalNodeEntry>();

        return page;
    }

    std::shared_ptr<Page> Page::new_leaf_node_page(ID id) {
        std::shared_ptr<Page> page(new Page);

        page->_type = LEAF_NODE;
        page->_id = id;

        page->_leaf.entries = new std::vector<LeafNodeEntry>();

        return page;
    }

    std::shared_ptr<Page> Page::new_page_from_stream(ID id, std::istream& stream) {
        stream.seekg(SIZE * id);

        Buffer buffer(SIZE, stream);
        BufferReader buffer_reader(buffer);

        std::shared_ptr<Page> page(new Page);

        page->_id = id;
        page->_type = buffer_reader.read<Type>();

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
        case INTERNAL_NODE: {
            size_t num_entries = buffer_reader.read<size_t>();
            page->_internal_node_entries = new std::vector<InternalNodeEntry>();
            for (size_t i = 0; i < num_entries; i++) {
                ID next_node_id = buffer_reader.read<ID>();

                size_t key_size = buffer_reader.read<size_t>();
                Buffer key(key_size);
                buffer_reader.read(key);

                page->_internal_node_entries->emplace_back(std::move(key), next_node_id);
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
            }
            break;
        }
        }

        return page;
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

    size_t Page::get_num_internal_node_entries() const {
        CHECK_EQ(_type, INTERNAL_NODE);
        return _internal_node_entries->size();
    }

    const std::vector<Page::InternalNodeEntry>* Page::get_internal_node_entries() const {
        CHECK_EQ(_type, INTERNAL_NODE);
        return _internal_node_entries;
    }

    const Page::InternalNodeEntry& Page::get_internal_node_entry(size_t i) const {
        CHECK_EQ(_type, INTERNAL_NODE);
        return _internal_node_entries->at(i);
    }

    size_t Page::get_num_leaf_node_entries() const {
        CHECK_EQ(_type, LEAF_NODE);
        return _leaf.entries->size();
    }

    const std::vector<Page::LeafNodeEntry>* Page::get_leaf_node_entries() const {
        CHECK_EQ(_type, LEAF_NODE);
        return _leaf.entries;
    }

    const Page::LeafNodeEntry& Page::get_leaf_node_entry(size_t i) const {
        CHECK_EQ(_type, LEAF_NODE);
        return _leaf.entries->at(i);
    }

    void Page::write_to_stream(std::ostream& stream) const {
        Buffer buffer(Page::SIZE);
        BufferWriter buffer_writer(buffer);

        buffer_writer.write<Type>(_type);

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
            size_t num_entries = _leaf.entries->size();
            buffer_writer.write<size_t>(num_entries);
            for (size_t i = 0; i < num_entries; i++) {
                const LeafNodeEntry& entry = _leaf.entries->at(i);

                buffer_writer.write<ID>(entry.next_data_id());
                buffer_writer.write<size_t>(entry.next_data_index());

                buffer_writer.write<size_t>(entry.key_size());
                buffer_writer.write(entry.key());
            }
            break;
        }
        }

        buffer.write_to_stream(stream);
    }

    Page::InternalNodeEntry::InternalNodeEntry(Buffer key, ID next_node_id)
        : _key(std::move(key)),
        _next_node_id(next_node_id) {}

    size_t Page::InternalNodeEntry::key_size() const {
        return _key.size();
    }

    const Buffer& Page::InternalNodeEntry::key() const {
        return _key;
    }

    Page::ID Page::InternalNodeEntry::next_node_id() const {
        return _next_node_id;
    }

    Page::LeafNodeEntry::LeafNodeEntry(Buffer key, ID next_data_id, ID next_data_index)
        : _key(std::move(key)),
        _next_data_id(next_data_id),
        _next_data_index(next_data_index) {}

    size_t Page::LeafNodeEntry::key_size() const {
        return _key.size();
    }

    const Buffer& Page::LeafNodeEntry::key() const {
        return _key;
    }

    Page::ID Page::LeafNodeEntry::next_data_id() const {
        return _next_data_id;
    }

    Page::ID Page::LeafNodeEntry::next_data_index() const {
        return _next_data_index;
    }

} // namespace diamond
