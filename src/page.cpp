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

    size_t page_default_compare(const Buffer& b0, const Buffer& b1) {
        size_t b0_n = b0.size();
        size_t b1_n = b1.size();
        size_t n = (b0_n <= b1_n) ? b0_n : b1_n;
        size_t r = std::memcmp(b0.buffer(), b1.buffer(), n);
        if (r != 0 || b0_n == b1_n) {
            return r;
        }
        return (b0_n < b1_n) ? -1 : 1;
    }

    uint64_t page_file_pos_for_id(PageID id) {
        return PAGE_SIZE * (id - 1);
    }

    DataEntry::DataEntry(Buffer data, PageID overflow_id, size_t overflow_index)
        : _data(std::move(data)),
        _overflow_id(overflow_id),
        _overflow_index(overflow_index) {
        if (_data.size() >= PAGE_SIZE) throw std::invalid_argument("data is too large");
    }

    size_t DataEntry::data_size() const {
        return _data.size();
    }

    const Buffer& DataEntry::data() const {
        return _data;
    }

    bool DataEntry::overflows() const {
        return _overflow_id != 0;
    }

    PageID DataEntry::overflow_id() const {
        return _overflow_id;
    }

    size_t DataEntry::overflow_index() const {
        return _overflow_index;
    }

    InternalNodeEntry::InternalNodeEntry(Buffer key, PageID next_node_id)
        : _key(std::move(key)),
        _next_node_id(next_node_id) {
        if (_key.size() >= PAGE_MAX_KEY_SIZE) throw std::invalid_argument("key is too large");
    }

    size_t InternalNodeEntry::key_size() const {
        return _key.size();
    }

    const Buffer& InternalNodeEntry::key() const {
        return _key;
    }

    PageID InternalNodeEntry::next_node_id() const {
        return _next_node_id;
    }

    LeafNodeEntry::LeafNodeEntry(Buffer key, PageID data_id, PageID data_index)
        : _key(std::move(key)),
        _data_id(data_id),
        _data_index(data_index) {
        if (_key.size() >= PAGE_MAX_KEY_SIZE) throw std::invalid_argument("key is too large");
    }

    size_t LeafNodeEntry::key_size() const {
        return _key.size();
    }

    const Buffer& LeafNodeEntry::key() const {
        return _key;
    }

    PageID LeafNodeEntry::data_id() const {
        return _data_id;
    }

    PageID LeafNodeEntry::data_index() const {
        return _data_index;
    }

    PageRep::~PageRep() {
        switch (_type) {
        case PageType::DATA:
            delete _data_entries;
            break;
        case PageType::INTERNAL_NODE:
            delete _internal_node_entries;
            break;
        case PageType::LEAF_NODE:
            delete _leaf.entries;
            break;
        }
    }

    PageType PageRep::get_type() const {
        return _type;
    }

    PageID PageRep::get_id() const {
        return _id;
    }

    uint16_t PageRep::get_size() const {
        return _size;
    }

    uint16_t PageRep::get_remaining_space() const {
        return PAGE_SIZE - _size;
    }

    size_t PageRep::header_size() const {
        size_t size = sizeof(PageType);
        switch (_type) {
        case PageType::DATA:
            return size + sizeof(size_t);
        case PageType::INTERNAL_NODE:
            return size + sizeof(size_t);
        case PageType::LEAF_NODE:
            return size + sizeof(PageID) + sizeof(size_t);
        }
    }

    uint64_t PageRep::file_pos() const {
        return page_file_pos_for_id(_id);
    }

    uint64_t PageRep::usage_count() const {
        return _use_count.load(std::memory_order::memory_order_acquire);
    }

    size_t PageRep::get_num_data_entries() const {
        ensure_type_is(PageType::DATA);
        return _data_entries->size();
    }

    const std::vector<DataEntry>* PageRep::get_data_entries() const {
        ensure_type_is(PageType::DATA);
        return _data_entries;
    }

    const DataEntry& PageRep::get_data_entry(size_t i) const {
        ensure_type_is(PageType::DATA);
        return _data_entries->at(i);
    }

    void PageRep::insert_data_entry(const Buffer& data, PageID overflow_id, size_t overflow_index) {
        ensure_type_is(PageType::DATA);
        size_t space = data.size();
        if (overflow_id > 0) space += sizeof(overflow_id) + sizeof(overflow_index);
        ensure_space_available(space);

        _data_entries->emplace_back(data, overflow_id, overflow_index);
        _size += space;
    }

    size_t PageRep::get_num_internal_node_entries() const {
        ensure_type_is(PageType::INTERNAL_NODE);
        return _internal_node_entries->size();
    }

    const std::vector<InternalNodeEntry>* PageRep::get_internal_node_entries() const {
        ensure_type_is(PageType::INTERNAL_NODE);
        return _internal_node_entries;
    }

    const InternalNodeEntry& PageRep::get_internal_node_entry(size_t i) const {
        ensure_type_is(PageType::INTERNAL_NODE);
        return _internal_node_entries->at(i);
    }

    size_t PageRep::search_internal_node_entries(const Buffer& key, PageCompare compare) const {
        ensure_type_is(PageType::INTERNAL_NODE);
        size_t n = _internal_node_entries->size();
        for (size_t i = 0; i < n - 1; i++) {
            if (compare(_internal_node_entries->at(i).key(), key) > 0) {
                return i;
            }
        }
        return n - 1;
    }

    void PageRep::insert_internal_node_entry(const Buffer& key, PageID next_node_id) {
        ensure_type_is(PageType::INTERNAL_NODE);
        size_t space = key.size() + sizeof(PageID);
        ensure_space_available(space);

        _internal_node_entries->emplace_back(key, next_node_id);
        _size += space;
    }

    bool PageRep::can_insert_internal_node_entry(const Buffer& key) const {
        ensure_type_is(PageType::INTERNAL_NODE);
        size_t space = key.size() + sizeof(PageID);
        return get_remaining_space() >= space;
    }

    size_t PageRep::get_num_leaf_node_entries() const {
        ensure_type_is(PageType::LEAF_NODE);
        return _leaf.entries->size();
    }

    const std::vector<LeafNodeEntry>* PageRep::get_leaf_node_entries() const {
        ensure_type_is(PageType::LEAF_NODE);
        return _leaf.entries;
    }

    const LeafNodeEntry& PageRep::get_leaf_node_entry(size_t i) const {
        ensure_type_is(PageType::LEAF_NODE);
        return _leaf.entries->at(i);
    }

    bool PageRep::find_leaf_node_entry(const Buffer& key, PageCompare compare, size_t& res) const {
        ensure_type_is(PageType::LEAF_NODE);
        size_t n = _leaf.entries->size();
        for (size_t i = 0; i < n; i++) {
            if (compare(_leaf.entries->at(i).key(), key) == 0) {
                res = i;   
                return true;
            }
        }
        return false;
    }

    void PageRep::insert_leaf_node_entry(const Buffer& key, PageID data_id, size_t data_index) {
        ensure_type_is(PageType::LEAF_NODE);
        size_t space = key.size() + sizeof(PageID) + sizeof(size_t);
        ensure_space_available(space);

        _leaf.entries->emplace_back(key, data_id, data_index);
        _size += space;
    }

    bool PageRep::can_insert_leaf_node_entry(const Buffer& key) const {
        ensure_type_is(PageType::LEAF_NODE);
        size_t space = key.size() + sizeof(PageID) + sizeof(size_t);
        return get_remaining_space() >= space;
    }

    void PageRep::write_to_storage(Storage& storage) const {
        Buffer buffer(PAGE_SIZE);
        write_to_buffer(buffer);
        buffer.write_to_storage(storage, file_pos());
    }

    void PageRep::write_to_buffer(Buffer& buffer) const {
        if (buffer.size() < _size) throw std::logic_error("buffer is too small to fit entire page data.");
        BufferWriter buffer_writer(buffer);

        buffer_writer.write<PageType>(_type);
        switch (_type) {
        case PageType::DATA: {
            size_t num_entries = _data_entries->size();
            buffer_writer.write<size_t>(num_entries);
            for (size_t i = 0; i < num_entries; i++) {
                const DataEntry& entry = _data_entries->at(i);

                buffer_writer.write<size_t>(entry.data_size());
                buffer_writer.write(entry.data());

                if (entry.overflows()) {
                    buffer_writer.write<PageID>(entry.overflow_id());
                    buffer_writer.write<size_t>(entry.overflow_index());
                }
            }
            break;
        }
        case PageType::INTERNAL_NODE: {
            size_t num_entries = _internal_node_entries->size();
            buffer_writer.write<size_t>(num_entries);
            for (size_t i = 0; i < num_entries; i++) {
                const InternalNodeEntry& entry = _internal_node_entries->at(i);

                buffer_writer.write<PageID>(entry.next_node_id());

                buffer_writer.write<size_t>(entry.key_size());
                buffer_writer.write(entry.key());
            }
            break;
        }
        case PageType::LEAF_NODE: {
            buffer_writer.write<PageID>(_leaf.next);
            size_t num_entries = _leaf.entries->size();
            buffer_writer.write<size_t>(num_entries);
            for (size_t i = 0; i < num_entries; i++) {
                const LeafNodeEntry& entry = _leaf.entries->at(i);

                buffer_writer.write<PageID>(entry.data_id());
                buffer_writer.write<size_t>(entry.data_index());

                buffer_writer.write<size_t>(entry.key_size());
                buffer_writer.write(entry.key());
            }
            break;
        }
        }
    }

    PageRep* PageRep::from_storage(PageID id, Storage& storage) {
        if (id == 0) throw std::invalid_argument("page ids must be greater than 0");

        if (storage.size() < PAGE_SIZE * id) return nullptr;

        Buffer buffer(storage, PAGE_SIZE, page_file_pos_for_id(id));
        BufferReader buffer_reader(buffer);

        PageRep* page = new PageRep(id, buffer_reader.read<PageType>());
        switch (page->_type) {
        case PageType::DATA: {
            size_t num_entries = buffer_reader.read<size_t>();
            page->_data_entries = new std::vector<DataEntry>();
            for (size_t i = 0; i < num_entries; i++) {
                size_t data_size = buffer_reader.read<size_t>();
                size_t rem = buffer_reader.bytes_remaining();
                if (rem < data_size) {
                    PageID overflow_id;
                    size_t overflow_index;

                    size_t to_read = rem - (sizeof(overflow_id) + sizeof(overflow_index));
                    Buffer data(to_read);
                    buffer_reader.read(data);

                    overflow_id = buffer_reader.read<PageID>();
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
        case PageType::INTERNAL_NODE: {
            size_t num_entries = buffer_reader.read<size_t>();
            page->_internal_node_entries = new std::vector<InternalNodeEntry>();
            for (size_t i = 0; i < num_entries; i++) {
                PageID next_node_id = buffer_reader.read<PageID>();

                size_t key_size = buffer_reader.read<size_t>();
                Buffer key(key_size);
                buffer_reader.read(key);

                page->_internal_node_entries->emplace_back(std::move(key), next_node_id);
                page->_size += key_size + sizeof(next_node_id);
            }
            break;
        }
        case PageType::LEAF_NODE: {
            page->_leaf.next = buffer_reader.read<PageID>();
            size_t num_entries = buffer_reader.read<size_t>();
            page->_leaf.entries = new std::vector<LeafNodeEntry>();
            for (size_t i = 0; i < num_entries; i++) {
                PageID data_id = buffer_reader.read<PageID>();
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

    PageRep::PageRep(PageID id, PageType type)
            : _id(id),
            _type(type),
            _size(header_size()),
            _use_count(1) {
        switch (type) {
        case PageType::DATA:
            _data_entries = new std::vector<DataEntry>();
            break;
        case PageType::INTERNAL_NODE:
            _internal_node_entries = new std::vector<InternalNodeEntry>();
            break;
        case PageType::LEAF_NODE:
            _leaf.next = 0;
            _leaf.entries = new std::vector<LeafNodeEntry>();
            break;
        }
    }

    Page Page::from_storage(PageID id, Storage& storage) {
        return Page(PageRep::from_storage(id, storage));
    }

    Page::Page(PageID id, PageType type)
        : _rep(new PageRep(id, type)) {}

    Page::Page(const Page& page)
            : _rep(page._rep) {
        _rep->_use_count++;
    }

    Page::~Page() {
        if (_rep && --_rep->_use_count == 0) {
            delete _rep;
        } 
    }

    PageRep* Page::operator->() const {
        return _rep;
    }

    Page::operator bool() const {
        return _rep != nullptr;
    }

    Page::Page(PageRep* rep)
        : _rep(rep) {}

} // namespace diamond
