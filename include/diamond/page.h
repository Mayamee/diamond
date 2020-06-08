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

#ifndef _DIAMOND_STORAGE_PageRep_H
#define _DIAMOND_STORAGE_PageRep_H

#include <atomic>
#include <exception>
#include <functional>
#include <memory>
#include <vector>

#include <boost/utility.hpp>

#include "diamond/buffer.h"
#include "diamond/storage.h"

namespace diamond {

    using PageID = uint64_t;
    using PageCompare = std::function<size_t(const Buffer&, const Buffer&)>;

    const PageID INVALID_PAGE = 0;

    const uint16_t PAGE_SIZE = 8192;
    const uint16_t PAGE_MAX_KEY_SIZE = PAGE_SIZE / 4;

    enum class PageType {
        DATA,
        FREE_LIST,
        INTERNAL_NODE,
        LEAF_NODE
    };

    class Page;

    size_t page_default_compare(const Buffer& b0, const Buffer& b1);
    uint64_t page_file_pos_for_id(PageID id);

    class DataEntry {
    public:
        DataEntry(Buffer data, PageID overflow_id = 0, size_t overflow_index = 0);

        size_t data_size() const;
        const Buffer& data() const;

        bool overflows() const;
        PageID overflow_id() const;
        size_t overflow_index() const;

    private:
        Buffer _data;
        PageID _overflow_id;
        size_t _overflow_index;
    };

    class FreeListEntry {
    public:
        FreeListEntry(PageID data_id, uint16_t free_space);

        PageID data_id() const;
        uint16_t free_space() const;

        void set_free_space(uint16_t free_space);

    private:
        PageID _data_id;
        uint16_t _free_space;
    };

    class InternalNodeEntry {
    public:
        InternalNodeEntry(Buffer key, PageID next_node_id);

        size_t key_size() const;
        const Buffer& key() const;

        PageID next_node_id() const;

    private:
        Buffer _key;
        PageID _next_node_id;
    };

    class LeafNodeEntry {
    public:
        LeafNodeEntry(Buffer key, PageID data_id, size_t data_index);

        size_t key_size() const;
        const Buffer& key() const;

        PageID data_id() const;
        size_t data_index() const;

    private:
        Buffer _key;
        PageID _data_id;
        size_t _data_index;
    };

    class PageRep : boost::noncopyable {
    public:
        ~PageRep();

        PageType get_type() const;
        PageID get_id() const;

        uint16_t get_size() const;
        uint16_t get_remaining_space() const;
        uint16_t header_size() const;

        uint64_t file_pos() const;

        uint64_t usage_count() const;

        size_t get_num_data_entries() const;
        const std::vector<DataEntry>* get_data_entries() const;
        const DataEntry& get_data_entry(size_t i) const;
        size_t insert_data_entry(const Buffer& data);
        bool can_insert_data_entry(const Buffer& data);

        PageID get_next_free_list_page() const;
        void set_next_free_list_page(PageID next);
        size_t get_num_free_list_entries() const;
        const std::vector<FreeListEntry>* get_free_list_entries() const;
        const FreeListEntry& get_free_list_entry(size_t i) const;
        bool reserve_free_list_entry(const Buffer& data, PageID& data_id);
        size_t insert_free_list_entry(PageID data_id, uint16_t free_space);
        bool can_insert_free_list_entry();

        size_t get_num_internal_node_entries() const;
        const std::vector<InternalNodeEntry>* get_internal_node_entries() const;
        const InternalNodeEntry& get_internal_node_entry(size_t i) const;
        size_t search_internal_node_entries(const Buffer& key, PageCompare compare) const;
        size_t insert_internal_node_entry(const Buffer& key, PageID next_node_id);
        bool can_insert_internal_node_entry(const Buffer& key) const;

        PageID get_next_leaf_node_page() const;
        size_t get_num_leaf_node_entries() const;
        const std::vector<LeafNodeEntry>* get_leaf_node_entries() const;
        const LeafNodeEntry& get_leaf_node_entry(size_t i) const;
        bool find_leaf_node_entry(const Buffer& key, PageCompare compare, size_t& res) const;
        size_t insert_leaf_node_entry(const Buffer& key, PageID data_id, size_t data_index);
        bool can_insert_leaf_node_entry(const Buffer& key) const;

        void write_to_storage(Storage& storage) const;
        void write_to_buffer(Buffer& buffer) const;

    private:
        friend class Page;

        PageID _id;
        PageType _type;
        uint16_t _size;
        union {
            std::vector<DataEntry>* _data_entries;
            struct {
                std::vector<FreeListEntry>* entries;
                PageID next;
            } _free_list;
            std::vector<InternalNodeEntry>* _internal_node_entries;
            struct {
                std::vector<LeafNodeEntry>* entries;
                PageID next;
            } _leaf;
        };
        std::atomic_uint64_t _use_count;

        static PageRep* from_storage(PageID id, Storage& storage);

        PageRep(PageID id, PageType type);

        uint16_t data_entry_space_req(const Buffer& data) const {
            return sizeof(size_t) + data.size();
        }

        uint16_t free_list_entry_space_req() const {
            return sizeof(PageID) + sizeof(uint16_t);
        }

        uint16_t internal_node_entry_space_req(const Buffer& key) const {
            return sizeof(size_t) + key.size() + sizeof(PageID);
        }

        uint16_t leaf_node_entry_space_req(const Buffer& key) const {
            return sizeof(size_t) + key.size() + sizeof(PageID) + sizeof(size_t);
        }

        void ensure_type_is(PageType type) const {
            if (_type != type) throw std::logic_error("invalid type");
        }

        void ensure_space_available(size_t space) const {
            if (space > get_remaining_space()) throw std::logic_error("not enough space in page");
        }
    };

    class Page {
    public:
        static Page from_storage(PageID id, Storage& storage);

        Page(PageID id, PageType type);
        Page(const Page& page);
        ~Page();

        PageRep* operator->() const;

        operator bool() const;

    private:
        PageRep* _rep;

        Page(PageRep* rep);
    };

} // namespace diamond

#endif // _DIAMOND_STORAGE_PageRep_H
