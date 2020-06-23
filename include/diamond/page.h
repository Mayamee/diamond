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

#ifndef _DIAMOND_STORAGE_PAGE_H
#define _DIAMOND_STORAGE_PAGE_H

#include <atomic>
#include <exception>
#include <functional>
#include <list>
#include <memory>
#include <vector>

#include <boost/thread.hpp>
#include <boost/utility.hpp>

#include "diamond/buffer.h"
#include "diamond/storage.h"

namespace diamond {

    class PageAccessor;
    class SharedPageLock;
    class UniquePageLock;
    class UpgradePageLock;

    class Page : boost::noncopyable {
    public:
        using ID = uint64_t;

        static const ID INVALID_ID;

        static const uint16_t SIZE;
        static const uint16_t MAX_KEY_SIZE;

        enum class Type {
            COLLECTIONS,
            DATA,
            FREE_LIST,
            INTERNAL_NODE,
            LEAF_NODE
        };

        class Collection {
        public:
            Collection(ID root_node_id, ID free_list_id);

            ID root_node_id() const;
            ID free_list_id() const;

        private:
            ID _root_node_id;
            ID _free_list_id;
        };

        class DataEntry {
        public:
            DataEntry(Buffer data, ID overflow_id = 0, size_t overflow_index = 0);

            size_t data_size() const;
            const Buffer& data() const;

            bool overflows() const;
            ID overflow_id() const;
            size_t overflow_index() const;

        private:
            Buffer _data;
            ID _overflow_id;
            size_t _overflow_index;
        };

        class FreeListEntry {
        public:
            FreeListEntry(ID data_id, uint16_t free_space);

            ID data_id() const;
            uint16_t free_space() const;

            void set_free_space(uint16_t free_space);

        private:
            ID _data_id;
            uint16_t _free_space;
        };

        class InternalNodeEntry {
        public:
            InternalNodeEntry(ID key_data_id, size_t key_data_index, ID next_node_id);

            ID key_data_id() const;
            size_t key_data_index() const;

            ID next_node_id() const;

        private:
            ID _key_data_id;
            size_t _key_data_index;
            ID _next_node_id;
        };

        class LeafNodeEntry {
        public:
            LeafNodeEntry(ID key_data_id, size_t key_data_index, ID val_data_id, size_t val_data_index);

            ID key_data_id() const;
            size_t key_data_index() const;

            ID val_data_id() const;
            size_t val_data_index() const;

            void set_val_data_ptr(ID val_data_id, size_t val_data_index);

        private:
            ID _key_data_id;
            size_t _key_data_index;
            ID _val_data_id;
            size_t _val_data_index;
        };

        using Collections = std::unordered_map<
            Buffer,
            Collection,
            Buffer::Hash,
            Buffer::EqualTo
        >;
        using CollectionsIterator = Collections::iterator;
        using InternalNodeEntryList = std::list<InternalNodeEntry>;
        using InternalNodeEntryListIterator = InternalNodeEntryList::iterator;
        using LeafNodeEntryList = std::list<LeafNodeEntry>;
        using LeafNodeEntryListIterator = LeafNodeEntryList::iterator;

        static uint64_t file_pos_for_id(ID id);
        static Page* from_storage(ID id, Storage& storage);
        static Page* new_page(ID id, Type type);

        ~Page();

        Type get_type() const;
        ID get_id() const;

        uint16_t get_size() const;
        uint16_t get_remaining_space() const;
        uint16_t header_size() const;

        uint64_t file_pos() const;

        uint64_t usage_count() const;

        ID get_next_collections_page() const;
        void set_next_collections_page(ID next);
        const Collections* get_collections() const;
        bool can_insert_collection(const Buffer& name) const;
        bool has_collection(const Buffer& name) const;
        const Collection& get_collection(const Buffer& name) const;
        void add_collection(Buffer name, ID root_node_id, ID free_list_id);

        size_t get_num_data_entries() const;
        const std::vector<DataEntry>* get_data_entries() const;
        const DataEntry& get_data_entry(size_t i) const;
        size_t insert_data_entry(Buffer data);
        bool can_insert_data_entry(const Buffer& data);

        ID get_next_free_list_page() const;
        void set_next_free_list_page(ID next);
        size_t get_num_free_list_entries() const;
        const std::vector<FreeListEntry>* get_free_list_entries() const;
        const FreeListEntry& get_free_list_entry(size_t i) const;
        bool reserve_free_list_entry(const Buffer& data, ID& data_id);
        size_t insert_free_list_entry(ID data_id, uint16_t free_space);
        bool can_insert_free_list_entry();

        size_t get_num_internal_node_entries() const;
        const InternalNodeEntryList* get_internal_node_entries() const;
        InternalNodeEntryListIterator internal_node_entries_begin() const;
        InternalNodeEntryListIterator internal_node_entries_end() const;
        void insert_internal_node_entry(
            InternalNodeEntryListIterator pos,
            ID key_data_id,
            size_t key_data_index,
            ID next_node_id);
        bool can_insert_internal_node_entry() const;

        ID get_next_leaf_node_page() const;
        size_t get_num_leaf_node_entries() const;
        const LeafNodeEntryList* get_leaf_node_entries() const;
        LeafNodeEntryListIterator leaf_node_entries_begin() const;
        LeafNodeEntryListIterator leaf_node_entries_end() const;
        void insert_leaf_node_entry(
            LeafNodeEntryListIterator pos,
            ID key_data_id,
            size_t key_data_index,
            ID val_data_id,
            size_t val_data_index);
        bool can_insert_leaf_node_entry() const;
        void split_leaf_node_entries(Page* other);

        void write_to_storage(Storage& storage) const;
        void write_to_buffer(Buffer& buffer) const;

    private:
        friend class PageAccessor;
        friend class SharedPageLock;
        friend class UniquePageLock;
        friend class UpgradePageLock;

        ID _id;
        Type _type;
        uint16_t _size;
        union {
            struct {
                Collections* map;
                ID next;
            } _collections;
            std::vector<DataEntry>* _data_entries;
            struct {
                std::vector<FreeListEntry>* entries;
                ID next;
            } _free_list;
            InternalNodeEntryList* _internal_node_entries;
            struct {
                LeafNodeEntryList* entries;
                ID next;
            } _leaf;
        };
        std::atomic_uint64_t _usage_count;
        boost::shared_mutex _mutex;

        Page(ID id, Type type);

        static uint16_t collection_space_req(const Buffer& id) {
            return sizeof(size_t) + id.size() + sizeof(ID) + sizeof(ID);
        }

        static uint16_t data_entry_space_req(const Buffer& data) {
            return sizeof(size_t) + data.size();
        }

        static uint16_t free_list_entry_space_req() {
            return sizeof(ID) + sizeof(uint16_t);
        }

        static uint16_t internal_node_entry_space_req() {
            // key_data_id, key_data_index, child_node_id
            return sizeof(ID) + sizeof(size_t) + sizeof(ID);
        }

        static uint16_t leaf_node_entry_space_req() {
            // key_data_id, key_data_index, val_data_id, val_data_index
            return sizeof(ID) + sizeof(size_t) + sizeof(ID) + sizeof(size_t);
        }

        void ensure_type_is(Type type) const {
            if (_type != type) throw std::logic_error("invalid type");
        }

        void ensure_space_available(size_t space) const {
            if (space > get_remaining_space()) throw std::logic_error("not enough space in page");
        }
    };

} // namespace diamond

#endif // _DIAMOND_STORAGE_PAGE_H
