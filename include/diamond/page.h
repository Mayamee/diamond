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

#ifndef _DIAMOND_STORAGE_PAGE_H
#define _DIAMOND_STORAGE_PAGE_H

#include <algorithm>
#include <ctime>
#include <exception>
#include <memory>
#include <tuple>
#include <vector>

namespace diamond {

    class Page {
    public:
        static const size_t PAGE_SIZE = 8192;

        enum Type {
            DATA,
            DATA_OFFSETS,
            NODE,
            NODE_OFFSETS,
            // Special Type for some static methods,
            // instance can never be of this type
            NONE
        };

        using Key = std::tuple<Type, size_t>;

        class DataEntry {
        public:
            DataEntry(size_t size, void* val);

            size_t size() const;
            const void* val() const;

        private:
            size_t _size;
            void* _val;
        };

        class NodeEntry {
        public:
            NodeEntry(size_t key_size, void* key, Page::Key next);

            size_t key_size() const;
            const void* key() const;
            Page::Key next() const;

            size_t compare(const void* other, size_t size) const;

        private:
            size_t _key_size;
            void* _key;
            Page::Key _next;
        };

        struct KeyHash {
            size_t operator()(const Key& key) const {
                return std::get<1>(key);
            }
        };

        static Key make_key(Type type, size_t id);

        static Type get_offsets_type(Type type);

        // static std::shared_ptr<Page> new_node_offsets_page(
        //     size_t id,
        //     size_t offset,
        //     std::vector<size_t>& offsets,
        //     size_t next = 0);

        static std::shared_ptr<Page> new_page_from_stream(std::istream& stream);

        ~Page();

        Type get_type() const;
        size_t get_id() const;
        Key get_key() const;

        size_t get_offset() const;

        std::time_t last_used() const;
        void set_last_used(std::time_t last_used);

        bool is_dirty() const;
        void set_dirty(bool dirty);

        size_t get_num_data_entries() const;
        const std::vector<DataEntry>* get_data_entries() const;
        const DataEntry& get_data_entry(size_t i) const;

        size_t get_num_node_entries() const;
        const std::vector<NodeEntry>* get_node_entries() const;
        const NodeEntry& get_node_entry(size_t i) const;
        const NodeEntry& search_node_entries(const void* key, size_t size) const;
        bool is_leaf_node() const;

        size_t get_num_offsets() const;
        const std::vector<size_t>* get_offsets() const;
        size_t get_offset(size_t i) const;
        size_t get_next_offsets() const;

        size_t memory_usage() const;

        void write_to_stream(std::ostream& stream) const;

    private:
        Type _type;
        size_t _id;
        size_t _offset;
        std::time_t _last_used;
        bool _is_dirty = false;

        union {
            struct {
                std::vector<DataEntry>* entries;
            } _data;
            struct {
                std::vector<NodeEntry>* entries;
                bool is_leaf;
            } _node;
            struct {
                std::vector<size_t>* offsets;
                size_t next;
            } _offsets;
        };

        Page(
            size_t id,
            size_t offset,
            std::vector<DataEntry>& entries);

        Page(
            size_t id,
            size_t offset,
            std::vector<NodeEntry>& entries,
            bool is_leaf = false);

        Page(
            size_t id,
            size_t offset,
            std::vector<size_t>& offsets,
            size_t next = 0);

        void ensure_type(Type type) const {
            if (_type != type) throw std::logic_error("invalid type");
        }

        void ensure_type_oneof(const std::vector<Type>& types) const {
            if (std::find(types.begin(), types.end(), _type) == types.end()) throw std::logic_error("invalid type");
        }
    };

} // namespace diamond

#endif // _DIAMOND_STORAGE_PAGE_H
