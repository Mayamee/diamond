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

#include "diamond/buffer.h"

namespace diamond {

    class Page {
    public:
        static const uint16_t SIZE = 8192;

        enum Type {
            DATA,
            DATA_OFFSETS,
            NODE,
            NODE_OFFSETS,
            // Special Type for some static methods,
            // instance can never be of this type
            NONE
        };

        using Key = std::tuple<Type, uint64_t>;

        class NodeEntry {
        public:
            NodeEntry(Buffer key, uint64_t data_id, size_t data_index);
            NodeEntry(Buffer key, uint64_t node_id);

            size_t key_size() const;
            const Buffer& key() const;

            uint64_t next_data_id() const;
            size_t next_data_index() const;

            uint64_t next_node_id() const;

            Page::Type next_type() const;
            Page::Key next_page_key() const;

            size_t compare(const char* other, size_t size) const;

        private:
            Buffer _key;

            Page::Type _next_page_type;
            union {
                struct {
                    uint64_t id;
                    size_t index;
                } _next_data_page;
                uint64_t _next_node_page_id;
            };
        };

        struct KeyHash {
            uint64_t operator()(const Key& key) const {
                return std::get<1>(key);
            }
        };

        static Key make_key(Type type, size_t id);

        static Type get_offsets_type(Type type);

        static std::shared_ptr<Page> new_page_from_stream(std::istream& stream);

        ~Page();

        Type get_type() const;
        uint64_t get_id() const;
        Key get_key() const;

        uint64_t get_offset() const;

        std::time_t last_used() const;
        void set_last_used(std::time_t last_used);

        bool is_dirty() const;
        void set_dirty(bool dirty);

        size_t get_num_data_entries() const;
        const std::vector<Buffer>* get_data_entries() const;
        const Buffer& get_data_entry(size_t i) const;

        size_t get_num_node_entries() const;
        const std::vector<NodeEntry>* get_node_entries() const;
        const NodeEntry& get_node_entry(size_t i) const;
        const NodeEntry& search_node_entries(const char* key, size_t size) const;
        bool is_leaf_node() const;

        size_t get_num_offsets() const;
        const std::vector<size_t>* get_offsets() const;
        uint64_t get_offset(size_t i) const;
        uint64_t get_next_offsets() const;

        size_t memory_usage() const;

        void write_to_stream(std::ostream& stream) const;

    private:
        Type _type;
        uint64_t _id;
        uint64_t _offset;
        std::time_t _last_used;
        bool _is_dirty = false;

        union {
            std::vector<Buffer>* _data_entries;
            struct {
                std::vector<NodeEntry>* entries;
                bool is_leaf;
            } _node;
            struct {
                std::vector<uint64_t>* offsets;
                uint64_t next;
            } _offsets;
        };

        Page(
            uint64_t id,
            uint64_t offset,
            std::vector<Buffer> entries);

        Page(
            uint64_t id,
            uint64_t offset,
            std::vector<NodeEntry> entries,
            bool is_leaf = false);

        Page(
            uint64_t id,
            uint64_t offset,
            std::vector<uint64_t> offsets,
            uint64_t next = 0);
    };

} // namespace diamond

#endif // _DIAMOND_STORAGE_PAGE_H
