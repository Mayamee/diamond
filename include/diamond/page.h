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

#ifndef _DIAMOND_STORAGE_PAGE_H
#define _DIAMOND_STORAGE_PAGE_H

#include <algorithm>
#include <ctime>
#include <exception>
#include <functional>
#include <memory>
#include <tuple>
#include <vector>

#include "diamond/buffer.h"

namespace diamond {

    class Page {
    public:
        static const uint16_t SIZE = 8192;
        static const uint16_t MAX_KEY_SIZE = SIZE / 4;

        using ID = uint64_t;
        using Compare = std::function<size_t(const Buffer&, const Buffer&)>;

        enum Type {
            DATA,
            INTERNAL_NODE,
            LEAF_NODE
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

        class InternalNodeEntry {
        public:
            InternalNodeEntry(Buffer key, ID next_node_id);

            size_t key_size() const;
            const Buffer& key() const;

            ID next_node_id() const;

        private:
            Buffer _key;
            ID _next_node_id;
        };

        class LeafNodeEntry {
        public:
            LeafNodeEntry(Buffer key, ID data_id, size_t data_index);

            size_t key_size() const;
            const Buffer& key() const;

            ID data_id() const;
            size_t data_index() const;

        private:
            Buffer _key;
            ID _data_id;
            size_t _data_index;
        };

        static std::shared_ptr<Page> new_page(ID id, Type type);
        static std::shared_ptr<Page> new_data_page(ID id);
        static std::shared_ptr<Page> new_internal_node_page(ID id);
        static std::shared_ptr<Page> new_leaf_node_page(ID id);

        static std::shared_ptr<Page> new_page_from_stream(ID id, std::istream& stream);

        static size_t default_compare(const Buffer& b0, const Buffer& b1);

        ~Page();

        Type get_type() const;
        ID get_id() const;
        uint16_t get_size() const;
        uint16_t get_remaining_space() const;
        size_t header_size() const;

        size_t get_num_data_entries() const;
        const std::vector<DataEntry>* get_data_entries() const;
        const DataEntry& get_data_entry(size_t i) const;
        void insert_data_entry(const Buffer& data, ID overflow_id = 0, size_t overflow_index = 0);

        size_t get_num_internal_node_entries() const;
        const std::vector<InternalNodeEntry>* get_internal_node_entries() const;
        const InternalNodeEntry& get_internal_node_entry(size_t i) const;
        size_t search_internal_node_entries(const Buffer& key, Compare compare) const;
        void insert_internal_node_entry(const Buffer& key, ID next_node_id);

        size_t get_num_leaf_node_entries() const;
        const std::vector<LeafNodeEntry>* get_leaf_node_entries() const;
        const LeafNodeEntry& get_leaf_node_entry(size_t i) const;
        bool find_leaf_node_entry(const Buffer& key, Compare compare, size_t& res) const;
        void insert_leaf_node_entry(const Buffer& key, ID data_id, size_t data_index);

        void write_to_stream(std::ostream& stream) const;

    private:
        Type _type;
        ID _id;
        uint16_t _size;
        union {
            std::vector<DataEntry>* _data_entries;
            std::vector<InternalNodeEntry>* _internal_node_entries;
            struct {
                std::vector<LeafNodeEntry>* entries;
                uint64_t next;
            } _leaf;
        };

        Page() = default;

        void ensure_type_is(Type type) const {
            if (_type != type) throw std::logic_error("invalid type");
        }

        void ensure_space_available(size_t space) {
            if (space > get_remaining_space()) throw std::logic_error("not enough space in page");
        }
    };

} // namespace diamond

#endif // _DIAMOND_STORAGE_PAGE_H
