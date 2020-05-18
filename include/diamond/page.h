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
#include <memory>
#include <tuple>
#include <vector>

#include "diamond/buffer.h"

namespace diamond {

    class Page {
    public:
        static const uint16_t SIZE = 8192;

        using ID = uint64_t;

        enum Type {
            DATA,
            INTERNAL_NODE,
            LEAF_NODE
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
            LeafNodeEntry(Buffer key, ID next_data_id, size_t next_data_index);

            size_t key_size() const;
            const Buffer& key() const;

            ID next_data_id() const;
            size_t next_data_index() const;

        private:
            Buffer _key;
            ID _next_data_id;
            size_t _next_data_index;
        };

        static std::shared_ptr<Page> new_data_page(ID id);
        static std::shared_ptr<Page> new_internal_node_page(ID id);
        static std::shared_ptr<Page> new_leaf_node_page(ID id);

        static std::shared_ptr<Page> new_page_from_stream(ID id, std::istream& stream);

        ~Page();

        Type get_type() const;
        ID get_id() const;

        size_t get_num_data_entries() const;
        const std::vector<Buffer>* get_data_entries() const;
        const Buffer& get_data_entry(size_t i) const;

        size_t get_num_internal_node_entries() const;
        const std::vector<InternalNodeEntry>* get_internal_node_entries() const;
        const InternalNodeEntry& get_internal_node_entry(size_t i) const;

        size_t get_num_leaf_node_entries() const;
        const std::vector<LeafNodeEntry>* get_leaf_node_entries() const;
        const LeafNodeEntry& get_leaf_node_entry(size_t i) const;

        void write_to_stream(std::ostream& stream) const;

    private:
        Type _type;
        ID _id;
        union {
            std::vector<Buffer>* _data_entries;
            std::vector<InternalNodeEntry>* _internal_node_entries;
            struct {
                std::vector<LeafNodeEntry>* entries;
                uint64_t next;
            } _leaf;
        };

        Page() = default;
    };

} // namespace diamond

#endif // _DIAMOND_STORAGE_PAGE_H
