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

#include "diamond/page_manager.h"

namespace diamond {

    PageManager::PageManager(const std::string& file_name, const Options& options)
        : _stream(file_name),
        _options(options),
        _memory_usage(0),
        _evictions(0) {}

    std::shared_ptr<Page>& PageManager::get_page(Page::Key key) {
        if (_pages.find(key) != _pages.end()) {
            std::shared_ptr<Page>& page = _pages.at(key);
            update_last_used(page);
            return page;
        }

        Page::Type type = std::get<0>(key);
        size_t id = std::get<1>(key);
        std::shared_ptr<Page> page;
        switch (type) {
        case Page::Type::DATA:
        case Page::Type::NODE: {
            size_t n = 0;
            size_t oid = 0;
            Page::Type offset_type = Page::get_offsets_type(type);
            while (true) {
                std::shared_ptr<Page> offsets = get_page(Page::make_key(offset_type, oid));
                if (!offsets) break;
                n += offsets->get_num_offsets();
                if (id < n) {
                    size_t offset = offsets->get_offset(id % offsets->get_num_offsets());
                    _stream.seekg(offset);
                    page = Page::new_page_from_stream(_stream);
                    break;
                }
                oid++;
            }
            break;
        }
        case Page::Type::DATA_OFFSETS:
        case Page::Type::NODE_OFFSETS: {
            std::shared_ptr<Page> prev_page = get_page(Page::make_key(type, id - 1));
            if (prev_page) {
                size_t offset = prev_page->get_next_offsets();
                _stream.seekg(offset);
                page = Page::new_page_from_stream(_stream);
            }
            break;
        }
        default:
            break;
        }

        if (page) {
            add_page(page);
        }

        return page;
    }

    std::shared_ptr<Page>& PageManager::get_root_data_page() {
        return get_page(Page::make_key(Page::DATA, 0));
    }

    void PageManager::write_page(std::shared_ptr<Page> page) {
        Page::Key key = page->get_key();
        if (_pages.find(key) == _pages.end()) {
            add_page(page);
        } else {
            update_last_used(page);
        }

        page->set_dirty(true);
    }

    size_t PageManager::memory_usage() const {
        return _memory_usage;
    }

    size_t PageManager::evictions() const {
        return _evictions;
    }

    void PageManager::add_page(std::shared_ptr<Page>& page) {
        if (_memory_usage > _options.eviction_threshold) {
            Page::Key key = _lru.back();
            std::shared_ptr<Page>& page = _pages[key];
            if (page->is_dirty()) {
                _stream.seekg(page->get_offset());
                page->write_to_stream(_stream);
            }
            _pages.erase(key);
            _lru.pop_back();
        }

        _pages[page->get_key()] = page;
        _memory_usage += page->memory_usage();
        update_last_used(page);
    }

    void PageManager::update_last_used(std::shared_ptr<Page>& page) {
        Page::Key key = page->get_key();
        if (_lru_node_map.find(key) != _lru_node_map.end()) {
            _lru_node_map[key] = _lru.insert(_lru.end(), key);
        } else {
            _lru.splice(_lru.end(), _lru, _lru_node_map[key]);
        }

        std::time_t time = std::time(nullptr);
        std::gmtime(&time);
        page->set_last_used(time);
    }

    void PageManager::background_writer_task() {
        while (true) {
            std::this_thread::sleep_for(
                std::chrono::milliseconds(_options.background_writer_delay));
        }
    }

} // namespace diamond
