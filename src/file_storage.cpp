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

#include <filesystem>

#include "diamond/file_storage.h"

namespace diamond {

    FileStorage::FileStorage(const std::string& file_name) {
        if (std::filesystem::exists(file_name)) {
            _fs.open(file_name);
        } else {
            // stupid way of making it create a file
            _fs.open(file_name, std::ios::out);
            _fs.close();
            _fs.open(file_name);
        }
    }

    void FileStorage::write_impl(const char* buffer, size_t n) {
        _fs.write(buffer, n);
    }

    void FileStorage::read_impl(char* buffer, size_t n) {
        _fs.read(buffer, n);
    }

    void FileStorage::seek_impl(size_t n) {
        _fs.seekg(n);
    }

    uint64_t FileStorage::size_impl() {
        std::streampos pos = _fs.tellg();
        _fs.seekg(0, std::ios::end);
        std::streampos size = _fs.tellg() - pos;
        _fs.seekg(pos);
        return size;
    }

} // namespace diamond
