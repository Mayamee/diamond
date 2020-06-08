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

#include "diamond/file_storage.h"

namespace diamond {

    FileStorage::FileStorage(const std::string& file_name) {
        if ((_file = fopen(file_name.c_str(), "r+")) == nullptr) {
            _file = fopen(file_name.c_str(), "w+");
        }
    }

    FileStorage::~FileStorage() {
        fclose(_file);
    }

    void FileStorage::write_impl(const char* buffer, size_t n) {
        fwrite(buffer, sizeof(char), n, _file);
    }

    void FileStorage::read_impl(char* buffer, size_t n) {
        fread(buffer, sizeof(char), n, _file);
    }

    void FileStorage::seek_impl(size_t n) {
        fseek(_file, n, SEEK_SET);
    }

    uint64_t FileStorage::size_impl() {
        uint64_t pos = ftell(_file);
        fseek(_file, 0, SEEK_END);
        uint64_t size = ftell(_file);
        fseek(_file, pos, SEEK_SET);
        return size;
    }

} // namespace diamond
