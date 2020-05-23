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

    FileStorage::FileStorage(const std::string& file_name)
        : _fs(file_name) {}

    void FileStorage::write(const char* buffer, size_t n) {
        _fs.write(buffer, n);
    }

    void FileStorage::write(const Buffer& buffer) {
        write(buffer.buffer(), buffer.size());
    }

    void FileStorage::read(char* buffer, size_t n) {
        _fs.read(buffer, n);
    }

    void FileStorage::read(Buffer& buffer, size_t n) {
        read(buffer.buffer(), n);
    }

    void FileStorage::seek(size_t n) {
        _fs.seekg(n);
    }

    size_t FileStorage::size() {
        size_t pos = _fs.tellg();
        _fs.seekg(0, std::ios::end);
        size_t size = _fs.tellg();
        _fs.seekg(pos);
        return size;
    }

} // namespace diamond
