# diamond cmake file
cmake_minimum_required(VERSION 3.6.0 FATAL_ERROR)

project(DIAMOND VERSION 0.0.0 LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 17)

set(CMAKE_ARCHIVE_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR}/lib)
set(CMAKE_LIBRARY_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR}/lib)
set(CMAKE_RUNTIME_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR}/bin)

option(DIAMOND_BUILD_TESTS "enable testing" ON)
option(DIAMOND_BUILD_EXAMPLE "enable example build" OFF)

if ("${CMAKE_CXX_COMPILER_ID}" STREQUAL "GNU" OR
    "${CMAKE_CXX_COMPILER_ID}" STREQUAL "Clang")
    set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -Wall -Wextra")
elseif("${CMAKE_CXX_COMPILER_ID}" STREQUAL "MSVC")
    set(CMAKE_CXX_FLAGS  "${CMAKE_CXX_FLAGS} /W4")
endif()

include(${CMAKE_BINARY_DIR}/conanbuildinfo.cmake)
conan_basic_setup(TARGETS)

find_package(Boost COMPONENTS thread REQUIRED)

include_directories(${CMAKE_CURRENT_SOURCE_DIR}/include)
include_directories(${Boost_INCLUDE_DIRS})

add_library(diamond SHARED
    src/bg_page_writer.cpp
    src/binary_archive.cpp
    src/buffer.cpp
    src/eviction_policy.cpp
    src/exception.cpp
    src/file_storage.cpp
    src/lru_eviction_policy.cpp
    src/memory_storage.cpp
    src/page.cpp
    src/page_accessor.cpp
    src/page_manager.cpp
    src/partitioned_page_manager.cpp
    src/storage.cpp
    src/storage_engine.cpp
    src/sync_page_writer.cpp)

target_link_libraries(diamond
    pthread
    stdc++fs
    ${Boost_LIBRARIES})

if (DIAMOND_BUILD_TESTS)
    include (GoogleTest)
    enable_testing()
    add_executable(diamond_tests
        test/main.cpp
        test/page.cpp
        test/partitioned_page_manager.cpp)
    target_link_libraries(diamond_tests
        diamond
        CONAN_PKG::gtest)
    gtest_discover_tests(diamond_tests)
endif()

if (DIAMOND_BUILD_EXAMPLE)
    add_executable(diamond_example
        example/main.cpp)
    target_link_libraries(diamond_example
        diamond)
endif()
