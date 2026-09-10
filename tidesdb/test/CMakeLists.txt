# Copyright (c) 2026 TidesDB Corp.
#
# This program is free software; you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation; version 2 of the License.

# Standalone unit-test build for the server-free plugin core. This project links only
# src/core, so it needs neither a MariaDB nor a MySQL source tree and runs on the local
# box. Build and run with
#
#   cmake -S tidesdb/test -B build-coretests -DCMAKE_BUILD_TYPE=Debug
#   cmake --build build-coretests --parallel
#   ctest --test-dir build-coretests --output-on-failure
#
# Sanitizers are on by default so ASAN and UBSAN gate every run; add
# -DTIDESQL_CORE_TESTS_SANITIZE=OFF to turn them off.

cmake_minimum_required(VERSION 3.16)
project(tidesql_core_tests CXX)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

if(NOT CMAKE_BUILD_TYPE)
  set(CMAKE_BUILD_TYPE Debug)
endif()

option(TIDESQL_CORE_TESTS_SANITIZE "Build the core unit tests with ASAN and UBSAN" ON)

add_compile_options(-Wall -Wextra -Werror)
if(TIDESQL_CORE_TESTS_SANITIZE)
  add_compile_options(-fsanitize=address,undefined -fno-omit-frame-pointer)
  add_link_options(-fsanitize=address,undefined)
endif()

# The server-free core, compiled once and shared by every test binary. Add a new core
# source here as each module is extracted.
add_library(tidesdb_core STATIC
  ${CMAKE_CURRENT_SOURCE_DIR}/../src/core/spatial.cc
  ${CMAKE_CURRENT_SOURCE_DIR}/../src/core/fts_text.cc
  ${CMAKE_CURRENT_SOURCE_DIR}/../src/core/sort_key.cc
  ${CMAKE_CURRENT_SOURCE_DIR}/../src/core/row_format.cc
  ${CMAKE_CURRENT_SOURCE_DIR}/../src/core/rir.cc
  ${CMAKE_CURRENT_SOURCE_DIR}/../src/core/cf_name.cc
  ${CMAKE_CURRENT_SOURCE_DIR}/../src/core/fts_score.cc
)
target_include_directories(tidesdb_core PUBLIC ${CMAKE_CURRENT_SOURCE_DIR}/../src/core)

enable_testing()

# One executable per <module>__tests.cc, mirroring the tidesdb library test layout. Add a
# module name here as its test file lands.
set(TIDESQL_CORE_TEST_MODULES
  spatial
  fts_text
  sort_key
  row_format
  rir
  cf_name
  fts_score
)

foreach(mod ${TIDESQL_CORE_TEST_MODULES})
  add_executable(${mod}_tests ${CMAKE_CURRENT_SOURCE_DIR}/${mod}__tests.cc)
  target_link_libraries(${mod}_tests PRIVATE tidesdb_core)
  add_test(NAME ${mod} COMMAND ${mod}_tests)
  set_tests_properties(${mod} PROPERTIES
    ENVIRONMENT "ASAN_OPTIONS=abort_on_error=1;UBSAN_OPTIONS=halt_on_error=1:print_stacktrace=1")
endforeach()
