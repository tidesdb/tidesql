cmake_minimum_required(VERSION 3.25)
project(tidesql)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_POSITION_INDEPENDENT_CODE ON)

# Find MySQL/MariaDB
# Set MYSQL_DIR or MARIADB_DIR to your installation path if not found automatically
find_path(MYSQL_INCLUDE_DIR
    NAMES mysql.h
    PATHS
        /usr/include/mysql
        /usr/local/include/mysql
        /usr/include/mariadb
        /usr/local/include/mariadb
        ${MYSQL_DIR}/include
        ${MARIADB_DIR}/include
)

find_path(MYSQL_SOURCE_DIR
    NAMES sql/handler.h
    PATHS
        ${MYSQL_SOURCE}
        ${MARIADB_SOURCE}
        /usr/src/mysql
        /usr/src/mariadb
    DOC "MySQL/MariaDB source directory (needed for storage engine development)"
)

# Find TidesDB
find_path(TIDESDB_INCLUDE_DIR
    NAMES tidesdb/tidesdb.h
    PATHS
        /usr/include
        /usr/local/include
        ${TIDESDB_DIR}/include
)

find_library(TIDESDB_LIBRARY
    NAMES tidesdb
    PATHS
        /usr/lib
        /usr/local/lib
        /usr/lib64
        /usr/local/lib64
        ${TIDESDB_DIR}/lib
)

# Find compression libraries (required by TidesDB)
find_package(PkgConfig QUIET)
if(PkgConfig_FOUND)
    pkg_check_modules(LZ4 QUIET liblz4)
    pkg_check_modules(ZSTD QUIET libzstd)
    pkg_check_modules(SNAPPY QUIET snappy)
endif()

find_library(LZ4_LIBRARY NAMES lz4)
find_library(ZSTD_LIBRARY NAMES zstd)
find_library(SNAPPY_LIBRARY NAMES snappy)

# Status messages
message(STATUS "MySQL include dir: ${MYSQL_INCLUDE_DIR}")
message(STATUS "MySQL source dir: ${MYSQL_SOURCE_DIR}")
message(STATUS "TidesDB include dir: ${TIDESDB_INCLUDE_DIR}")
message(STATUS "TidesDB library: ${TIDESDB_LIBRARY}")

# Storage engine plugin
add_library(ha_tidesql SHARED
    ha_tidesql.cpp
)

target_include_directories(ha_tidesql PRIVATE
    ${CMAKE_CURRENT_SOURCE_DIR}
    ${MYSQL_INCLUDE_DIR}
    ${MYSQL_SOURCE_DIR}
    ${TIDESDB_INCLUDE_DIR}
)

target_link_libraries(ha_tidesql PRIVATE
    ${TIDESDB_LIBRARY}
    ${LZ4_LIBRARY}
    ${ZSTD_LIBRARY}
    ${SNAPPY_LIBRARY}
    pthread
)

# Set output name to match MySQL plugin naming convention
set_target_properties(ha_tidesql PROPERTIES
    OUTPUT_NAME "ha_tidesql"
    PREFIX ""
    SUFFIX ".so"
)

# Installation
install(TARGETS ha_tidesql
    LIBRARY DESTINATION lib/mysql/plugin
)

# Optional: Build a standalone test executable
option(BUILD_TESTS "Build test executable" OFF)
if(BUILD_TESTS)
    add_executable(tidesql_test
        test/test_kv_store.cpp
    )
    target_include_directories(tidesql_test PRIVATE
        ${CMAKE_CURRENT_SOURCE_DIR}
        ${TIDESDB_INCLUDE_DIR}
    )
    target_link_libraries(tidesql_test PRIVATE
        ${TIDESDB_LIBRARY}
        ${LZ4_LIBRARY}
        ${ZSTD_LIBRARY}
        ${SNAPPY_LIBRARY}
        pthread
    )
endif()
