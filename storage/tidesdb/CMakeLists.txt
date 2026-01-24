# Copyright (c) 2024 TidesDB
# 
# This program is free software; you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation; version 2 of the License.
# 
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
# 
# You should have received a copy of the GNU General Public License
# along with this program; if not, write to the Free Software
# Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA  02110-1301  USA

SET(CMAKE_CXX_FLAGS_DEBUG "${CMAKE_CXX_FLAGS_DEBUG} -DSAFEMALLOC -DSAFE_MUTEX")
SET(CMAKE_C_FLAGS_DEBUG "${CMAKE_C_FLAGS_DEBUG} -DSAFEMALLOC -DSAFE_MUTEX")

INCLUDE("${PROJECT_SOURCE_DIR}/storage/mysql_storage_engine.cmake")

SET(TIDESDB_SOURCES ha_tidesdb.cc)

# Find TidesDB library
FIND_LIBRARY(TIDESDB_LIB tidesdb)
FIND_LIBRARY(ZSTD_LIB zstd)
FIND_LIBRARY(LZ4_LIB lz4)
FIND_LIBRARY(SNAPPY_LIB snappy)

# Add include path for TidesDB headers
INCLUDE_DIRECTORIES(/usr/local/include)

# Define the storage engine
MYSQL_STORAGE_ENGINE(TIDESDB)

# Link against TidesDB and compression libraries
IF(TIDESDB_LIB)
  TARGET_LINK_LIBRARIES(tidesdb ${TIDESDB_LIB})
ENDIF()

IF(ZSTD_LIB)
  TARGET_LINK_LIBRARIES(tidesdb ${ZSTD_LIB})
ENDIF()

IF(LZ4_LIB)
  TARGET_LINK_LIBRARIES(tidesdb ${LZ4_LIB})
ENDIF()

IF(SNAPPY_LIB)
  TARGET_LINK_LIBRARIES(tidesdb ${SNAPPY_LIB})
ENDIF()
