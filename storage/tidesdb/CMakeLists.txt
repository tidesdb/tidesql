# Copyright (c) 2026 TidesDB
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

# Add include path for TidesDB headers
INCLUDE_DIRECTORIES(/usr/local/include /usr/include)

# Find TidesDB library and compression libraries
FIND_LIBRARY(TIDESDB_LIB tidesdb PATHS /usr/local/lib /usr/lib)
FIND_LIBRARY(ZSTD_LIB zstd PATHS /usr/local/lib /usr/lib)
FIND_LIBRARY(LZ4_LIB lz4 PATHS /usr/local/lib /usr/lib)
FIND_LIBRARY(SNAPPY_LIB snappy PATHS /usr/local/lib /usr/lib)

# Set source files
SET(TIDESDB_SOURCES ha_tidesdb.cc)

# Set libraries to link (used by MYSQL_STORAGE_ENGINE macro)
SET(TIDESDB_LIBS "")
IF(TIDESDB_LIB)
  LIST(APPEND TIDESDB_LIBS ${TIDESDB_LIB})
ELSE()
  # Fallback to library name if not found by path
  LIST(APPEND TIDESDB_LIBS tidesdb)
ENDIF()

IF(ZSTD_LIB)
  LIST(APPEND TIDESDB_LIBS ${ZSTD_LIB})
ELSE()
  LIST(APPEND TIDESDB_LIBS zstd)
ENDIF()

IF(LZ4_LIB)
  LIST(APPEND TIDESDB_LIBS ${LZ4_LIB})
ELSE()
  LIST(APPEND TIDESDB_LIBS lz4)
ENDIF()

IF(SNAPPY_LIB)
  LIST(APPEND TIDESDB_LIBS ${SNAPPY_LIB})
ELSE()
  LIST(APPEND TIDESDB_LIBS snappy)
ENDIF()

# Define the storage engine (uses TIDESDB_SOURCES and TIDESDB_LIBS)
MYSQL_STORAGE_ENGINE(TIDESDB)
