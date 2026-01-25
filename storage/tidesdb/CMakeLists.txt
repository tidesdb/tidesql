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

# Find TidesDB library
FIND_LIBRARY(TIDESDB_LIB 
  NAMES tidesdb
  HINTS 
    ENV TIDESDB_ROOT
    ENV LIBRARY_PATH
  PATHS
    /usr/local/lib
    /usr/lib
    /usr/lib/x86_64-linux-gnu
    /opt/local/lib
)

# Find compression libraries using find_package when available
FIND_PACKAGE(PkgConfig QUIET)
IF(PKG_CONFIG_FOUND)
  PKG_CHECK_MODULES(ZSTD QUIET libzstd)
  PKG_CHECK_MODULES(LZ4 QUIET liblz4)
ENDIF()

# Fallback to FIND_LIBRARY if pkg-config didn't find them
IF(NOT ZSTD_FOUND)
  FIND_LIBRARY(ZSTD_LIBRARIES
    NAMES zstd
    HINTS ENV LIBRARY_PATH
    PATHS /usr/local/lib /usr/lib /usr/lib/x86_64-linux-gnu
  )
  IF(ZSTD_LIBRARIES)
    SET(ZSTD_FOUND TRUE)
  ENDIF()
ENDIF()

IF(NOT LZ4_FOUND)
  FIND_LIBRARY(LZ4_LIBRARIES
    NAMES lz4
    HINTS ENV LIBRARY_PATH
    PATHS /usr/local/lib /usr/lib /usr/lib/x86_64-linux-gnu
  )
  IF(LZ4_LIBRARIES)
    SET(LZ4_FOUND TRUE)
  ENDIF()
ENDIF()

FIND_LIBRARY(SNAPPY_LIBRARIES
  NAMES snappy
  HINTS ENV LIBRARY_PATH
  PATHS /usr/local/lib /usr/lib /usr/lib/x86_64-linux-gnu
)

# Find include directories
FIND_PATH(TIDESDB_INCLUDE_DIR
  NAMES tidesdb.h
  HINTS
    ENV TIDESDB_ROOT
    ENV CPATH
  PATHS
    /usr/local/include
    /usr/include
    /opt/local/include
)

# Validate required dependencies
IF(NOT TIDESDB_LIB)
  MESSAGE(FATAL_ERROR "TidesDB library not found. Please install TidesDB or set TIDESDB_ROOT environment variable.")
ENDIF()

IF(NOT ZSTD_FOUND AND NOT ZSTD_LIBRARIES)
  MESSAGE(FATAL_ERROR "zstd library not found. Please install libzstd-dev.")
ENDIF()

IF(NOT LZ4_FOUND AND NOT LZ4_LIBRARIES)
  MESSAGE(FATAL_ERROR "lz4 library not found. Please install liblz4-dev.")
ENDIF()

# Add include directories
IF(TIDESDB_INCLUDE_DIR)
  INCLUDE_DIRECTORIES(${TIDESDB_INCLUDE_DIR})
ENDIF()
IF(ZSTD_INCLUDE_DIRS)
  INCLUDE_DIRECTORIES(${ZSTD_INCLUDE_DIRS})
ENDIF()
IF(LZ4_INCLUDE_DIRS)
  INCLUDE_DIRECTORIES(${LZ4_INCLUDE_DIRS})
ENDIF()

# Set source files
SET(TIDESDB_SOURCES ha_tidesdb.cc)

# Set libraries to link (used by MYSQL_STORAGE_ENGINE macro)
SET(TIDESDB_LIBS ${TIDESDB_LIB})

IF(ZSTD_LIBRARIES)
  LIST(APPEND TIDESDB_LIBS ${ZSTD_LIBRARIES})
ELSE()
  LIST(APPEND TIDESDB_LIBS zstd)
ENDIF()

IF(LZ4_LIBRARIES)
  LIST(APPEND TIDESDB_LIBS ${LZ4_LIBRARIES})
ELSE()
  LIST(APPEND TIDESDB_LIBS lz4)
ENDIF()

IF(SNAPPY_LIBRARIES)
  LIST(APPEND TIDESDB_LIBS ${SNAPPY_LIBRARIES})
ELSE()
  LIST(APPEND TIDESDB_LIBS snappy)
ENDIF()

# Define the storage engine (uses TIDESDB_SOURCES and TIDESDB_LIBS)
MYSQL_STORAGE_ENGINE(TIDESDB)
