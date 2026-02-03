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
set(PROJECT_VERSION 1.2.0)

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

# Find compression libraries - always use FIND_LIBRARY to get full paths.
# We cannot use pkg-config because it returns library names like "zstd" which
# conflict with MariaDB's own "zstd" MODULE target (provider_zstd plugin).
FIND_LIBRARY(ZSTD_LIBRARY
  NAMES zstd
  HINTS ENV LIBRARY_PATH
  PATHS /usr/local/lib /usr/lib /usr/lib/x86_64-linux-gnu /opt/homebrew/lib
)

FIND_LIBRARY(LZ4_LIBRARY
  NAMES lz4
  HINTS ENV LIBRARY_PATH
  PATHS /usr/local/lib /usr/lib /usr/lib/x86_64-linux-gnu /opt/homebrew/lib
)

FIND_LIBRARY(SNAPPY_LIBRARY
  NAMES snappy
  HINTS ENV LIBRARY_PATH
  PATHS /usr/local/lib /usr/lib /usr/lib/x86_64-linux-gnu /opt/homebrew/lib
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
  MESSAGE(STATUS "TidesDB: library not found - plugin will not be built. Install TidesDB or set TIDESDB_ROOT.")
  RETURN()
ENDIF()

IF(NOT ZSTD_LIBRARY)
  MESSAGE(STATUS "TidesDB: zstd library not found - plugin will not be built. Install libzstd-dev.")
  RETURN()
ENDIF()

IF(NOT LZ4_LIBRARY)
  MESSAGE(STATUS "TidesDB: lz4 library not found - plugin will not be built. Install liblz4-dev.")
  RETURN()
ENDIF()

# Add include directories
IF(TIDESDB_INCLUDE_DIR)
  INCLUDE_DIRECTORIES(${TIDESDB_INCLUDE_DIR})
ENDIF()

# Build list of libraries to link - use full paths from FIND_LIBRARY
SET(TIDESDB_LIBS ${TIDESDB_LIB} ${ZSTD_LIBRARY} ${LZ4_LIBRARY})

IF(SNAPPY_LIBRARY)
  LIST(APPEND TIDESDB_LIBS ${SNAPPY_LIBRARY})
ENDIF()

# Define the storage engine using MariaDB's MYSQL_ADD_PLUGIN macro
MYSQL_ADD_PLUGIN(tidesdb ha_tidesdb.cc STORAGE_ENGINE MODULE_ONLY LINK_LIBRARIES ${TIDESDB_LIBS})
