# Copyright (c) 2026 TidesDB Corp.
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

# Optional Plugin-level ASAN + UBSAN 
#
# You enable with cmake -DTIDESDB_WITH_ASAN=ON -DTIDESDB_WITH_UBSAN=ON ..
# This instruments only the TidesDB plugin .so, not the whole server, though its good to build the server with sanitizers too.
# At runtime, LD_PRELOAD the ASAN runtime for shared-lib mode:
#   export LD_PRELOAD=$(gcc -print-file-name=libasan.so)
#   export ASAN_OPTIONS=detect_leaks=0:abort_on_error=1
OPTION(TIDESDB_WITH_ASAN "Enable AddressSanitizer for TidesDB plugin" OFF)
OPTION(TIDESDB_WITH_UBSAN "Enable UndefinedBehaviorSanitizer for TidesDB plugin" OFF)

SET(TIDESDB_SANITIZER_COMPILE_FLAGS "")
SET(TIDESDB_SANITIZER_LINK_FLAGS "")
IF(TIDESDB_WITH_ASAN)
  LIST(APPEND TIDESDB_SANITIZER_COMPILE_FLAGS -fsanitize=address -fno-omit-frame-pointer)
  LIST(APPEND TIDESDB_SANITIZER_LINK_FLAGS -fsanitize=address)
ENDIF()
IF(TIDESDB_WITH_UBSAN)
  LIST(APPEND TIDESDB_SANITIZER_COMPILE_FLAGS -fsanitize=undefined -fno-sanitize=alignment)
  LIST(APPEND TIDESDB_SANITIZER_LINK_FLAGS -fsanitize=undefined)
ENDIF()

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
        /opt/homebrew/lib
        "C:/Program Files/tidesdb/lib"
        "C:/Program Files (x86)/tidesdb/lib"
        PATH_SUFFIXES lib
)

# We find compression libraries and always use FIND_LIBRARY to get full paths
# We cannot use pkg-config because it returns library names like "zstd" which 
# conflict with MariaDB's own "zstd" MODULE target (provider_zstd plugin).    
FIND_LIBRARY(ZSTD_LIBRARY
        NAMES zstd zstd_static
        HINTS ENV LIBRARY_PATH ENV VCPKG_INSTALLED_DIR
        PATHS
        /usr/local/lib
        /usr/lib
        /usr/lib/x86_64-linux-gnu
        /opt/homebrew/lib
        "C:/vcpkg/installed/x64-windows/lib"
        "$ENV{VCPKG_INSTALLATION_ROOT}/installed/x64-windows/lib"
)

FIND_LIBRARY(LZ4_LIBRARY
        NAMES lz4
        HINTS ENV LIBRARY_PATH ENV VCPKG_INSTALLED_DIR
        PATHS
        /usr/local/lib
        /usr/lib
        /usr/lib/x86_64-linux-gnu
        /opt/homebrew/lib
        "C:/vcpkg/installed/x64-windows/lib"
        "$ENV{VCPKG_INSTALLATION_ROOT}/installed/x64-windows/lib"
)

FIND_LIBRARY(SNAPPY_LIBRARY
        NAMES snappy
        HINTS ENV LIBRARY_PATH ENV VCPKG_INSTALLED_DIR
        PATHS
        /usr/local/lib
        /usr/lib
        /usr/lib/x86_64-linux-gnu
        /opt/homebrew/lib
        "C:/vcpkg/installed/x64-windows/lib"
        "$ENV{VCPKG_INSTALLATION_ROOT}/installed/x64-windows/lib"
)

# We find include directories
# and look for tidesdb/db.h to get the parent include directory
# The plugin primarily uses #include <tidesdb/db.h> so we need the parent of the tidesdb folder
FIND_PATH(TIDESDB_INCLUDE_DIR
        NAMES tidesdb/db.h
        HINTS
        ENV TIDESDB_ROOT
        ENV CPATH
        PATHS
        /usr/local/include
        /usr/include
        /opt/local/include
        /opt/homebrew/include
        "C:/tidesdb/include"
        "C:/Program Files/tidesdb/include"
        "C:/Program Files (x86)/tidesdb/include"
        PATH_SUFFIXES include
)

# We validate required dependencies
IF(NOT TIDESDB_LIB)
  MESSAGE(STATUS "TidesDB: library not found - plugin will not be built. Install TidesDB or set TIDESDB_ROOT.")
  RETURN()
ENDIF()

IF(NOT TIDESDB_INCLUDE_DIR)
  MESSAGE(STATUS "TidesDB: tidesdb/db.h header not found - plugin will not be built. Install TidesDB headers or set TIDESDB_ROOT.")
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


INCLUDE_DIRECTORIES(${TIDESDB_INCLUDE_DIR})

# We build list of libraries to link, we use full paths from FIND_LIBRARY 
SET(TIDESDB_LIBS ${TIDESDB_LIB} ${ZSTD_LIBRARY} ${LZ4_LIBRARY})

IF(SNAPPY_LIBRARY)
  LIST(APPEND TIDESDB_LIBS ${SNAPPY_LIBRARY})
ENDIF()

# On Windows, we need pthreads library 
IF(WIN32)
  FIND_LIBRARY(PTHREADS_LIBRARY
          NAMES pthreadVC3 pthreadVC2 pthread pthreads
          HINTS ENV LIBRARY_PATH ENV VCPKG_INSTALLED_DIR
          PATHS
          "C:/vcpkg/installed/x64-windows/lib"
          "$ENV{VCPKG_INSTALLATION_ROOT}/installed/x64-windows/lib"
  )
  IF(PTHREADS_LIBRARY)
    LIST(APPEND TIDESDB_LIBS ${PTHREADS_LIBRARY})
  ENDIF()
ENDIF()

# Optional S3 object store connector 
OPTION(TIDESDB_WITH_S3 "Enable S3-compatible object store connector" OFF)
IF(TIDESDB_WITH_S3)
  FIND_PACKAGE(CURL REQUIRED)
  FIND_PACKAGE(OpenSSL REQUIRED)
  LIST(APPEND TIDESDB_LIBS CURL::libcurl OpenSSL::SSL OpenSSL::Crypto)
  MESSAGE(STATUS "TidesDB: S3 object store connector enabled")
ENDIF()

# Define the storage engine using MariaDB's MYSQL_ADD_PLUGIN macro 
MYSQL_ADD_PLUGIN(tidesdb ha_tidesdb.cc STORAGE_ENGINE MODULE_ONLY LINK_LIBRARIES ${TIDESDB_LIBS})

# S3 compile definition for the plugin 
IF(TIDESDB_WITH_S3)
  IF(TARGET tidesdb)
    TARGET_COMPILE_DEFINITIONS(tidesdb PRIVATE TIDESDB_WITH_S3)
  ENDIF()
ENDIF()

# We apply sanitizer flags to the plugin target only
IF(TIDESDB_SANITIZER_COMPILE_FLAGS)
  IF(TARGET tidesdb)
    TARGET_COMPILE_OPTIONS(tidesdb PRIVATE ${TIDESDB_SANITIZER_COMPILE_FLAGS})
    TARGET_LINK_OPTIONS(tidesdb PRIVATE ${TIDESDB_SANITIZER_LINK_FLAGS})
    MESSAGE(STATUS "TidesDB: Sanitizer compile flags: ${TIDESDB_SANITIZER_COMPILE_FLAGS}")
    MESSAGE(STATUS "TidesDB: Sanitizer link flags: ${TIDESDB_SANITIZER_LINK_FLAGS}")
  ENDIF()
ENDIF()

# Plugin-only -Wmaybe-uninitialized.  GCC reports may-uninit reads with
# considerably less noise than clang's -Wsometimes-uninitialized and is
# the canonical signal we lean on to catch uninitialized SHOW_FUNC
# fillers and missed branch initialization.  We always enable the
# warning on GCC; CI also flips TIDESDB_WERROR_UNINIT=ON so the warning
# is a hard error rather than something easy to overlook in a long
# build log.
OPTION(TIDESDB_WERROR_UNINIT "Promote -Wmaybe-uninitialized to a hard error in the plugin" OFF)
IF(TARGET tidesdb AND CMAKE_CXX_COMPILER_ID STREQUAL "GNU")
  TARGET_COMPILE_OPTIONS(tidesdb PRIVATE -Wmaybe-uninitialized)
  IF(TIDESDB_WERROR_UNINIT)
    TARGET_COMPILE_OPTIONS(tidesdb PRIVATE -Werror=maybe-uninitialized)
    MESSAGE(STATUS "TidesDB: -Werror=maybe-uninitialized enabled for plugin")
  ELSE()
    MESSAGE(STATUS "TidesDB: -Wmaybe-uninitialized enabled for plugin (warning only)")
  ENDIF()
ENDIF()
