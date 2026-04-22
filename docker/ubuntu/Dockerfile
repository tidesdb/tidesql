FROM ubuntu:24.04

# Build-time arguments 
# Override with --build-arg KEY=VALUE at docker build time.
ARG MARIADB_VERSION
ARG TIDESDB_VERSION
ARG TIDESDB_PREFIX=/usr/local
ARG MARIADB_PREFIX=/usr/local/mariadb
# Set WITH_TESTS=1 to include the MariaDB + TidesDB test suite in the image.
ARG WITH_TESTS=0
# Comma-separated list of optional storage engines to disable at compile time.
# Managed automatically by docker/rebuild.sh via EXCLUDE_ENGINES / INCLUDE_ENGINES.
ARG DISABLED_ENGINES=""
# Set WITH_S3=1 to build with S3-compatible object store support (requires libcurl + OpenSSL).
ARG WITH_S3=0
# Memory allocator for libtidesdb.so: system (default), jemalloc, mimalloc, or tcmalloc.
# Affects only TidesDB's internal allocations; mariadbd's allocator is unchanged.
# Combine with LD_PRELOAD at container startup for a process-wide swap.
ARG ALLOCATOR=system

# Persist key paths as environment variables so they are available in every
# RUN step and at container runtime.
ENV TIDESDB_PREFIX=${TIDESDB_PREFIX} \
    MARIADB_PREFIX=${MARIADB_PREFIX} \
    DEBIAN_FRONTEND=noninteractive
ENV PATH="${MARIADB_PREFIX}/bin:${PATH}"

# 1. Install system dependencies
RUN apt-get update -qq && \
    case "${ALLOCATOR}" in \
        jemalloc) ALLOC_PKG=libjemalloc-dev ;; \
        mimalloc) ALLOC_PKG=libmimalloc-dev ;; \
        tcmalloc) ALLOC_PKG=libgoogle-perftools-dev ;; \
        system|'') ALLOC_PKG= ;; \
        *) echo "ERROR: ALLOCATOR must be one of system|jemalloc|mimalloc|tcmalloc (got '${ALLOCATOR}')"; exit 1 ;; \
    esac && \
    apt-get install -y -qq \
        build-essential cmake ninja-build bison flex \
        libzstd-dev liblz4-dev libsnappy-dev \
        libncurses-dev libssl-dev libxml2-dev \
        libevent-dev libcurl4-openssl-dev \
        pkg-config git gnutls-dev \
        ${ALLOC_PKG} && \
    rm -rf /var/lib/apt/lists/*

# 2. Copy TidesDB storage engine plugin and test suite from this repo 
# The build context must be the repository root (see docker/README).
COPY tidesdb                        /opt/tidesql/tidesdb
COPY mysql-test/suite/tidesdb       /opt/tidesql/mysql-test/suite/tidesdb

# 3. Build and install TidesDB library
ARG WITH_S3=0
ARG ALLOCATOR=system
RUN TIDESDB_VER="${TIDESDB_VERSION}" && \
    if [ -z "$TIDESDB_VER" ]; then \
        TIDESDB_VER=$(curl -sf https://api.github.com/repos/tidesdb/tidesdb/releases/latest \
            | grep '"tag_name"' | head -1 | cut -d'"' -f4) || true; \
    fi && \
    if [ -z "$TIDESDB_VER" ]; then echo "ERROR: could not resolve TidesDB version"; exit 1; fi && \
    echo "Building TidesDB version: $TIDESDB_VER with allocator=${ALLOCATOR}" && \
    S3_FLAG=$([ "${WITH_S3}" = "1" ] && echo "-DTIDESDB_WITH_S3=ON" || true) && \
    case "${ALLOCATOR}" in \
        jemalloc) ALLOC_FLAG="-DTIDESDB_WITH_JEMALLOC=ON" ;; \
        mimalloc) ALLOC_FLAG="-DTIDESDB_WITH_MIMALLOC=ON" ;; \
        tcmalloc) ALLOC_FLAG="-DTIDESDB_WITH_TCMALLOC=ON" ;; \
        *)        ALLOC_FLAG="" ;; \
    esac && \
    git clone --depth 1 --branch "$TIDESDB_VER" \
        https://github.com/tidesdb/tidesdb.git /tmp/tidesql-build/tidesdb-lib && \
    cmake \
        -S /tmp/tidesql-build/tidesdb-lib \
        -B /tmp/tidesql-build/tidesdb-lib/build \
        -DCMAKE_BUILD_TYPE=Release \
        -DCMAKE_INSTALL_PREFIX="${TIDESDB_PREFIX}" \
        -DTIDESDB_BUILD_TESTS=OFF \
        -DBUILD_SHARED_LIBS=ON \
        ${S3_FLAG} \
        ${ALLOC_FLAG} && \
    cmake --build /tmp/tidesql-build/tidesdb-lib/build \
        --config Release --parallel "$(nproc)" && \
    cmake --install /tmp/tidesql-build/tidesdb-lib/build --config Release && \
    ldconfig && \
    if [ "${ALLOCATOR}" != "system" ]; then \
        LINKED=$(ldd "${TIDESDB_PREFIX}/lib/libtidesdb.so" 2>/dev/null | grep -Eo '(jemalloc|mimalloc|tcmalloc)[^ ]*' | head -1 || true); \
        if [ -n "$LINKED" ]; then \
            echo "[OK] libtidesdb linked against ${LINKED}"; \
        else \
            echo "[WARNING] ALLOCATOR=${ALLOCATOR} requested but no allocator library appears in ldd output"; \
        fi; \
    fi && \
    rm -rf /tmp/tidesql-build/tidesdb-lib

# 4. Clone MariaDB source and inject TidesDB storage engine plugin
RUN MARIADB_VER="${MARIADB_VERSION}" && \
    if [ -z "$MARIADB_VER" ]; then \
        MARIADB_VER=$(curl -sf https://api.github.com/repos/MariaDB/server/releases/latest \
            | grep '"tag_name"' | head -1 | cut -d'"' -f4) || true; \
    fi && \
    if [ -z "$MARIADB_VER" ]; then echo "ERROR: could not resolve MariaDB version"; exit 1; fi && \
    echo "Building MariaDB version: $MARIADB_VER" && \
    git clone --depth 1 --branch "$MARIADB_VER" \
        https://github.com/MariaDB/server.git /tmp/tidesql-build/mariadb-server && \
    cd /tmp/tidesql-build/mariadb-server && \
    git submodule update --init --recursive --force && \
    cp -r /opt/tidesql/tidesdb storage/ && \
    if [ "${WITH_TESTS}" = "1" ]; then \
        cp -r /opt/tidesql/mysql-test/suite/tidesdb mysql-test/suite/; \
    fi

# 5. Configure, build, and install MariaDB 
# Re-declare build ARGs so they are visible in this layer (Docker scoping rule).
ARG WITH_TESTS=0
ARG DISABLED_ENGINES=""
COPY docker/ubuntu/utils/cmake_exclude_engines.sh /usr/local/bin/cmake_exclude_engines.sh
RUN chmod +x /usr/local/bin/cmake_exclude_engines.sh
RUN export TIDESDB_ROOT="${TIDESDB_PREFIX}" && \
    TEST_DIR_FLAG=$([ "${WITH_TESTS:-0}" = "1" ] || echo "-DINSTALL_MYSQLTESTDIR=OFF") && \
    ENGINE_FLAGS=$(cmake_exclude_engines.sh "${DISABLED_ENGINES:-}") && \
    S3_FLAG=$([ "${WITH_S3}" = "1" ] && echo "-DTIDESDB_WITH_S3=ON" || true) && \
    cmake \
        -S /tmp/tidesql-build/mariadb-server \
        -B /tmp/tidesql-build/mariadb-server/build \
        -DCMAKE_INSTALL_PREFIX="${MARIADB_PREFIX}" \
        -DCMAKE_BUILD_TYPE=RelWithDebInfo \
        -DWITH_MARIABACKUP=ON \
        -DWITH_UNIT_TESTS=OFF \
        -DCMAKE_PREFIX_PATH="${TIDESDB_PREFIX}" \
        ${TEST_DIR_FLAG} \
        ${ENGINE_FLAGS} \
        ${S3_FLAG} && \
    cmake --build /tmp/tidesql-build/mariadb-server/build \
        --config RelWithDebInfo --parallel "$(nproc)" && \
    cmake --install /tmp/tidesql-build/mariadb-server/build \
        --config RelWithDebInfo && \
    if [ "${WITH_TESTS:-0}" != "1" ]; then \
        rm -rf "${MARIADB_PREFIX}/mysql-test" "${MARIADB_PREFIX}/mariadb-test"; \
    fi && \
    rm -rf /tmp/tidesql-build

# 6. Create mysql system user and prepare runtime directories 
RUN useradd -r -s /bin/false -d "${MARIADB_PREFIX}/data" mysql && \
    mkdir -p "${MARIADB_PREFIX}/data/default" \
             "${MARIADB_PREFIX}/log" && \
    chown -R mysql:mysql "${MARIADB_PREFIX}/data" \
                         "${MARIADB_PREFIX}/log"

# bind-address is set to 0.0.0.0 so the port is reachable from the Docker host.
COPY docker/conf/ /etc/mysql/
RUN chown -R mysql:mysql /etc/mysql

# 7. Install entrypoint and init-scripts directory 
COPY docker/ubuntu/entrypoint.sh /usr/local/bin/tidesql-entrypoint.sh
RUN chmod +x /usr/local/bin/tidesql-entrypoint.sh

COPY docker/initdb.d/ /docker-entrypoint-initdb.d/

VOLUME /etc/mysql
VOLUME /docker-entrypoint-initdb.d
VOLUME ${MARIADB_PREFIX}/data
VOLUME ${MARIADB_PREFIX}/log

EXPOSE 3306

ENTRYPOINT ["/usr/local/bin/tidesql-entrypoint.sh"]
