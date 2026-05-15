#!/usr/bin/env bash
# 
# install.sh - Build & install MariaDB with InnoDB and TidesDB(TideSQL)
#
# Supported platforms: Linux (Debian/Ubuntu, RHEL/Fedora, Arch), macOS, Windows
# (MSYS2/Git Bash).
#
# Flow:
#   1. Detect OS (Debian, RHEL, Arch, macOS, Windows)
#   2. Install system dependencies
#        - Linux      apt/dnf/pacman (cmake, compilers, zstd, lz4, snappy, ssl, etc.)
#        - macOS      Homebrew (cmake, ninja, zstd, lz4, snappy, openssl, etc.)
#        - Windows    vcpkg (zstd, lz4, snappy, pthreads)
#   3. Build & install TidesDB library
#        - Clone tidesdb at the requested version tag
#        - cmake Release build
#        - Install to --tidesdb-prefix (default /usr/local or C:/tidesdb)
#        - Update shared library cache (ldconfig on Linux)
#   4. Clone MariaDB server source
#        - Checkout the requested branch/tag
#        - Init submodules
#        - Copy tidesdb/ storage engine plugin into storage/
#        - Copy tidesdb test suite into mysql-test/suite/
#   5. Build MariaDB (full server)
#        - All default storage engines (InnoDB, Aria, CONNECT, etc.)
#        - All standard tools (mariadb, mysqldump, mariadb-admin, etc.)
#        - mariabackup enabled
#        - TidesDB plugin built as a MODULE via the copied source
#        - cmake points at --tidesdb-prefix so FIND_LIBRARY resolves
#   6. Install MariaDB to --mariadb-prefix
#   7. Setup
#        - Create mysql system user (Unix only)
#        - Write production my.cnf / my.ini (InnoDB tuning, logging, utf8mb4,
#          TidesDB plugin_load_add, client/mysqldump/mariabackup sections)
#        - Run mariadb-install-db to initialize the data directory
#        - Set proper file ownership (Unix only)
#   8. Print summary with start/connect/test commands
#
# Usage:
#  ./install.sh [OPTIONS]
#
# Options:
#   --tidesdb-version VERSION   TidesDB release tag        (default: latest from GitHub)
#   --mariadb-version VERSION   MariaDB branch or tag      (default: latest from GitHub)
#   --tidesdb-prefix  DIR       TidesDB install prefix     (default: platform-dependent)
#   --mariadb-prefix  DIR       MariaDB install prefix     (default: platform-dependent)
#   --build-dir       DIR       Working directory          (default: platform-dependent)
#   --jobs            N         Parallel build jobs        (default: auto-detected)
#   --skip-deps                 Skip system dependency installation
#   --skip-tidesdb              Skip TidesDB library build (use if already installed)
#   --skip-engines  ENGINES     Comma-separated list of storage engines to skip
#   --list-engines              List storage engines that can be skipped and exit
#   --rebuild-plugin             Rebuild only the TidesDB plugin (fast dev cycle)
#   --pgo                       Enable Profile-Guided Optimization (3-phase build)
#   --s3                        Build TidesDB with S3 object store connector (requires libcurl + OpenSSL)
#   --allocator  NAME           Memory allocator for libtidesdb.so: system (default), jemalloc, mimalloc, or tcmalloc.
#                               Only affects TidesDB's internal allocations; mariadbd's allocator is unchanged.
#                               For a process-wide swap also LD_PRELOAD the allocator at mariadbd startup.
#                               Note: --rebuild-plugin does not rebuild libtidesdb, so changing this flag
#                               requires a full install run (omit --rebuild-plugin) to take effect.
#   --help                      Show this help message
#
# Platform defaults:
#   Linux / macOS:
#     tidesdb-prefix  = /usr/local
#     mariadb-prefix  = /usr/local/mariadb
#     build-dir       = /tmp/tidesql-build
#   Windows (MSYS2 / Git Bash):
#     tidesdb-prefix  = C:/tidesdb
#     mariadb-prefix  = C:/mariadb
#     build-dir       = C:/tidesql-build
#
# Examples:
#  ./install.sh
#  ./install.sh --tidesdb-version v8.6.1 --mariadb-version 12.1
#  ./install.sh --tidesdb-prefix /opt/tidesdb --mariadb-prefix /opt/mariadb
#  ./install.sh --mariadb-version mariadb-12.1.2
#  ./install.sh --skip-deps --skip-tidesdb
#  ./install.sh --pgo          # Full PGO build (instrument -> train -> optimize)
#  ./install.sh --list-engines # Show which engines can be skipped
#  ./install.sh --skip-engines mroonga,rocksdb,connect,spider,oqgraph,columnstore
# 
set -euo pipefail

# Resolve the tidesql repo root (where this script lives) 
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

detect_os() {
    case "$(uname -s)" in
        Linux*)
            if [[ -f /etc/os-release ]]; then
                . /etc/os-release
                case "$ID" in
                    ubuntu|debian|pop|linuxmint|raspbian) echo "debian"  ;;
                    fedora|rhel|centos|rocky|alma|ol|amzn|scientific) echo "redhat" ;;
                    arch|manjaro|endeavouros) echo "arch" ;;
                    *)
                        # Fallback: check ID_LIKE for parent distro family
                        case "${ID_LIKE:-}" in
                            *debian*|*ubuntu*) echo "debian"  ;;
                            *rhel*|*fedora*|*centos*) echo "redhat" ;;
                            *arch*) echo "arch" ;;
                            *) echo "linux-unknown" ;;
                        esac
                        ;;
                esac
            else
                echo "linux-unknown"
            fi
            ;;
        Darwin*)  echo "macos"   ;;
        MINGW*|MSYS*|CYGWIN*) echo "windows" ;;
        *)        echo "unknown" ;;
    esac
}

OS="$(detect_os)"

# Platform-dependent defaults 
if [[ "$OS" == "windows" ]]; then
    DEFAULT_TIDESDB_PREFIX="C:/tidesdb"
    DEFAULT_MARIADB_PREFIX="C:/mariadb"
    DEFAULT_BUILD_DIR="C:/tidesql-build"
else
    DEFAULT_TIDESDB_PREFIX="/usr/local"
    DEFAULT_MARIADB_PREFIX="/usr/local/mariadb"
    DEFAULT_BUILD_DIR="/tmp/tidesql-build"
fi

# Fetch latest release versions from GitHub 
# Works on Linux, macOS, and Windows (MSYS2/Git Bash) using curl or wget
_fetch_url() {
    local url="$1"
    if command -v curl &>/dev/null; then
        curl -fsSL "$url" 2>/dev/null
    elif command -v wget &>/dev/null; then
        wget -qO- "$url" 2>/dev/null
    else
        echo ""
    fi
}

get_latest_tidesdb_version() {
    local version
    version=$(_fetch_url "https://api.github.com/repos/tidesdb/tidesdb/releases/latest" \
        | grep '"tag_name":' | sed -E 's/.*"tag_name": *"([^"]+)".*/\1/')
    if [[ -z "$version" ]]; then
        echo "v8.6.1"  # fallback
    else
        echo "$version"
    fi
}

get_latest_mariadb_version() {
    local version
    version=$(_fetch_url "https://api.github.com/repos/MariaDB/server/releases/latest" \
        | grep '"tag_name":' | sed -E 's/.*"tag_name": *"([^"]+)".*/\1/')
    if [[ -z "$version" ]]; then
        echo "12.1"  # fallback
    else
        echo "$version"
    fi
}

TIDESDB_VERSION=""
MARIADB_VERSION=""
TIDESDB_PREFIX="${DEFAULT_TIDESDB_PREFIX}"
MARIADB_PREFIX="${DEFAULT_MARIADB_PREFIX}"
BUILD_DIR="${DEFAULT_BUILD_DIR}"
JOBS="$(nproc 2>/dev/null || sysctl -n hw.ncpu 2>/dev/null || echo 4)"
SKIP_DEPS=false
SKIP_TIDESDB=false
REBUILD_PLUGIN=false
PGO_ENABLED=false
SKIP_ENGINES=""
WITH_S3=false
# Memory allocator to build libtidesdb against.  One of:
#   system     glibc / platform default (no extra dep)
#   jemalloc   routes TidesDB allocations through jemalloc (pkg: libjemalloc-dev)
#   mimalloc   routes TidesDB allocations through mimalloc (pkg: libmimalloc-dev)
#   tcmalloc   routes TidesDB allocations through tcmalloc (pkg: libgoogle-perftools-dev)
# Note this affects only libtidesdb.so's internal allocations; mariadbd's own
# allocator is unchanged.  For a process-wide swap, combine with
# LD_PRELOAD=/usr/lib/x86_64-linux-gnu/libjemalloc.so.2 at mariadbd startup.
ALLOCATOR="system"

# Ensure VCPKG_ROOT is set on Windows (needed even with --skip-deps) 
if [[ "$OS" == "windows" ]]; then
    if [[ -z "${VCPKG_ROOT:-}" ]]; then
        if [[ -d "C:/vcpkg" ]]; then
            VCPKG_ROOT="C:/vcpkg"
        fi
    fi
    export VCPKG_ROOT="${VCPKG_ROOT:-}"
fi

# Skippable storage engines 
# These are MariaDB storage engines that can safely be disabled to save build
# time and reduce compiler warnings.  InnoDB, Aria, MyISAM, and CSV are NOT
# listed here because the server or mysql-test framework depends on them.
SKIPPABLE_ENGINES=(
    "archive:Archive storage engine (read-only row-format tables)"
    "blackhole:Blackhole engine (accepts writes, stores nothing)"
    "columnstore:MariaDB ColumnStore (columnar analytics engine)"
    "connect:CONNECT engine (access external data sources)"
    "example:Example storage engine (test/demo only)"
    "federated:Legacy Federated engine (MODULE_ONLY)"
    "federatedx:FederatedX engine (query remote MySQL/MariaDB tables)"
    "mroonga:Mroonga full-text search engine (requires Groonga)"
    "oqgraph:Open Query GRAPH engine (graph computation)"
    "rocksdb:MyRocks / RocksDB LSM-tree engine"
    "sequence:Sequence engine (virtual auto-increment sequences)"
    "sphinx:SphinxSE engine (Sphinx full-text search integration)"
    "spider:Spider engine (sharding / federation)"
)

# Color helpers 
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
CYAN='\033[0;36m'
NC='\033[0m'

info()  { echo -e "${CYAN}[INFO]${NC}  $*"; }
ok()    { echo -e "${GREEN}[OK]${NC}    $*"; }
warn()  { echo -e "${YELLOW}[WARN]${NC}  $*"; }
err()   { echo -e "${RED}[ERROR]${NC} $*" >&2; }
die()   { err "$@"; exit 1; }

# Parse arguments 
usage() {
    sed -n '2,/^# /p' "$0" | grep '^#' | sed 's/^# \?//'
    exit 0
}

list_engines() {
    echo ""
    echo -e "${CYAN}Skippable storage engines:${NC}"
    echo -e "${CYAN}${NC}"
    for entry in "${SKIPPABLE_ENGINES[@]}"; do
        local name="${entry%%:*}"
        local desc="${entry#*:}"
        printf "  ${GREEN}%-14s${NC} %s\n" "$name" "$desc"
    done
    echo ""
    echo -e "Usage: ${GREEN}--skip-engines mroonga,rocksdb,connect${NC}"
    echo ""
    exit 0
}

while [[ $# -gt 0 ]]; do
    case "$1" in
        --tidesdb-version)  TIDESDB_VERSION="$2";  shift 2 ;;
        --mariadb-version)  MARIADB_VERSION="$2";  shift 2 ;;
        --tidesdb-prefix)   TIDESDB_PREFIX="$2";   shift 2 ;;
        --mariadb-prefix)   MARIADB_PREFIX="$2";   shift 2 ;;
        --build-dir)        BUILD_DIR="$2";         shift 2 ;;
        --jobs)             JOBS="$2";              shift 2 ;;
        --skip-deps)        SKIP_DEPS=true;         shift   ;;
        --skip-tidesdb)     SKIP_TIDESDB=true;      shift   ;;
        --rebuild-plugin)   REBUILD_PLUGIN=true;    shift   ;;
        --skip-engines)     SKIP_ENGINES="$2";      shift 2 ;;
        --list-engines)     list_engines ;;
        --pgo)              PGO_ENABLED=true;       shift   ;;
        --s3)               WITH_S3=true;           shift   ;;
        --allocator)
            ALLOCATOR="$2"
            case "$ALLOCATOR" in
                system|jemalloc|mimalloc|tcmalloc) ;;
                *) die "--allocator must be one of system|jemalloc|mimalloc|tcmalloc (got '$ALLOCATOR')" ;;
            esac
            shift 2 ;;
        --help|-h)          usage ;;
        *) die "Unknown option: $1 (try --help)" ;;
    esac
done

# Resolve versions (fetch from GitHub if not specified) 
if [[ -z "$TIDESDB_VERSION" ]]; then
    info "Fetching latest TidesDB version from GitHub..."
    TIDESDB_VERSION="$(get_latest_tidesdb_version)"
fi
if [[ -z "$MARIADB_VERSION" ]]; then
    info "Fetching latest MariaDB version from GitHub..."
    MARIADB_VERSION="$(get_latest_mariadb_version)"
fi

# Auto-sizing box drawing 
# Usage:
# draw_box <border_color> <title> <array_varname>
# where <array_varname> is the name of a bash array holding the body lines.
# The box auto-sizes to fit the widest visible line (ANSI codes stripped).
_strip_ansi() { echo -e "$1" | sed 's/\x1b\[[0-9;]*m//g'; }

draw_box() {
    local color="$1" title="$2" arr_name="$3"
    local -n _lines="${arr_name}"

    # Measure the widest visible line (title + all body lines)
    local max_w=0 plain
    plain="$(_strip_ansi "$title")"
    (( ${#plain} > max_w )) && max_w=${#plain}
    for line in "${_lines[@]}"; do
        plain="$(_strip_ansi "$line")"
        (( ${#plain} > max_w )) && max_w=${#plain}
    done

    # Box inner width = max visible line + 2 (1 space padding each side)
    local W=$max_w
    local border
    border="$(printf '═%.0s' $(seq 1 $((W + 2))))"

    _box_row() {
        local text="$1"
        plain="$(_strip_ansi "$text")"
        local pad=$(( W - ${#plain} ))
        (( pad < 0 )) && pad=0
        printf "${color}║${NC} %b%*s ${color}║${NC}\n" "$text" "$pad" ""
    }

    echo ""
    echo -e "${color}╔${border}╗${NC}"
    _box_row "${color}${title}${NC}"
    echo -e "${color}╠${border}╣${NC}"
    for line in "${_lines[@]}"; do
        _box_row "$line"
    done
    echo -e "${color}╚${border}╝${NC}"
    echo ""
}

# Print configuration 
_cfg_lines=(
    "TidesDB version  : ${GREEN}${TIDESDB_VERSION}${NC}"
    "MariaDB version  : ${GREEN}${MARIADB_VERSION}${NC}"
    "TidesDB prefix   : ${GREEN}${TIDESDB_PREFIX}${NC}"
    "MariaDB prefix   : ${GREEN}${MARIADB_PREFIX}${NC}"
    "Build directory  : ${GREEN}${BUILD_DIR}${NC}"
    "Parallel jobs    : ${GREEN}${JOBS}${NC}"
    "Detected OS      : ${GREEN}${OS}${NC}"
    "PGO build        : ${GREEN}${PGO_ENABLED}${NC}"
    "Allocator        : ${GREEN}${ALLOCATOR}${NC}"
)
if [[ -n "$SKIP_ENGINES" ]]; then
    _cfg_lines+=("Skip engines     : ${YELLOW}${SKIP_ENGINES}${NC}")
fi
_cfg_lines+=("TideSQL repo     : ${GREEN}${SCRIPT_DIR}${NC}")

draw_box "${CYAN}" "TIDESQL Installer" _cfg_lines

# Privilege helper (sudo on Unix only when needed, direct on Windows) 
# Uses sudo only when the target prefix directory is not writable by the
# current user, avoiding root-owned files in user-writable prefixes.
_needs_sudo() {
    local dir="$1"
    # Walk up to find the first existing ancestor
    while [[ ! -d "$dir" ]]; do
        dir="$(dirname "$dir")"
    done
    [[ ! -w "$dir" ]]
}

run_privileged() {
    if [[ "$OS" == "windows" ]]; then
        "$@"
    elif _needs_sudo "${MARIADB_PREFIX}"; then
        sudo "$@"
    else
        "$@"
    fi
}

run_privileged_tidesdb() {
    if [[ "$OS" == "windows" ]]; then
        "$@"
    elif _needs_sudo "${TIDESDB_PREFIX}"; then
        sudo "$@"
    else
        "$@"
    fi
}

install_deps() {
    if $SKIP_DEPS; then
        warn "Skipping dependency installation (--skip-deps)"
        return
    fi

    info "Installing system dependencies for ${OS}..."

    # Per-OS package name for the selected allocator (empty for 'system').
    local allocator_pkg=""
    case "$OS:$ALLOCATOR" in
        debian:jemalloc)  allocator_pkg="libjemalloc-dev" ;;
        debian:mimalloc)  allocator_pkg="libmimalloc-dev" ;;
        debian:tcmalloc)  allocator_pkg="libgoogle-perftools-dev" ;;
        redhat:jemalloc)  allocator_pkg="jemalloc-devel" ;;
        redhat:mimalloc)  allocator_pkg="mimalloc-devel" ;;
        redhat:tcmalloc)  allocator_pkg="gperftools-devel" ;;
        arch:jemalloc)    allocator_pkg="jemalloc" ;;
        arch:mimalloc)    allocator_pkg="mimalloc" ;;
        arch:tcmalloc)    allocator_pkg="gperftools" ;;
        macos:jemalloc)   allocator_pkg="jemalloc" ;;
        macos:mimalloc)   allocator_pkg="mimalloc" ;;
        macos:tcmalloc)   allocator_pkg="gperftools" ;;
    esac
    [[ -n "$allocator_pkg" ]] && info "Allocator ${ALLOCATOR} -> installing ${allocator_pkg}"

    case "$OS" in
        debian)
            sudo apt-get update -qq
            sudo apt-get install -y -qq \
                build-essential cmake ninja-build bison flex \
                libzstd-dev liblz4-dev libsnappy-dev \
                libncurses-dev libssl-dev libxml2-dev \
                libevent-dev libcurl4-openssl-dev \
                pkg-config git gnutls-dev \
                ${allocator_pkg}
            ;;
        redhat)
            sudo dnf install -y \
                gcc gcc-c++ cmake ninja-build bison flex \
                libzstd-devel lz4-devel snappy-devel \
                ncurses-devel openssl-devel libxml2-devel \
                libevent-devel libcurl-devel \
                pkg-config git gnutls-devel \
                ${allocator_pkg}
            ;;
        arch)
            sudo pacman -Sy --noconfirm --needed \
                base-devel cmake ninja bison flex \
                zstd lz4 snappy \
                ncurses openssl libxml2 \
                libevent curl \
                pkg-config git gnutls \
                ${allocator_pkg}
            ;;
        macos)
            if ! command -v brew &>/dev/null; then
                die "Homebrew is required on macOS. Install from https://brew.sh"
            fi
            brew install cmake ninja bison flex \
                snappy lz4 zstd openssl@3 gnutls \
                ${allocator_pkg}
            ;;
        windows)
            if [[ -z "${VCPKG_ROOT:-}" ]]; then
                die "vcpkg not found. Set VCPKG_ROOT or install to C:/vcpkg.\n" \
                    "  git clone https://github.com/Microsoft/vcpkg.git C:/vcpkg\n" \
                    "  C:/vcpkg/bootstrap-vcpkg.bat"
            fi

            info "Installing vcpkg packages..."
            "${VCPKG_ROOT}/vcpkg.exe" install \
                zstd:x64-windows lz4:x64-windows \
                snappy:x64-windows pthreads:x64-windows

            if ! command -v cmake &>/dev/null; then
                die "CMake not found. Install via: choco install cmake"
            fi
            ;;
        linux-unknown)
            warn "Unrecognized Linux distribution."
            warn "Install manually: cmake, build-essential/gcc, libzstd-dev, liblz4-dev,"
            warn "  libsnappy-dev, libncurses-dev, libssl-dev, libxml2-dev, libevent-dev,"
            warn "  libcurl-dev, bison, flex, pkg-config, git, gnutls-dev"
            warn "Then re-run with --skip-deps"
            die "Cannot auto-install dependencies for this distribution"
            ;;
        *)
            die "Unsupported OS. Install dependencies manually and re-run with --skip-deps"
            ;;
    esac

    ok "Dependencies installed"
}

# Build and install TidesDB library 
build_tidesdb() {
    if $SKIP_TIDESDB; then
        warn "Skipping TidesDB build (--skip-tidesdb)"
        # Quick check that the library exists at the expected prefix
        local found=false
        for ext in so dylib a lib; do
            if ls "${TIDESDB_PREFIX}/lib/libtidesdb"*.${ext} &>/dev/null 2>&1; then
                found=true; break
            fi
        done
        if ! $found; then
            if [[ "$OS" != "windows" ]] && ldconfig -p 2>/dev/null | grep -q libtidesdb; then
                found=true
            fi
        fi
        if ! $found; then
            warn "libtidesdb not found at ${TIDESDB_PREFIX}/lib - MariaDB build may fail"
        fi
        return
    fi

    info "Building TidesDB ${TIDESDB_VERSION}..."

    local tidesdb_src="${BUILD_DIR}/tidesdb-lib"

    if [[ -d "${tidesdb_src}" ]]; then
        info "Removing previous TidesDB source..."
        rm -rf "${tidesdb_src}"
    fi

    git clone --depth 1 --branch "${TIDESDB_VERSION}" \
        https://github.com/tidesdb/tidesdb.git "${tidesdb_src}"

    local cmake_args=(
        -S "${tidesdb_src}"
        -B "${tidesdb_src}/build"
        -DCMAKE_BUILD_TYPE=Release
        -DCMAKE_INSTALL_PREFIX="${TIDESDB_PREFIX}"
        -DTIDESDB_BUILD_TESTS=OFF
        -DBUILD_SHARED_LIBS=ON
    )

    if $WITH_S3; then
        cmake_args+=(-DTIDESDB_WITH_S3=ON)
        info "S3 object store connector enabled"
    fi

    case "$ALLOCATOR" in
        jemalloc)
            cmake_args+=(-DTIDESDB_WITH_JEMALLOC=ON)
            info "Building libtidesdb with jemalloc"
            ;;
        mimalloc)
            cmake_args+=(-DTIDESDB_WITH_MIMALLOC=ON)
            info "Building libtidesdb with mimalloc"
            ;;
        tcmalloc)
            cmake_args+=(-DTIDESDB_WITH_TCMALLOC=ON)
            info "Building libtidesdb with tcmalloc"
            ;;
    esac

    case "$OS" in
        macos)
            local sdk_root
            sdk_root="$(xcrun --show-sdk-path 2>/dev/null || true)"
            [[ -n "$sdk_root" ]] && cmake_args+=(-DCMAKE_OSX_SYSROOT="${sdk_root}")
            ;;
        windows)
            cmake_args+=(
                -G "Visual Studio 17 2022" -A x64
                -DCMAKE_TOOLCHAIN_FILE="${VCPKG_ROOT}/scripts/buildsystems/vcpkg.cmake"
            )
            ;;
    esac

    cmake "${cmake_args[@]}"
    cmake --build "${tidesdb_src}/build" --config Release --parallel "${JOBS}"
    run_privileged_tidesdb cmake --install "${tidesdb_src}/build" --config Release

    # Show which allocator is actually linked so users can verify after a build.
    if [[ "$ALLOCATOR" != "system" ]]; then
        local installed_lib
        installed_lib="$(ls -1 "${TIDESDB_PREFIX}/lib/libtidesdb."{so,dylib} 2>/dev/null | head -1 || true)"
        if [[ -n "$installed_lib" ]] && command -v ldd &>/dev/null; then
            local allocator_linkage
            allocator_linkage="$(ldd "$installed_lib" 2>/dev/null | grep -Eo '(jemalloc|mimalloc|tcmalloc)[^ ]*' | head -1 || true)"
            if [[ -n "$allocator_linkage" ]]; then
                ok "libtidesdb is linked against ${allocator_linkage}"
            else
                warn "Requested allocator ${ALLOCATOR} but none of libjemalloc/libmimalloc/libtcmalloc appears in \`ldd ${installed_lib}\`."
                warn "  Check cmake output above for missing packages."
            fi
        fi
    fi

    # Update shared library cache on Linux
    case "$OS" in
        debian|redhat|arch|linux-unknown)
            sudo ldconfig
            ;;
    esac

    ok "TidesDB ${TIDESDB_VERSION} installed to ${TIDESDB_PREFIX}"
}

# Clone MariaDB and copy TidesDB storage engine 
prepare_mariadb() {
    info "Cloning MariaDB (branch/tag: ${MARIADB_VERSION})..."

    local mariadb_src="${BUILD_DIR}/mariadb-server"

    if [[ -d "${mariadb_src}" ]]; then
        info "Removing previous MariaDB source..."
        rm -rf "${mariadb_src}"
    fi

    git clone --depth 1 --branch "${MARIADB_VERSION}" \
        https://github.com/MariaDB/server.git "${mariadb_src}"

    info "Initializing MariaDB submodules..."
    (cd "${mariadb_src}" && git submodule update --init --recursive --force)

    info "Copying TidesDB storage engine plugin into MariaDB source..."
    cp -r "${SCRIPT_DIR}/tidesdb" "${mariadb_src}/storage/"

    info "Copying TidesDB test suite into MariaDB source..."
    cp -r "${SCRIPT_DIR}/mysql-test/suite/tidesdb" "${mariadb_src}/mysql-test/suite/"

    ok "MariaDB source prepared"
}

build_mariadb() {
    info "Building MariaDB with InnoDB + TidesDB..."

    local mariadb_src="${BUILD_DIR}/mariadb-server"
    local mariadb_build="${mariadb_src}/build"

    mkdir -p "${mariadb_build}"

    # Full-featured MariaDB build with TidesDB added
    local cmake_args=(
        -S "${mariadb_src}"
        -B "${mariadb_build}"
        -DCMAKE_INSTALL_PREFIX="${MARIADB_PREFIX}"
        -DCMAKE_BUILD_TYPE=RelWithDebInfo
    )

    # Hint so the TidesDB plugin's FIND_LIBRARY / FIND_PATH succeed
    # (CMakeLists.txt uses ENV TIDESDB_ROOT in HINTS)
    export TIDESDB_ROOT="${TIDESDB_PREFIX}"

    # Ensure full server with all standard features
    cmake_args+=(
        -DWITH_MARIABACKUP=ON
        -DWITH_UNIT_TESTS=OFF
    )

    # S3 object store connector for the plugin
    if $WITH_S3; then
        cmake_args+=(-DTIDESDB_WITH_S3=ON)
    fi

    # Disable skipped engines
    if [[ -n "$SKIP_ENGINES" ]]; then
        IFS=',' read -ra _engines <<< "$SKIP_ENGINES"
        for _eng in "${_engines[@]}"; do
            _eng="$(echo "$_eng" | tr -d ' ' | tr '[:lower:]' '[:upper:]')"
            cmake_args+=("-DPLUGIN_${_eng}=NO")
            info "Skipping storage engine: ${_eng}"
        done
    fi

    case "$OS" in
        macos)
            local sdk_root cc cxx
            sdk_root="$(xcrun --show-sdk-path 2>/dev/null || true)"
            cc="$(xcrun -find clang 2>/dev/null || true)"
            cxx="$(xcrun -find clang++ 2>/dev/null || true)"

            [[ -n "$cc" ]]       && cmake_args+=(-DCMAKE_C_COMPILER="${cc}")
            [[ -n "$cxx" ]]      && cmake_args+=(-DCMAKE_CXX_COMPILER="${cxx}")
            [[ -n "$sdk_root" ]] && cmake_args+=(-DCMAKE_OSX_SYSROOT="${sdk_root}")
            cmake_args+=(
                -DCMAKE_PREFIX_PATH="${TIDESDB_PREFIX}"
                "-DCMAKE_C_FLAGS=-Wno-nullability-completeness"
                "-DCMAKE_CXX_FLAGS=-Wno-nullability-completeness"
                -DWITH_SSL=bundled
                -DWITH_PCRE=bundled
                -G Ninja
            )
            ;;
        windows)
            cmake_args+=(
                -G "Visual Studio 17 2022" -A x64
                "-DCMAKE_PREFIX_PATH=${TIDESDB_PREFIX};${VCPKG_ROOT}/installed/x64-windows"
            )
            ;;
        *)
            cmake_args+=(-DCMAKE_PREFIX_PATH="${TIDESDB_PREFIX}")
            ;;
    esac

    cmake "${cmake_args[@]}"
    cmake --build "${mariadb_build}" --config RelWithDebInfo --parallel "${JOBS}"

    ok "MariaDB build complete"
}

install_mariadb() {
    info "Installing MariaDB to ${MARIADB_PREFIX}..."

    local mariadb_build="${BUILD_DIR}/mariadb-server/build"
    local build_config="RelWithDebInfo"
    if $PGO_ENABLED; then
        build_config="Release"
    fi

    run_privileged cmake --install "${mariadb_build}" --config "${build_config}"

    ok "MariaDB installed to ${MARIADB_PREFIX}"
}

# Initialize MariaDB data directory & enable plugins 
setup_mariadb() {
    local datadir="${MARIADB_PREFIX}/data"

    # Determine whether this is a system-level install (needs root/mysql user)
    # or a user-local install (run everything as current user).
    local use_mysql_user=false
    if [[ "$OS" != "windows" ]] && _needs_sudo "${MARIADB_PREFIX}"; then
        use_mysql_user=true
        # Ensure mysql user exists
        if ! id -u mysql &>/dev/null; then
            info "Creating mysql system user..."
            sudo useradd -r -s /bin/false -d "${datadir}" mysql 2>/dev/null || true
        fi
    fi

    local run_user
    if $use_mysql_user; then
        run_user="mysql"
    else
        run_user="$(id -un)"
    fi

    # Write config before init so mariadb-install-db can pick it up
    local cnf_file="${MARIADB_PREFIX}/my.cnf"
    if [[ "$OS" == "windows" ]]; then
        cnf_file="${MARIADB_PREFIX}/my.ini"
    fi

    if [[ ! -f "${cnf_file}" ]]; then
        info "Creating MariaDB configuration at ${cnf_file}..."

        local socket_line="" client_socket="" socket_path="" plugin_ext="so"
        local user_line=""
        if [[ "$OS" == "windows" ]]; then
            plugin_ext="dll"
        else
            socket_path="/tmp/mariadb.sock"
            socket_line="socket  = ${socket_path}"
            client_socket="socket = ${socket_path}"
            user_line="user    = ${run_user}"
        fi

        # When --allocator selects a non-system allocator, locate the
        # matching shared library so we can wire it into mariadbd_safe's
        # malloc-lib option. mariadbd_safe LD_PRELOADs malloc-lib before
        # forking mariadbd, which makes the allocator's TLS reservations
        # happen before the static TLS budget is exhausted, sidestepping
        # the "cannot allocate memory in static TLS block" failure when
        # the plugin pulls in libjemalloc/mimalloc/tcmalloc via dlopen.
        local mysqld_safe_section=""
        if [[ "$OS" != "windows" && "$ALLOCATOR" != "system" ]]; then
            local alloc_pattern alloc_lib=""
            case "$ALLOCATOR" in
                jemalloc) alloc_pattern='libjemalloc\.so\.2' ;;
                mimalloc) alloc_pattern='libmimalloc\.so(\.[0-9]+)?' ;;
                tcmalloc) alloc_pattern='libtcmalloc\.so(\.[0-9]+)?' ;;
            esac
            if [[ -n "$alloc_pattern" ]]; then
                alloc_lib=$(ldconfig -p 2>/dev/null \
                            | awk -v pat="$alloc_pattern" \
                                  '$1 ~ ("^"pat"$") {print $NF; exit}')
            fi
            if [[ -n "$alloc_lib" && -f "$alloc_lib" ]]; then
                mysqld_safe_section="
[mysqld_safe]
# Preload the ${ALLOCATOR} allocator before forking mariadbd so the
# plugin's transitive libjemalloc/mimalloc/tcmalloc load does not run
# out of static TLS slots.
malloc-lib = ${alloc_lib}"
            else
                warn "Allocator ${ALLOCATOR} requested but ${alloc_pattern} not found via ldconfig; mysqld_safe malloc-lib will not be set"
            fi
        fi

        local cnf_content
        cnf_content="[mysqld]
basedir = ${MARIADB_PREFIX}
datadir = ${datadir}
port    = 3306
${socket_line}
${user_line}
pid-file = ${datadir}/mariadb.pid
log-error = ${datadir}/mariadb.err

# Networking
bind-address = 127.0.0.1
max_connections = 151

# InnoDB
default_storage_engine = InnoDB
innodb_buffer_pool_size = 256M
innodb_log_file_size = 48M
innodb_flush_log_at_trx_commit = 1
innodb_file_per_table = ON

# Logging
slow_query_log = ON
slow_query_log_file = ${datadir}/slow.log
long_query_time = 2

# Character set
character-set-server = utf8mb4
collation-server = utf8mb4_general_ci

# TidesDB plugin - loaded at startup
plugin_maturity = gamma
plugin_load_add = ha_tidesdb.${plugin_ext}

# TidesDB settings (tune as needed)
tidesdb_flush_threads = 4
tidesdb_compaction_threads = 4
tidesdb_block_cache_size = 256M
tidesdb_max_open_sstables = 256
tidesdb_log_level = WARN
tidesdb_unified_memtable_write_buffer_size = 256M

[client]
port = 3306
${client_socket}
default-character-set = utf8mb4

[mysqldump]
quick
max_allowed_packet = 64M

[mariadb-backup]
# mariabackup settings (defaults are fine)
${mysqld_safe_section}"

        if [[ "$OS" == "windows" ]]; then
            mkdir -p "$(dirname "${cnf_file}")"
            echo "$cnf_content" > "${cnf_file}"
        else
            run_privileged mkdir -p "$(dirname "${cnf_file}")"
            if $use_mysql_user; then
                echo "$cnf_content" | sudo tee "${cnf_file}" > /dev/null
            else
                echo "$cnf_content" > "${cnf_file}"
            fi
        fi
        ok "Configuration written to ${cnf_file}"
    else
        warn "Config file already exists at ${cnf_file}, skipping"
    fi

    info "Initializing MariaDB data directory..."

    local install_db=""
    # The script CONFIGURE_FILE'd into the build tree always lives at
    # ${BUILD_DIR}/mariadb-server/build/scripts/, regardless of whether
    # cmake --install copied it onward. Check the install prefix first,
    # then fall back to the build tree so a partial install still
    # bootstraps cleanly.
    for candidate in \
        "${MARIADB_PREFIX}/scripts/mariadb-install-db" \
        "${MARIADB_PREFIX}/scripts/mysql_install_db" \
        "${MARIADB_PREFIX}/bin/mariadb-install-db" \
        "${MARIADB_PREFIX}/bin/mysql_install_db" \
        "${BUILD_DIR}/mariadb-server/build/scripts/mariadb-install-db" \
        "${BUILD_DIR}/mariadb-server/build/scripts/mysql_install_db"; do
        if [[ -f "$candidate" ]]; then
            install_db="$candidate"
            break
        fi
    done

    if [[ -z "$install_db" ]]; then
        # A silent warn here used to leave the user with a fully built
        # mariadbd against an empty data dir, which manifests as
        # "Table 'mysql.plugin' doesn't exist" at first startup. Fail
        # loudly instead so the operator sees the cause immediately.
        error "Could not find mariadb-install-db. Looked in:"
        error "  ${MARIADB_PREFIX}/{scripts,bin}/"
        error "  ${BUILD_DIR}/mariadb-server/build/scripts/"
        error "Cannot initialize the data directory. Aborting."
        exit 1
    elif [[ -d "${datadir}/mysql" ]]; then
        warn "Data directory already exists at ${datadir}, skipping initialization"
    else
        # mariadb-install-db spawns a temporary mariadbd to populate the
        # mysql.* system tables. We pass --no-defaults so that bootstrap
        # mariadbd does not read ${cnf_file}; that file already contains
        # `plugin_load_add = ha_tidesdb.so`, which would force the
        # bootstrap mariadbd to dlopen the plugin and pull in libjemalloc
        # via libtidesdb. With jemalloc not preloaded into the bootstrap
        # process, dlopen aborts with "cannot allocate memory in static
        # TLS block" and the data dir ends up empty. Bootstrap doesn't
        # need anything from my.cnf -- only basedir/datadir/user, which
        # we pass on the command line.
        local -a install_db_env=()
        if [[ "$ALLOCATOR" == "jemalloc" && "$OS" != "windows" ]]; then
            local jelib
            jelib=$(ldconfig -p 2>/dev/null \
                    | awk '/libjemalloc\.so\.2/ {print $NF; exit}')
            if [[ -n "$jelib" && -f "$jelib" ]]; then
                install_db_env+=("LD_PRELOAD=$jelib")
            fi
        fi

        if [[ "$OS" == "windows" ]]; then
            "${install_db}" \
                --no-defaults \
                --basedir="${MARIADB_PREFIX}" \
                --datadir="${datadir}" \
                --skip-test-db
        elif $use_mysql_user; then
            sudo env "${install_db_env[@]}" "${install_db}" \
                --no-defaults \
                --user=mysql \
                --basedir="${MARIADB_PREFIX}" \
                --datadir="${datadir}" \
                --skip-test-db
            sudo chown -R mysql:mysql "${datadir}"
        else
            env "${install_db_env[@]}" "${install_db}" \
                --no-defaults \
                --user="${run_user}" \
                --basedir="${MARIADB_PREFIX}" \
                --datadir="${datadir}" \
                --skip-test-db
        fi
        ok "Data directory initialized"
    fi

    # Set proper ownership on Unix (only needed for system-level installs)
    if $use_mysql_user && [[ -d "${datadir}" ]]; then
        sudo chown -R mysql:mysql "${datadir}"
    fi
}

print_summary() {
    local cnf_name="my.cnf"
    local start_cmd="${MARIADB_PREFIX}/bin/mariadbd-safe"
    local test_dir="${BUILD_DIR}/mariadb-server/build/mysql-test"

    # Use the correct connect user, root for system installs, current user otherwise
    local connect_user="root"
    if [[ "$OS" != "windows" ]] && ! _needs_sudo "${MARIADB_PREFIX}"; then
        connect_user="$(id -un)"
    fi
    local connect_cmd="${MARIADB_PREFIX}/bin/mariadb -u ${connect_user}"

    if [[ "$OS" == "windows" ]]; then
        cnf_name="my.ini"
        start_cmd="${MARIADB_PREFIX}/bin/mysqld.exe"
    fi

    local _summary_lines=(
        ""
        "TidesDB installed to : ${CYAN}${TIDESDB_PREFIX}${NC}"
        "MariaDB installed to : ${CYAN}${MARIADB_PREFIX}${NC}"
    )
    if $PGO_ENABLED; then
        _summary_lines+=("Build type           : ${CYAN}Release + PGO${NC}")
    fi
    if [[ "$ALLOCATOR" != "system" ]]; then
        _summary_lines+=(
            "Allocator            : ${CYAN}${ALLOCATOR}${NC}"
            ""
            "Verify the allocator is linked into libtidesdb:"
            "  ldd ${TIDESDB_PREFIX}/lib/libtidesdb.so | grep -E 'jemalloc|mimalloc|tcmalloc'"
            ""
            "For a process-wide allocator swap also LD_PRELOAD at startup:"
            "  LD_PRELOAD=\$(pkg-config --variable=libdir ${ALLOCATOR})/lib${ALLOCATOR}.so.2 \\"
            "    ${start_cmd} --defaults-file=${MARIADB_PREFIX}/${cnf_name} &"
        )
    fi
    _summary_lines+=(
        ""
        "Start MariaDB:"
        "  ${start_cmd} \\"
        "    --defaults-file=${MARIADB_PREFIX}/${cnf_name} &"
        ""
        "Connect:"
        "  ${connect_cmd}"
        ""
        "Verify TidesDB plugin:"
        "  SHOW PLUGINS;"
        "  -- or if not auto-loaded:"
        "  INSTALL SONAME 'ha_tidesdb';"
        ""
        "Quick test:"
        "  CREATE TABLE t (id INT PRIMARY KEY) ENGINE=TIDESDB;"
        "  INSERT INTO t VALUES (1), (2), (3);"
        "  SELECT * FROM t;"
        "  DROP TABLE t;"
        ""
        "Run TidesDB test suite:"
        "  cd ${test_dir}"
        "  perl mtr --suite=tidesdb --parallel=4"
        ""
        "Add to PATH (optional):"
        "  export PATH=\"${MARIADB_PREFIX}/bin:\$PATH\""
        ""
    )

    draw_box "${GREEN}" "Installation Complete!" _summary_lines
}

# Rebuild only the TidesDB plugin (fast dev cycle) 
rebuild_plugin() {
    local mariadb_src="${BUILD_DIR}/mariadb-server"
    local mariadb_build="${mariadb_src}/build"

    if [[ ! -d "${mariadb_build}" ]]; then
        die "MariaDB build directory not found at ${mariadb_build}.\n" \
            "  Run a full install first before using --rebuild-plugin."
    fi

    # Re-copy plugin source & test suite into the existing source tree
    info "Copying TidesDB plugin source into MariaDB source tree..."
    cp -r "${SCRIPT_DIR}/tidesdb" "${mariadb_src}/storage/"
    cp -r "${SCRIPT_DIR}/mysql-test/suite/tidesdb" "${mariadb_src}/mysql-test/suite/"

    # Point cmake at the TidesDB library
    export TIDESDB_ROOT="${TIDESDB_PREFIX}"

    # Sync the S3 setting with the current --s3 flag so cached builds
    # don't keep a stale TIDESDB_WITH_S3 value from a previous configure.
    if $WITH_S3; then
        cmake "${mariadb_build}" -DTIDESDB_WITH_S3=ON
    else
        cmake "${mariadb_build}" -DTIDESDB_WITH_S3=OFF
    fi

    # Build just the plugin target
    info "Building tidesdb plugin target (${JOBS} jobs)..."
    cmake --build "${mariadb_build}" --target tidesdb --parallel "${JOBS}"

    # Find the built .so/.dylib/.dll and copy it into the installed plugin dir
    local plugin_ext="so"
    [[ "$OS" == "macos" ]]   && plugin_ext="dylib"
    [[ "$OS" == "windows" ]] && plugin_ext="dll"

    local built_so
    built_so="$(find "${mariadb_build}" -name "ha_tidesdb.${plugin_ext}" -print -quit 2>/dev/null)"
    if [[ -z "$built_so" ]]; then
        die "Could not find ha_tidesdb.${plugin_ext} in ${mariadb_build}"
    fi

    local plugin_dir="${MARIADB_PREFIX}/lib/plugin"
    if [[ ! -d "$plugin_dir" ]]; then
        plugin_dir="${MARIADB_PREFIX}/lib64/plugin"
    fi
    if [[ ! -d "$plugin_dir" ]]; then
        die "Plugin directory not found at ${MARIADB_PREFIX}/lib/plugin or lib64/plugin"
    fi

    info "Installing ha_tidesdb.${plugin_ext} -> ${plugin_dir}/"
    run_privileged cp -f "${built_so}" "${plugin_dir}/"

    ok "Plugin rebuilt and installed"

    # Determine config file & socket for restart hint
    local cnf_file="${MARIADB_PREFIX}/my.cnf"
    [[ "$OS" == "windows" ]] && cnf_file="${MARIADB_PREFIX}/my.ini"

    local _rebuild_lines=(
        ""
        "Plugin rebuilt : ${CYAN}${plugin_dir}/ha_tidesdb.${plugin_ext}${NC}"
        ""
        "Restart MariaDB to pick up the new plugin:"
        "  ${MARIADB_PREFIX}/bin/mariadb-admin shutdown"
        "  ${MARIADB_PREFIX}/bin/mariadbd-safe \\"
        "    --defaults-file=${cnf_file} &"
        ""
        "Run TidesDB test suite:"
        "  cd ${mariadb_build}/mysql-test"
        "  perl mtr --suite=tidesdb --parallel=4"
        ""
    )

    draw_box "${GREEN}" "Plugin Rebuild Complete" _rebuild_lines
}

# PGO Phase 1 -- Instrument build 
pgo_instrument() {
    info "PGO Phase 1/3: Building MariaDB with profiling instrumentation..."

    local mariadb_src="${BUILD_DIR}/mariadb-server"
    local mariadb_build="${mariadb_src}/build"
    local profile_dir="${BUILD_DIR}/pgo-profiles"

    mkdir -p "${profile_dir}"
    # Clean previous build to ensure instrumentation flags apply everywhere
    rm -rf "${mariadb_build}"
    mkdir -p "${mariadb_build}"

    local profraw="${profile_dir}/default-%m.profraw"

    local cmake_args=(
        -S "${mariadb_src}"
        -B "${mariadb_build}"
        -DCMAKE_INSTALL_PREFIX="${MARIADB_PREFIX}"
        -DCMAKE_BUILD_TYPE=Release
        -DWITH_MARIABACKUP=ON
        -DWITH_UNIT_TESTS=OFF
    )

    export TIDESDB_ROOT="${TIDESDB_PREFIX}"

    # Disable skipped engines
    if [[ -n "$SKIP_ENGINES" ]]; then
        IFS=',' read -ra _engines <<< "$SKIP_ENGINES"
        for _eng in "${_engines[@]}"; do
            _eng="$(echo "$_eng" | tr -d ' ' | tr '[:lower:]' '[:upper:]')"
            cmake_args+=("-DPLUGIN_${_eng}=NO")
        done
    fi

    case "$OS" in
        macos)
            local sdk_root cc cxx
            sdk_root="$(xcrun --show-sdk-path 2>/dev/null || true)"
            cc="$(xcrun -find clang 2>/dev/null || true)"
            cxx="$(xcrun -find clang++ 2>/dev/null || true)"

            [[ -n "$cc" ]]       && cmake_args+=(-DCMAKE_C_COMPILER="${cc}")
            [[ -n "$cxx" ]]      && cmake_args+=(-DCMAKE_CXX_COMPILER="${cxx}")
            [[ -n "$sdk_root" ]] && cmake_args+=(-DCMAKE_OSX_SYSROOT="${sdk_root}")
            cmake_args+=(
                -DCMAKE_PREFIX_PATH="${TIDESDB_PREFIX}"
                "-DCMAKE_C_FLAGS=-fprofile-instr-generate=${profraw} -Wno-nullability-completeness"
                "-DCMAKE_CXX_FLAGS=-fprofile-instr-generate=${profraw} -Wno-nullability-completeness"
                "-DCMAKE_EXE_LINKER_FLAGS=-fprofile-instr-generate"
                "-DCMAKE_SHARED_LINKER_FLAGS=-fprofile-instr-generate"
                "-DCMAKE_MODULE_LINKER_FLAGS=-fprofile-instr-generate"
                -DWITH_SSL=bundled
                -DWITH_PCRE=bundled
                -G Ninja
            )
            ;;
        windows)
            cmake_args+=(
                -G "Visual Studio 17 2022" -A x64
                "-DCMAKE_PREFIX_PATH=${TIDESDB_PREFIX};${VCPKG_ROOT}/installed/x64-windows"
            )
            warn "PGO is not supported on Windows (MSVC); falling back to normal Release build"
            ;;
        *)
            # -Wno-error instrumentation unlocks new -Wmaybe-uninitialized /
            # -Wformat-overflow warnings in bundled third-party code (pcre2,
            # mroonga, etc.) that compile with -Werror on their own targets.
            cmake_args+=(
                -DCMAKE_PREFIX_PATH="${TIDESDB_PREFIX}"
                "-DCMAKE_C_FLAGS=-fprofile-generate=${profile_dir} -fprofile-update=atomic -Wno-error"
                "-DCMAKE_CXX_FLAGS=-fprofile-generate=${profile_dir} -fprofile-update=atomic -Wno-error"
                "-DCMAKE_EXE_LINKER_FLAGS=-fprofile-generate=${profile_dir}"
                "-DCMAKE_SHARED_LINKER_FLAGS=-fprofile-generate=${profile_dir}"
                "-DCMAKE_MODULE_LINKER_FLAGS=-fprofile-generate=${profile_dir}"
            )
            ;;
    esac

    cmake "${cmake_args[@]}"
    cmake --build "${mariadb_build}" --config Release --parallel "${JOBS}"

    ok "PGO Phase 1/3: Instrumented build complete"
}

# PGO Phase 2 -- Train - run MTR to generate profile data 
pgo_train() {
    info "PGO Phase 2/3: Running TidesDB test suite to generate profile data..."

    local mariadb_src="${BUILD_DIR}/mariadb-server"
    local mtr_dir="${mariadb_src}/build/mysql-test"

    if [[ ! -f "${mtr_dir}/mtr" ]]; then
        die "MTR not found at ${mtr_dir}/mtr - instrumented build may have failed"
    fi

    # Run the tidesdb test suite as the training workload
    info "Running: perl mtr --suite=tidesdb --parallel=${JOBS}"
    (
        cd "${mtr_dir}" && \
        perl mtr --suite=tidesdb --parallel="${JOBS}" --force --retry=0
    ) || warn "Some MTR tests may have failed during PGO training (non-fatal)"

    local profile_dir="${BUILD_DIR}/pgo-profiles"
    local profile_count=0

    if [[ "$OS" == "macos" ]]; then
        # Clang generates .profraw files; merge them into a single .profdata
        profile_count="$(find "${profile_dir}" -name '*.profraw' 2>/dev/null | wc -l)"
        if [[ "${profile_count}" -eq 0 ]]; then
            die "No profile data generated in ${profile_dir} - PGO training failed"
        fi
        info "Merging ${profile_count} .profraw files..."
        xcrun llvm-profdata merge -output="${profile_dir}/default.profdata" \
            "${profile_dir}"/*.profraw
    else
        # GCC generates .gcda files
        profile_count="$(find "${profile_dir}" -name '*.gcda' 2>/dev/null | wc -l)"
        if [[ "${profile_count}" -eq 0 ]]; then
            die "No profile data generated in ${profile_dir} - PGO training failed"
        fi
    fi

    ok "PGO Phase 2/3: Training complete (${profile_count} profile files generated)"
}

# PGO Phase 3 -- Optimized rebuild using profile data 
pgo_optimize() {
    info "PGO Phase 3/3: Rebuilding MariaDB with profile-guided optimizations..."

    local mariadb_src="${BUILD_DIR}/mariadb-server"
    local mariadb_build="${mariadb_src}/build"
    local profile_dir="${BUILD_DIR}/pgo-profiles"

    # Clean the build but keep the profile data
    rm -rf "${mariadb_build}"
    mkdir -p "${mariadb_build}"

    local profdata="${profile_dir}/default.profdata"

    local cmake_args=(
        -S "${mariadb_src}"
        -B "${mariadb_build}"
        -DCMAKE_INSTALL_PREFIX="${MARIADB_PREFIX}"
        -DCMAKE_BUILD_TYPE=Release
        -DWITH_MARIABACKUP=ON
        -DWITH_UNIT_TESTS=OFF
    )

    export TIDESDB_ROOT="${TIDESDB_PREFIX}"

    # Disable skipped engines
    if [[ -n "$SKIP_ENGINES" ]]; then
        IFS=',' read -ra _engines <<< "$SKIP_ENGINES"
        for _eng in "${_engines[@]}"; do
            _eng="$(echo "$_eng" | tr -d ' ' | tr '[:lower:]' '[:upper:]')"
            cmake_args+=("-DPLUGIN_${_eng}=NO")
        done
    fi

    case "$OS" in
        macos)
            local sdk_root cc cxx
            sdk_root="$(xcrun --show-sdk-path 2>/dev/null || true)"
            cc="$(xcrun -find clang 2>/dev/null || true)"
            cxx="$(xcrun -find clang++ 2>/dev/null || true)"

            [[ -n "$cc" ]]       && cmake_args+=(-DCMAKE_C_COMPILER="${cc}")
            [[ -n "$cxx" ]]      && cmake_args+=(-DCMAKE_CXX_COMPILER="${cxx}")
            [[ -n "$sdk_root" ]] && cmake_args+=(-DCMAKE_OSX_SYSROOT="${sdk_root}")
            cmake_args+=(
                -DCMAKE_PREFIX_PATH="${TIDESDB_PREFIX}"
                "-DCMAKE_C_FLAGS=-fprofile-instr-use=${profdata} -Wno-nullability-completeness"
                "-DCMAKE_CXX_FLAGS=-fprofile-instr-use=${profdata} -Wno-nullability-completeness"
                -DWITH_SSL=bundled
                -DWITH_PCRE=bundled
                -G Ninja
            )
            ;;
        windows)
            cmake_args+=(
                -G "Visual Studio 17 2022" -A x64
                "-DCMAKE_PREFIX_PATH=${TIDESDB_PREFIX};${VCPKG_ROOT}/installed/x64-windows"
            )
            ;;
        *)
            # Same -Wno-error reasoning as pgo_instrument, -fprofile-use can
            # still perturb warning output vs a vanilla Release build.
            cmake_args+=(
                -DCMAKE_PREFIX_PATH="${TIDESDB_PREFIX}"
                "-DCMAKE_C_FLAGS=-fprofile-use=${profile_dir} -fprofile-correction -Wno-error"
                "-DCMAKE_CXX_FLAGS=-fprofile-use=${profile_dir} -fprofile-correction -Wno-error"
                "-DCMAKE_EXE_LINKER_FLAGS=-fprofile-use=${profile_dir}"
                "-DCMAKE_SHARED_LINKER_FLAGS=-fprofile-use=${profile_dir}"
                "-DCMAKE_MODULE_LINKER_FLAGS=-fprofile-use=${profile_dir}"
            )
            ;;
    esac

    cmake "${cmake_args[@]}"
    cmake --build "${mariadb_build}" --config Release --parallel "${JOBS}"

    ok "PGO Phase 3/3: Optimized build complete"
}

main() {
    mkdir -p "${BUILD_DIR}"

    # Fast path is we rebuild only the plugin and exit
    if $REBUILD_PLUGIN; then
        # Allocator choice is baked into libtidesdb.so by build_tidesdb().  The
        # rebuild-plugin path only recompiles ha_tidesdb.so against the already
        # installed library, so --allocator has no effect here -- warn so users
        # don't silently get a stale allocator.
        if [[ "$ALLOCATOR" != "system" ]]; then
            warn "--allocator=${ALLOCATOR} has no effect with --rebuild-plugin."
            warn "  libtidesdb.so keeps its prior allocator linkage; to change"
            warn "  allocators, re-run install.sh without --rebuild-plugin."
        fi
        rebuild_plugin
        return
    fi

    install_deps
    build_tidesdb
    prepare_mariadb

    if $PGO_ENABLED; then
        info "PGO enabled -- performing 3-phase build (instrument ⤍ train ⤍ optimize)"
        pgo_instrument
        pgo_train
        pgo_optimize
    else
        build_mariadb
    fi

    install_mariadb
    setup_mariadb
    print_summary
}

main
