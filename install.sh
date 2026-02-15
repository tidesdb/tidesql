#!/usr/bin/env bash
# ─────────────────────────────────────────────────────────────────────────────
# install.sh - Build & install MariaDB with InnoDB + TidesDB(TideSQL)
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
#   ./install.sh [OPTIONS]
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
#   --pgo                       Enable Profile-Guided Optimization (3-phase build)
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
#   ./install.sh
#   ./install.sh --tidesdb-version v8.4.0 --mariadb-version 12.1
#   ./install.sh --tidesdb-prefix /opt/tidesdb --mariadb-prefix /opt/mariadb
#   ./install.sh --mariadb-version mariadb-12.1.2
#   ./install.sh --skip-deps --skip-tidesdb
#   ./install.sh --pgo          # Full PGO build (instrument → train → optimize)
# ─────────────────────────────────────────────────────────────────────────────
set -euo pipefail

# ── Resolve the tidesql repo root (where this script lives) ────────────────
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

# ── Platform-dependent defaults ─────────────────────────────────────────────
if [[ "$OS" == "windows" ]]; then
    DEFAULT_TIDESDB_PREFIX="C:/tidesdb"
    DEFAULT_MARIADB_PREFIX="C:/mariadb"
    DEFAULT_BUILD_DIR="C:/tidesql-build"
else
    DEFAULT_TIDESDB_PREFIX="/usr/local"
    DEFAULT_MARIADB_PREFIX="/usr/local/mariadb"
    DEFAULT_BUILD_DIR="/tmp/tidesql-build"
fi

# ── Fetch latest release versions from GitHub ────────────────────────────────
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
        echo "v8.3.2"  # fallback
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
PGO_ENABLED=false

# ── Color helpers ───────────────────────────────────────────────────────────
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

# ── Parse arguments ─────────────────────────────────────────────────────────
usage() {
    sed -n '2,/^# ──────/p' "$0" | grep '^#' | sed 's/^# \?//'
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
        --pgo)              PGO_ENABLED=true;       shift   ;;
        --help|-h)          usage ;;
        *) die "Unknown option: $1 (try --help)" ;;
    esac
done

# ── Resolve versions (fetch from GitHub if not specified) ────────────────────
if [[ -z "$TIDESDB_VERSION" ]]; then
    info "Fetching latest TidesDB version from GitHub..."
    TIDESDB_VERSION="$(get_latest_tidesdb_version)"
fi
if [[ -z "$MARIADB_VERSION" ]]; then
    info "Fetching latest MariaDB version from GitHub..."
    MARIADB_VERSION="$(get_latest_mariadb_version)"
fi

# ── Print configuration ────────────────────────────────────────────────────
_cfg_row() {
    local text="$1"
    local plain
    plain="$(echo -e "$text" | sed 's/\x1b\[[0-9;]*m//g')"
    local len=${#plain}
    local pad=$((66 - len))
    if (( pad < 0 )); then pad=0; fi
    printf "${CYAN}║${NC} %b%*s ${CYAN}║${NC}\n" "$text" "$pad" ""
}

echo ""
echo -e "${CYAN}╔$(printf '═%.0s' $(seq 1 68))╗${NC}"
_cfg_row "${CYAN}TIDESQL Installer${NC}"
echo -e "${CYAN}╠$(printf '═%.0s' $(seq 1 68))╣${NC}"
_cfg_row "TidesDB version  : ${GREEN}${TIDESDB_VERSION}${NC}"
_cfg_row "MariaDB version  : ${GREEN}${MARIADB_VERSION}${NC}"
_cfg_row "TidesDB prefix   : ${GREEN}${TIDESDB_PREFIX}${NC}"
_cfg_row "MariaDB prefix   : ${GREEN}${MARIADB_PREFIX}${NC}"
_cfg_row "Build directory  : ${GREEN}${BUILD_DIR}${NC}"
_cfg_row "Parallel jobs    : ${GREEN}${JOBS}${NC}"
_cfg_row "Detected OS      : ${GREEN}${OS}${NC}"
_cfg_row "PGO build        : ${GREEN}${PGO_ENABLED}${NC}"
_cfg_row "TideSQL repo     : ${GREEN}${SCRIPT_DIR}${NC}"
echo -e "${CYAN}╚$(printf '═%.0s' $(seq 1 68))╝${NC}"
echo ""

# ── Privilege helper (sudo on Unix, direct on Windows) ──────────────────────
run_privileged() {
    if [[ "$OS" == "windows" ]]; then
        "$@"
    else
        sudo "$@"
    fi
}

install_deps() {
    if $SKIP_DEPS; then
        warn "Skipping dependency installation (--skip-deps)"
        return
    fi

    info "Installing system dependencies for ${OS}..."

    case "$OS" in
        debian)
            sudo apt-get update -qq
            sudo apt-get install -y -qq \
                build-essential cmake ninja-build bison flex \
                libzstd-dev liblz4-dev libsnappy-dev \
                libncurses-dev libssl-dev libxml2-dev \
                libevent-dev libcurl4-openssl-dev \
                pkg-config git gnutls-dev
            ;;
        redhat)
            sudo dnf install -y \
                gcc gcc-c++ cmake ninja-build bison flex \
                libzstd-devel lz4-devel snappy-devel \
                ncurses-devel openssl-devel libxml2-devel \
                libevent-devel libcurl-devel \
                pkg-config git gnutls-devel
            ;;
        arch)
            sudo pacman -Sy --noconfirm --needed \
                base-devel cmake ninja bison flex \
                zstd lz4 snappy \
                ncurses openssl libxml2 \
                libevent curl \
                pkg-config git gnutls
            ;;
        macos)
            if ! command -v brew &>/dev/null; then
                die "Homebrew is required on macOS. Install from https://brew.sh"
            fi
            brew install cmake ninja bison flex \
                snappy lz4 zstd openssl@3 gnutls
            ;;
        windows)
            if [[ -z "${VCPKG_ROOT:-}" ]]; then
                if [[ -d "C:/vcpkg" ]]; then
                    VCPKG_ROOT="C:/vcpkg"
                else
                    die "vcpkg not found. Set VCPKG_ROOT or install to C:/vcpkg.\n" \
                        "  git clone https://github.com/Microsoft/vcpkg.git C:/vcpkg\n" \
                        "  C:/vcpkg/bootstrap-vcpkg.bat"
                fi
            fi
            export VCPKG_ROOT

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

# ── Build and install TidesDB library ──────────────────────────────
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
            warn "libtidesdb not found at ${TIDESDB_PREFIX}/lib — MariaDB build may fail"
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
    )

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
    run_privileged cmake --install "${tidesdb_src}/build" --config Release

    # Update shared library cache on Linux
    case "$OS" in
        debian|redhat|arch|linux-unknown)
            sudo ldconfig
            ;;
    esac

    ok "TidesDB ${TIDESDB_VERSION} installed to ${TIDESDB_PREFIX}"
}

# ── Clone MariaDB and copy TidesDB storage engine ──────────────────
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
    (cd "${mariadb_src}" && git submodule update --init --recursive)

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
        -DCMAKE_PREFIX_PATH="${TIDESDB_PREFIX}"
    )

    # Hint so the TidesDB plugin's FIND_LIBRARY / FIND_PATH succeed
    cmake_args+=(-DTIDESDB_ROOT="${TIDESDB_PREFIX}")
    export TIDESDB_ROOT="${TIDESDB_PREFIX}"

    # Ensure full server with all standard features
    cmake_args+=(
        -DWITH_MARIABACKUP=ON
        -DWITH_UNIT_TESTS=OFF
    )

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

# ── Initialize MariaDB data directory & enable plugins ─────────────
setup_mariadb() {
    local datadir="${MARIADB_PREFIX}/data"

    # Ensure mysql user exists on Unix
    if [[ "$OS" != "windows" ]]; then
        if ! id -u mysql &>/dev/null; then
            info "Creating mysql system user..."
            sudo useradd -r -s /bin/false -d "${datadir}" mysql 2>/dev/null || true
        fi
    fi

    # Write config before init so mariadb-install-db can pick it up
    local cnf_file="${MARIADB_PREFIX}/my.cnf"
    if [[ "$OS" == "windows" ]]; then
        cnf_file="${MARIADB_PREFIX}/my.ini"
    fi

    if [[ ! -f "${cnf_file}" ]]; then
        info "Creating MariaDB configuration at ${cnf_file}..."

        local socket_line="" client_socket="" socket_path=""
        if [[ "$OS" != "windows" ]]; then
            socket_path="/tmp/mariadb.sock"
            socket_line="socket  = ${socket_path}"
            client_socket="socket = ${socket_path}"
        fi

        local cnf_content
        cnf_content="[mysqld]
basedir = ${MARIADB_PREFIX}
datadir = ${datadir}
port    = 3306
${socket_line}
pid-file = ${datadir}/mariadb.pid
log-error = ${datadir}/mariadb.err

# Networking
bind-address = 127.0.0.1
max_connections = 151

# InnoDB
default_storage_engine = InnoDB
innodb_buffer_pool_size = 128M
innodb_log_file_size = 48M
innodb_flush_log_at_trx_commit = 1
innodb_file_per_table = ON

# Query cache (disabled by default in modern MariaDB, kept explicit)
query_cache_type = 0
query_cache_size = 0

# Logging
slow_query_log = ON
slow_query_log_file = ${datadir}/slow.log
long_query_time = 2

# Character set
character-set-server = utf8mb4
collation-server = utf8mb4_general_ci

# TidesDB plugin — loaded at startup
plugin_load_add = ha_tidesdb

# TidesDB settings (tune as needed)
# tidesdb_flush_threads = 2
# tidesdb_compaction_threads = 2
# tidesdb_block_cache_size = 268435456
# tidesdb_log_level = WARN

[client]
port = 3306
${client_socket}
default-character-set = utf8mb4

[mysqldump]
quick
max_allowed_packet = 64M

[mariadb-backup]
# mariabackup settings (defaults are fine)"

        if [[ "$OS" == "windows" ]]; then
            mkdir -p "$(dirname "${cnf_file}")"
            echo "$cnf_content" > "${cnf_file}"
        else
            sudo mkdir -p "$(dirname "${cnf_file}")"
            echo "$cnf_content" | sudo tee "${cnf_file}" > /dev/null
        fi
        ok "Configuration written to ${cnf_file}"
    else
        warn "Config file already exists at ${cnf_file}, skipping"
    fi

    info "Initializing MariaDB data directory..."

    local install_db=""
    for candidate in \
        "${MARIADB_PREFIX}/scripts/mariadb-install-db" \
        "${MARIADB_PREFIX}/scripts/mysql_install_db" \
        "${MARIADB_PREFIX}/bin/mariadb-install-db" \
        "${MARIADB_PREFIX}/bin/mysql_install_db"; do
        if [[ -f "$candidate" ]]; then
            install_db="$candidate"
            break
        fi
    done

    if [[ -z "$install_db" ]]; then
        warn "Could not find mariadb-install-db — skipping data directory init"
        warn "You may need to run it manually after installation"
    elif [[ -d "${datadir}/mysql" ]]; then
        warn "Data directory already exists at ${datadir}, skipping initialization"
    else
        if [[ "$OS" == "windows" ]]; then
            "${install_db}" \
                --basedir="${MARIADB_PREFIX}" \
                --datadir="${datadir}" 2>&1 || true
        else
            sudo "${install_db}" \
                --user=mysql \
                --basedir="${MARIADB_PREFIX}" \
                --datadir="${datadir}" 2>&1 || true

            sudo chown -R mysql:mysql "${datadir}"
        fi
        ok "Data directory initialized"
    fi

    # Set proper ownership on Unix
    if [[ "$OS" != "windows" && -d "${datadir}" ]]; then
        sudo chown -R mysql:mysql "${datadir}"
    fi
}

print_summary() {
    local cnf_name="my.cnf"
    local start_cmd="${MARIADB_PREFIX}/bin/mariadbd-safe"
    local connect_cmd="${MARIADB_PREFIX}/bin/mariadb -u root"
    local test_dir="${BUILD_DIR}/mariadb-server/build/mysql-test"

    if [[ "$OS" == "windows" ]]; then
        cnf_name="my.ini"
        start_cmd="${MARIADB_PREFIX}/bin/mysqld.exe"
    fi

    local W=66  # inner width between ║ and ║

    # Print a row, colored ║, padded content, colored ║
    row() {
        local text="$1"
        local plain
        # Strip ANSI escape sequences to measure visible length
        plain="$(echo -e "$text" | sed 's/\x1b\[[0-9;]*m//g')"
        local len=${#plain}
        local pad=$((W - len))
        if (( pad < 0 )); then pad=0; fi
        printf "${GREEN}║${NC} %b%*s ${GREEN}║${NC}\n" "$text" "$pad" ""
    }

    echo ""
    echo -e "${GREEN}╔$(printf '═%.0s' $(seq 1 $((W + 2))))╗${NC}"
    row "${GREEN}Installation Complete!${NC}"
    echo -e "${GREEN}╠$(printf '═%.0s' $(seq 1 $((W + 2))))╣${NC}"
    row ""
    row "TidesDB installed to : ${CYAN}${TIDESDB_PREFIX}${NC}"
    row "MariaDB installed to : ${CYAN}${MARIADB_PREFIX}${NC}"
    if $PGO_ENABLED; then
        row "Build type           : ${CYAN}Release + PGO${NC}"
    fi
    row ""
    row "Start MariaDB:"
    row "  ${start_cmd} \\"
    row "    --defaults-file=${MARIADB_PREFIX}/${cnf_name} &"
    row ""
    row "Connect:"
    row "  ${connect_cmd}"
    row ""
    row "Verify TidesDB plugin:"
    row "  SHOW PLUGINS;"
    row "  -- or if not auto-loaded:"
    row "  INSTALL SONAME 'ha_tidesdb';"
    row ""
    row "Quick test:"
    row "  CREATE TABLE t (id INT PRIMARY KEY) ENGINE=TIDESDB;"
    row "  INSERT INTO t VALUES (1), (2), (3);"
    row "  SELECT * FROM t;"
    row "  DROP TABLE t;"
    row ""
    row "Run TidesDB test suite:"
    row "  cd ${test_dir}"
    row "  perl mtr --suite=tidesdb --parallel=4"
    row ""
    row "Add to PATH (optional):"
    row "  export PATH=\"${MARIADB_PREFIX}/bin:\$PATH\""
    row ""
    echo -e "${GREEN}╚$(printf '═%.0s' $(seq 1 $((W + 2))))╝${NC}"
    echo ""
}

# ── PGO Phase 1 —— Instrument build ─────────────────────────────────
pgo_instrument() {
    info "PGO Phase 1/3: Building MariaDB with profiling instrumentation..."

    local mariadb_src="${BUILD_DIR}/mariadb-server"
    local mariadb_build="${mariadb_src}/build"
    local profile_dir="${BUILD_DIR}/pgo-profiles"

    mkdir -p "${profile_dir}"
    # Clean previous build to ensure instrumentation flags apply everywhere
    rm -rf "${mariadb_build}"
    mkdir -p "${mariadb_build}"

    local cmake_args=(
        -S "${mariadb_src}"
        -B "${mariadb_build}"
        -DCMAKE_INSTALL_PREFIX="${MARIADB_PREFIX}"
        -DCMAKE_BUILD_TYPE=Release
        -DCMAKE_PREFIX_PATH="${TIDESDB_PREFIX}"
        -DTIDESDB_ROOT="${TIDESDB_PREFIX}"
        -DWITH_MARIABACKUP=ON
        -DWITH_UNIT_TESTS=OFF
        "-DCMAKE_C_FLAGS=-fprofile-generate=${profile_dir} -fprofile-update=atomic"
        "-DCMAKE_CXX_FLAGS=-fprofile-generate=${profile_dir} -fprofile-update=atomic"
        "-DCMAKE_EXE_LINKER_FLAGS=-fprofile-generate=${profile_dir}"
        "-DCMAKE_SHARED_LINKER_FLAGS=-fprofile-generate=${profile_dir}"
        "-DCMAKE_MODULE_LINKER_FLAGS=-fprofile-generate=${profile_dir}"
    )

    export TIDESDB_ROOT="${TIDESDB_PREFIX}"

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
                -DWITH_SSL=bundled
                -DWITH_PCRE=bundled
                -G Ninja
            )
            ;;
    esac

    cmake "${cmake_args[@]}"
    cmake --build "${mariadb_build}" --config Release --parallel "${JOBS}"

    ok "PGO Phase 1/3: Instrumented build complete"
}

# ── PGO Phase 2 —— Train — run MTR to generate profile data ────────
pgo_train() {
    info "PGO Phase 2/3: Running TidesDB test suite to generate profile data..."

    local mariadb_src="${BUILD_DIR}/mariadb-server"
    local mtr_dir="${mariadb_src}/build/mysql-test"

    if [[ ! -f "${mtr_dir}/mtr" ]]; then
        die "MTR not found at ${mtr_dir}/mtr — instrumented build may have failed"
    fi

    # Run the tidesdb test suite as the training workload
    info "Running: perl mtr --suite=tidesdb --parallel=${JOBS}"
    (
        cd "${mtr_dir}" && \
        perl mtr --suite=tidesdb --parallel="${JOBS}" --force --retry=0
    ) || warn "Some MTR tests may have failed during PGO training (non-fatal)"

    local profile_dir="${BUILD_DIR}/pgo-profiles"
    local profile_count
    profile_count="$(find "${profile_dir}" -name '*.gcda' 2>/dev/null | wc -l)"

    if [[ "${profile_count}" -eq 0 ]]; then
        die "No profile data generated in ${profile_dir} — PGO training failed"
    fi

    ok "PGO Phase 2/3: Training complete (${profile_count} profile files generated)"
}

# ── PGO Phase 3 —— Optimized rebuild using profile data ─────────────
pgo_optimize() {
    info "PGO Phase 3/3: Rebuilding MariaDB with profile-guided optimizations..."

    local mariadb_src="${BUILD_DIR}/mariadb-server"
    local mariadb_build="${mariadb_src}/build"
    local profile_dir="${BUILD_DIR}/pgo-profiles"

    # Clean the build but keep the profile data
    rm -rf "${mariadb_build}"
    mkdir -p "${mariadb_build}"

    local cmake_args=(
        -S "${mariadb_src}"
        -B "${mariadb_build}"
        -DCMAKE_INSTALL_PREFIX="${MARIADB_PREFIX}"
        -DCMAKE_BUILD_TYPE=Release
        -DCMAKE_PREFIX_PATH="${TIDESDB_PREFIX}"
        -DTIDESDB_ROOT="${TIDESDB_PREFIX}"
        -DWITH_MARIABACKUP=ON
        -DWITH_UNIT_TESTS=OFF
        "-DCMAKE_C_FLAGS=-fprofile-use=${profile_dir} -fprofile-correction"
        "-DCMAKE_CXX_FLAGS=-fprofile-use=${profile_dir} -fprofile-correction"
        "-DCMAKE_EXE_LINKER_FLAGS=-fprofile-use=${profile_dir}"
        "-DCMAKE_SHARED_LINKER_FLAGS=-fprofile-use=${profile_dir}"
        "-DCMAKE_MODULE_LINKER_FLAGS=-fprofile-use=${profile_dir}"
    )

    export TIDESDB_ROOT="${TIDESDB_PREFIX}"

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
                -DWITH_SSL=bundled
                -DWITH_PCRE=bundled
                -G Ninja
            )
            ;;
    esac

    cmake "${cmake_args[@]}"
    cmake --build "${mariadb_build}" --config Release --parallel "${JOBS}"

    ok "PGO Phase 3/3: Optimized build complete"
}

main() {
    mkdir -p "${BUILD_DIR}"

    install_deps
    build_tidesdb
    prepare_mariadb

    if $PGO_ENABLED; then
        info "PGO enabled —— performing 3-phase build (instrument ⤍ train ⤍ optimize)"
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
