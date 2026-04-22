#!/bin/bash
set -e

# Resolve the repository root regardless of where the script is invoked from.
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"

# Configuration
IMAGE_NAME=${IMAGE_NAME:-"tidesql"}
TAG=${TAG:-"latest"}
CONTAINER_NAME=${CONTAINER_NAME:-"tidesql"}
VOLUME_DATA="tidesql-data"
VOLUME_CONF="tidesql-conf"
VOLUME_LOG="tidesql-log"

# Storage engine selection 
# Optional engines that can be excluded or selectively included.
KNOWN_OPTIONAL_ENGINES="ARCHIVE BLACKHOLE CONNECT EXAMPLE FEDERATED FEDERATEDX MROONGA ROCKSDB S3 SPHINX SPIDER"
# Engines that are always present in the build (cannot be excluded).
ALWAYS_INCLUDED_ENGINES="TIDESDB INNODB ARIA MYISAM CSV MEMORY HEAP MERGE MRG_MyISAM SEQUENCE"

_upper_space() {
    echo "$1" | tr '[:lower:]' '[:upper:]' | tr ',' ' '
}

_is_optional_engine() {
    local e="$1"
    for k in $KNOWN_OPTIONAL_ENGINES; do [ "$k" = "$e" ] && return 0; done
    return 1
}

_is_always_included() {
    local e="$1"
    for k in $ALWAYS_INCLUDED_ENGINES; do [ "$k" = "$e" ] && return 0; done
    return 1
}

_is_known_engine() {
    _is_optional_engine "$1" || _is_always_included "$1"
}

DISABLED_ENGINES=""

if [ -n "${EXCLUDE_ENGINES:-}" ] && [ -n "${INCLUDE_ENGINES:-}" ]; then
    echo "Error: EXCLUDE_ENGINES and INCLUDE_ENGINES cannot both be set." >&2
    exit 1
fi

if [ -n "${EXCLUDE_ENGINES:-}" ]; then
    EXCL_NORM="$(_upper_space "$EXCLUDE_ENGINES")"
    if [ "$EXCL_NORM" != "ALL" ]; then
        for engine in $EXCL_NORM; do
            if _is_always_included "$engine"; then
                echo "Error: '${engine}' is always included and cannot be excluded." >&2
                exit 1
            fi
            if ! _is_optional_engine "$engine"; then
                echo "Error: Unknown engine '${engine}'. Excludable engines: $(echo "$KNOWN_OPTIONAL_ENGINES" | tr ' ' ',')" >&2
                exit 1
            fi
        done
        DISABLED_ENGINES="$(echo "$EXCL_NORM" | tr ' ' ',')"
    fi
    # EXCLUDE_ENGINES=ALL -> include all optional engines -> DISABLED_ENGINES=""

elif [ -n "${INCLUDE_ENGINES:-}" ]; then
    INCL_NORM="$(_upper_space "$INCLUDE_ENGINES")"
    for engine in $INCL_NORM; do
        if ! _is_known_engine "$engine"; then
            echo "Error: Unknown engine '${engine}'. Optional engines: $(echo "$KNOWN_OPTIONAL_ENGINES" | tr ' ' ',')" >&2
            exit 1
        fi
    done
    # DISABLED = KNOWN_OPTIONAL minus what was requested to be included.
    _disabled=""
    for engine in $KNOWN_OPTIONAL_ENGINES; do
        _found=0
        for incl in $INCL_NORM; do
            if [ "$incl" = "$engine" ]; then _found=1; break; fi
        done
        if [ "$_found" = "0" ]; then
            _disabled="${_disabled:+${_disabled},}${engine}"
        fi
    done
    DISABLED_ENGINES="$_disabled"
fi

# Resolve MARIADB_VERSION and TIDESDB_VERSION 
MARIADB_VERSION=${MARIADB_VERSION:-"11.8"}

if [ -z "${TIDESDB_VERSION:-}" ]; then
    _tidesdb_latest=$(curl -fsSL "https://api.github.com/repos/tidesdb/tidesdb/releases/latest" 2>/dev/null \
        | grep '"tag_name":' | sed -E 's/.*"tag_name": *"([^"]+)".*/\1/')
    if [ -z "$_tidesdb_latest" ]; then
        echo "Warning: Could not fetch latest TidesDB version from GitHub; falling back to v8.9.0." >&2
        _tidesdb_latest="v8.9.0"
    fi
    TIDESDB_VERSION="$_tidesdb_latest"
fi

# Memory allocator for libtidesdb.so.  One of: system (default), jemalloc, mimalloc, tcmalloc.
# Forwarded to the Dockerfile as --build-arg ALLOCATOR=...  Affects only TidesDB's
# internal allocations; for a process-wide swap also LD_PRELOAD at container startup.
ALLOCATOR=${ALLOCATOR:-system}
case "${ALLOCATOR}" in
    system|jemalloc|mimalloc|tcmalloc) ;;
    *) echo "Error: ALLOCATOR must be one of system|jemalloc|mimalloc|tcmalloc (got '${ALLOCATOR}')" >&2; exit 1 ;;
esac

echo "### 2. Building the new image..."
BUILD_ARGS=(
    --build-arg "MARIADB_VERSION=${MARIADB_VERSION}"
    --build-arg "TIDESDB_VERSION=${TIDESDB_VERSION}"
    --build-arg "ALLOCATOR=${ALLOCATOR}"
)
[ -n "$DISABLED_ENGINES" ] && BUILD_ARGS+=(--build-arg "DISABLED_ENGINES=${DISABLED_ENGINES}")
docker build \
    -f "${REPO_ROOT}/docker/ubuntu/Dockerfile" \
    -t "${IMAGE_NAME}:${TAG}" \
    "${BUILD_ARGS[@]}" \
    "${REPO_ROOT}"

echo "### 3. Starting the container..."
docker run -d \
    --name "$CONTAINER_NAME" \
    -p 3306:3306 \
    -v "$VOLUME_CONF":/etc/mysql \
    -v "$VOLUME_DATA":"${MARIADB_PREFIX:-/usr/local/mariadb}"/data \
    -v "$VOLUME_LOG":"${MARIADB_PREFIX:-/usr/local/mariadb}"/log \
    "${IMAGE_NAME}:${TAG}"
r=$?

echo
echo '-----'
echo
echo "### Done!"
echo "Exit code: $r"
echo "Logs:"
docker logs -f "$CONTAINER_NAME"
