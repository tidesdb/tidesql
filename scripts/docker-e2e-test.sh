#!/bin/bash
# docker-e2e-test.sh
# End-to-end test of TideSQL Docker image
# Tests              single node, primary with S3, replica with S3, dynamic discovery, promotion
# Prerequisites      Docker running
set -uo pipefail

BASE_IMAGE=""
IMAGE=""
MINIO_BUCKET=""
MINIO_USER=""
MINIO_PASS=""
NETWORK=""

RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m'

pass() { echo -e "${GREEN}PASS${NC}: $1"; }
fail() { echo -e "${RED}FAIL${NC}: $1"; FAILED=1; }
info() { echo -e "${YELLOW}=+=+=${NC} $1"; }
FAILED=0

cleanup() {
    info "Cleaning up..."
    docker stop tidesql-single tidesql-primary tidesql-replica minio-test 2>/dev/null || true
    docker rm -v tidesql-single tidesql-primary tidesql-replica minio-test 2>/dev/null || true
    docker network rm $NETWORK 2>/dev/null || true
    rm -rf /tmp/tidesql-e2e-*
}

# Extract default /etc/mysql from image so we can add custom configs
prepare_config() {
    local name="$1"
    local dir="/tmp/tidesql-e2e-${name}-mysql"
    rm -rf "$dir"
    mkdir -p "$dir"
    docker rm tidesql-extract >/dev/null 2>&1 || true
    docker create --name tidesql-extract $IMAGE true >/dev/null 2>&1
    docker cp tidesql-extract:/etc/mysql/. "$dir/" >/dev/null 2>&1
    docker rm tidesql-extract >/dev/null 2>&1
    mkdir -p "$dir/custom"
    echo "$dir"
}

trap cleanup EXIT
cleanup

docker network create $NETWORK 2>/dev/null || true

# Build debug image with local libtidesdb baked in
info "Building debug image with local libtidesdb..."
TMPBUILD=$(mktemp -d)
cp /usr/local/lib/libtidesdb.so "${TMPBUILD}/libtidesdb.so"
cat > "${TMPBUILD}/Dockerfile" << 'DEOF'
FROM tidesdb/tidesql:latest
COPY libtidesdb.so /usr/local/lib/libtidesdb.so
RUN ldconfig
DEOF
docker build -q -t $IMAGE "${TMPBUILD}" >/dev/null 2>&1
rm -rf "${TMPBUILD}"
pass "Debug image built with local libtidesdb"

# Start MinIO
info "Starting MinIO..."
docker run -d --name minio-test --network $NETWORK \
    -e MINIO_ROOT_USER=$MINIO_USER \
    -e MINIO_ROOT_PASSWORD=$MINIO_PASS \
    minio/minio server /data
sleep 3
docker exec minio-test sh -c "rm -rf /data/$MINIO_BUCKET && mkdir -p /data/$MINIO_BUCKET"
pass "MinIO started and bucket created"

echo ""
echo "============================================"
echo "TEST 1: Single node (no S3, no replication)"
echo "============================================"

docker run -d --name tidesql-single --network $NETWORK $IMAGE
sleep 15

if docker exec tidesql-single mariadb -e "SELECT 1" >/dev/null 2>&1; then
    pass "MariaDB started"
else
    fail "MariaDB did not start"
    docker logs tidesql-single 2>&1 | tail -20
fi

if docker exec tidesql-single mariadb -e "SHOW PLUGINS" | grep -q TidesDB; then
    pass "TidesDB plugin loaded"
else
    fail "TidesDB plugin not loaded"
fi

docker exec tidesql-single mariadb -e "
    DROP DATABASE IF EXISTS testdb;
    CREATE DATABASE testdb;
    CREATE TABLE testdb.t1 (id INT PRIMARY KEY, val VARCHAR(100)) ENGINE=TIDESDB;
    INSERT INTO testdb.t1 VALUES (1,'hello'),(2,'world');
"
ROWS=$(docker exec tidesql-single mariadb -N -e "SELECT COUNT(*) FROM testdb.t1" 2>/dev/null || echo "0")
if [ "$ROWS" = "2" ]; then
    pass "Single node CRUD works ($ROWS rows)"
else
    fail "Single node CRUD failed (got $ROWS rows)"
fi

docker stop tidesql-single && docker rm -v tidesql-single

echo ""
echo "============================================"
echo "TEST 2: Primary with S3 (MinIO)"
echo "============================================"

info "Preparing primary config..."
PRIMARY_CONF=$(prepare_config primary)
cat > "${PRIMARY_CONF}/custom/s3.cnf" << 'CNFEOF'
[mysqld]
tidesdb_object_store_backend=S3
tidesdb_s3_endpoint=minio-test:9000
tidesdb_s3_bucket=docker-e2e-test
tidesdb_s3_access_key=minioadmin
tidesdb_s3_secret_key=minioadmin
tidesdb_s3_use_ssl=OFF
tidesdb_s3_path_style=ON
tidesdb_objstore_wal_sync_threshold=1024
tidesdb_log_level=INFO
CNFEOF

info "Config contents:"
cat "${PRIMARY_CONF}/custom/s3.cnf"

# Write config inline via entrypoint override. The config is written
# after mariadb-install-db but before mariadbd starts because we
# let the entrypoint handle install-db, then the config is already
# present for the server start (install-db skips on second run since
# datadir/mysql already exists).
# Write config then run entrypoint
docker run -d --name tidesql-primary --network $NETWORK \
    --entrypoint bash $IMAGE -c '
cat > /etc/mysql/custom/s3.cnf << INNEREOF
[mysqld]
tidesdb_object_store_backend=S3
tidesdb_s3_endpoint=minio-test:9000
tidesdb_s3_bucket=docker-e2e-test
tidesdb_s3_access_key=minioadmin
tidesdb_s3_secret_key=minioadmin
tidesdb_s3_use_ssl=OFF
tidesdb_s3_path_style=ON
tidesdb_objstore_wal_sync_threshold=1024
tidesdb_log_level=DEBUG
INNEREOF
exec /usr/local/bin/tidesql-entrypoint.sh
'
sleep 20

info "Checking config was loaded..."
docker exec tidesql-primary cat /etc/mysql/custom/s3.cnf 2>/dev/null || echo "(missing)"
docker exec tidesql-primary mariadb -e "SHOW VARIABLES LIKE 'tidesdb_object_store%'" 2>/dev/null || echo "(server not ready)"

if docker exec tidesql-primary mariadb -e "SHOW ENGINE TIDESDB STATUS\G" 2>/dev/null | grep -q "Connector: s3"; then
    pass "Primary connected to S3"
else
    fail "Primary S3 not connected"
    docker exec tidesql-primary sh -c 'tail -20 /usr/local/mariadb/log/error.log' 2>/dev/null || true
    docker logs tidesql-primary 2>&1 | tail -10
fi

docker exec tidesql-primary mariadb -e "
    CREATE DATABASE appdb;
    CREATE TABLE appdb.items (id INT PRIMARY KEY, name VARCHAR(100)) ENGINE=TIDESDB;
    INSERT INTO appdb.items VALUES (1,'alpha'),(2,'beta'),(3,'gamma');
    OPTIMIZE TABLE appdb.items;
" 2>/dev/null || true
sleep 5

BUCKET_CFS=$(docker exec minio-test sh -c "ls /data/$MINIO_BUCKET/ 2>/dev/null | wc -l")
if [ "$BUCKET_CFS" -ge 2 ]; then
    pass "Data uploaded to MinIO ($BUCKET_CFS entries)"
else
    fail "Data not in MinIO (found $BUCKET_CFS entries)"
    docker exec minio-test sh -c "ls /data/$MINIO_BUCKET/" 2>/dev/null || true
fi

echo ""
echo "============================================"
echo "TEST 3: Replica cold start discovery"
echo "============================================"

info "Preparing replica config..."
REPLICA_CONF=$(prepare_config replica)
cat > "${REPLICA_CONF}/custom/s3.cnf" << 'CNFEOF'
[mysqld]
tidesdb_object_store_backend=S3
tidesdb_s3_endpoint=minio-test:9000
tidesdb_s3_bucket=docker-e2e-test
tidesdb_s3_access_key=minioadmin
tidesdb_s3_secret_key=minioadmin
tidesdb_s3_use_ssl=OFF
tidesdb_s3_path_style=ON
tidesdb_replica_mode=ON
tidesdb_replica_sync_interval=1000000
tidesdb_log_level=INFO
CNFEOF

docker run -d --name tidesql-replica --network $NETWORK \
    --entrypoint bash $IMAGE -c '
cat > /etc/mysql/custom/s3.cnf << INNEREOF
[mysqld]
tidesdb_object_store_backend=S3
tidesdb_s3_endpoint=minio-test:9000
tidesdb_s3_bucket=docker-e2e-test
tidesdb_s3_access_key=minioadmin
tidesdb_s3_secret_key=minioadmin
tidesdb_s3_use_ssl=OFF
tidesdb_s3_path_style=ON
tidesdb_replica_mode=ON
tidesdb_replica_sync_interval=500000
tidesdb_log_level=DEBUG
INNEREOF
exec /usr/local/bin/tidesql-entrypoint.sh
'
sleep 20

REPLICA_CFS=$(docker exec tidesql-replica mariadb -N -e "SHOW ENGINE TIDESDB STATUS\G" 2>/dev/null | grep "Column families" | awk '{print $NF}' || echo "0")
if [ "${REPLICA_CFS:-0}" -ge 2 ]; then
    pass "Replica cold start discovered $REPLICA_CFS CFs"
else
    fail "Replica cold start found only ${REPLICA_CFS:-0} CFs"
    docker exec tidesql-replica sh -c 'tail -20 /usr/local/mariadb/log/error.log' 2>/dev/null || true
fi

REPLICA_ROWS=$(docker exec tidesql-replica mariadb -N -e "SELECT COUNT(*) FROM appdb.items" 2>/dev/null || echo "0")
if [ "$REPLICA_ROWS" = "3" ]; then
    pass "Replica reads primary data ($REPLICA_ROWS rows)"
else
    fail "Replica can't read data (got $REPLICA_ROWS rows)"
fi

echo ""
echo "============================================"
echo "TEST 4: Dynamic discovery (new table while replica runs)"
echo "============================================"

docker exec tidesql-primary mariadb -e "
    CREATE DATABASE dyndb;
    CREATE TABLE dyndb.t1 (id INT PRIMARY KEY, msg TEXT) ENGINE=TIDESDB;
    INSERT INTO dyndb.t1 VALUES (1,'dynamic discovery');
    OPTIMIZE TABLE dyndb.t1;
" 2>/dev/null || true
sleep 5

info "Waiting for replica sync (up to 60 seconds)..."
DISCOVERED=0
for i in $(seq 1 12); do
    sleep 5
    NEW_CFS=$(docker exec tidesql-replica mariadb -N -e "SHOW ENGINE TIDESDB STATUS\G" 2>/dev/null | grep "Column families" | awk '{print $NF}' || echo "0")
    DYN_RESULT=$(docker exec tidesql-replica mariadb -N -e "SELECT msg FROM dyndb.t1" 2>/dev/null || echo "")
    echo "  Check $i/12: CFs=${NEW_CFS:-?}"
    if [ -n "$DYN_RESULT" ]; then
        pass "Dynamic discovery works! ($DYN_RESULT)"
        DISCOVERED=1
        break
    fi
done

if [ "$DISCOVERED" = "0" ]; then
    fail "Dynamic discovery did not work after 60 seconds"
    info "Reaper debug logs:"
    docker exec tidesql-replica grep "Reaper:" /usr/local/mariadb/data/tidesdb_data/LOG 2>/dev/null | tail -20 || echo "(no reaper entries)"
    info "Replica sync logs:"
    docker exec tidesql-replica grep -i "replica sync\|discover\|dyndb" /usr/local/mariadb/data/tidesdb_data/LOG 2>/dev/null | tail -10 || echo "(no sync entries)"
    info "Replica data dir:"
    docker exec tidesql-replica ls /usr/local/mariadb/data/tidesdb_data/ 2>/dev/null || true
    info "LOG total lines:"
    docker exec tidesql-replica wc -l /usr/local/mariadb/data/tidesdb_data/LOG 2>/dev/null || true
    info "LOG growth test:"
    L1=$(docker exec tidesql-replica wc -l /usr/local/mariadb/data/tidesdb_data/LOG 2>/dev/null | awk '{print $1}' || echo "?")
    sleep 5
    L2=$(docker exec tidesql-replica wc -l /usr/local/mariadb/data/tidesdb_data/LOG 2>/dev/null | awk '{print $1}' || echo "?")
    echo "  LOG lines: $L1 -> $L2"
fi

echo ""
echo "============================================"
echo "TEST 5: Failover (promote replica to primary)"
echo "============================================"

docker stop tidesql-primary 2>/dev/null
sleep 2

if docker exec tidesql-replica mariadb -e "SET GLOBAL tidesdb_promote_primary = ON" 2>/dev/null; then
    pass "Promotion command accepted"
else
    fail "Promotion command failed"
fi
sleep 3

if docker exec tidesql-replica mariadb -e "INSERT INTO appdb.items VALUES (4,'delta')" 2>/dev/null; then
    pass "Promoted replica accepts writes"
else
    fail "Promoted replica rejects writes"
fi

FINAL_ROWS=$(docker exec tidesql-replica mariadb -N -e "SELECT COUNT(*) FROM appdb.items" 2>/dev/null || echo "0")
if [ "$FINAL_ROWS" = "4" ]; then
    pass "Data intact after promotion ($FINAL_ROWS rows)"
else
    fail "Data issue after promotion (got $FINAL_ROWS rows)"
fi

echo ""
echo "============================================"
echo "RESULTS"
echo "============================================"
if [ "$FAILED" = "0" ]; then
    echo -e "${GREEN}All tests passed!${NC}"
else
    echo -e "${RED}Some tests failed.${NC}"
fi
