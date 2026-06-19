#!/bin/bash
# One mariadbd cluster node for the k8s failover simulation. Every node boots as a
# read-only replica pointed at the shared MinIO bucket over the S3 object-store
# backend; run_k8s.sh promotes one to primary. Mirrors test/failover's tidesdb_node
# but one layer up -- the node is a real MariaDB server driven over SQL.
set -e

PREFIX=/opt/mariadb
DATADIR=/data/mysql
mkdir -p "$DATADIR" /data/tdb

"$PREFIX/scripts/mariadb-install-db" --no-defaults --datadir="$DATADIR" \
    --auth-root-authentication-method=normal >/dev/null 2>&1

cat > /etc/tidesdb-node.cnf <<EOF
[mysqld]
# the container runs as root; mariadbd refuses root unless told explicitly.
user=root
datadir=$DATADIR
port=${TDB_NODE_PORT:-3306}
socket=/tmp/mysql.sock
bind-address=0.0.0.0
skip-grant-tables
plugin-maturity=gamma
plugin-load-add=ha_tidesdb.so
default_storage_engine=TidesDB

tidesdb_data_home_dir=/data/tdb
tidesdb_object_store_backend=S3
tidesdb_s3_endpoint=${TIDESDB_S3_ENDPOINT}
tidesdb_s3_bucket=${TIDESDB_S3_BUCKET}
tidesdb_s3_access_key=${TIDESDB_S3_ACCESS_KEY}
tidesdb_s3_secret_key=${TIDESDB_S3_SECRET_KEY}
tidesdb_s3_use_ssl=OFF
tidesdb_s3_path_style=ON

tidesdb_replica_mode=ON
tidesdb_replica_sync_interval=${TDB_NODE_REPLICA_SYNC_US:-500000}
tidesdb_objstore_wal_sync_on_commit=${TDB_NODE_WAL_SYNC_ON_COMMIT:-0}

tidesdb_unified_memtable=ON
tidesdb_unified_memtable_write_buffer_size=${TDB_NODE_WRITE_BUFFER:-4M}
tidesdb_log_level=ERROR
EOF

export LD_PRELOAD="${TDB_NODE_JEMALLOC:-/usr/lib/x86_64-linux-gnu/libjemalloc.so.2}"
exec "$PREFIX/bin/mariadbd" --defaults-file=/etc/tidesdb-node.cnf
