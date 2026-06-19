#!/bin/bash
# SQL-level single-writer failover test for TideSQL.
#
# The library proves the fence with raw tidesdb_node processes (tidesdb's
# test/failover/run_local.sh). This is the same idea one layer up -- real mariadbd
# nodes, driven entirely over SQL, sharing one directory as the object-store
# bucket via the FS backend (tidesdb_object_store_backend=FS) -- so it needs no
# S3/MinIO and runs anywhere.
#
# N nodes share $BUCKET. All boot tidesdb_replica_mode=ON; one is promoted to
# primary. The FS connector serialises conditional writes with flock, so the
# single-writer epoch fence holds across processes exactly as it does over S3.
#
# Scenarios:
#   failover_catchup -- promote a lagging replica; it catches up to all rows and
#                       can itself DDL+write (the runtime-role regression test).
#   zombie_fence     -- promote a replica while the old primary stays alive; the
#                       old primary's later writes must be fenced out.
#
# Env-- TIDESQL_PREFIX (MariaDB install with ha_tidesdb.so), TDB_HA_KEEP=1 to keep
# the scratch dir after the run, TDB_HA_JEMALLOC to override the LD_PRELOAD path.
set -u

ROOT="$(cd "$(dirname "$0")/../.." && pwd)"
PREFIX="${TIDESQL_PREFIX:?set TIDESQL_PREFIX to the MariaDB install prefix}"
MARIADBD="$PREFIX/bin/mariadbd"
MARIADB="$PREFIX/bin/mariadb"
INSTALL_DB="$PREFIX/scripts/mariadb-install-db"
JEMALLOC="${TDB_HA_JEMALLOC:-/lib/x86_64-linux-gnu/libjemalloc.so.2}"
SCRATCH="${TDB_HA_SCRATCH:-$ROOT/test/ha_replication/.scratch}"
BUCKET="$SCRATCH/bucket"
PORT_BASE=7500
SYNC_US=500000          # replica MANIFEST poll interval
WBUF=4M                 # small write buffer -> several sstables, exercises compaction/fence
declare -a PIDS=()
fail=0

log()   { echo "[$(date -u +%FT%TZ)] $*"; }
check() { if [ "$2" = "$3" ]; then echo "  ok   $1"; else echo "  FAIL $1: got [$2] want [$3]"; fail=1; fi; }
ge()    { if [ "$2" -ge "$3" ] 2>/dev/null; then echo "  ok   $1 ($2)"; else echo "  FAIL $1: got [$2] want >= [$3]"; fail=1; fi; }

kill_nodes(){ for p in "${PIDS[@]:-}"; do kill -9 "$p" 2>/dev/null; done; PIDS=(); wait 2>/dev/null; }
final_cleanup(){ kill_nodes; [ -n "${TDB_HA_KEEP:-}" ] || rm -rf "$SCRATCH"; }
trap final_cleanup EXIT

port_of(){ echo $((PORT_BASE + $1)); }
q(){  "$MARIADB" --no-defaults -h127.0.0.1 -P"$(port_of "$1")" -uroot -N -B -e "$2" 2>/dev/null; }
x(){  "$MARIADB" --no-defaults -h127.0.0.1 -P"$(port_of "$1")" -uroot -e "$2" >/dev/null 2>&1; }
xrc(){ "$MARIADB" --no-defaults -h127.0.0.1 -P"$(port_of "$1")" -uroot -e "$2" >/dev/null 2>&1; echo $?; }
stat(){ q "$1" "SHOW GLOBAL STATUS LIKE 'Tidesdb_$2'" | awk '{print $2}'; }
rows(){ q "$1" "SELECT COUNT(*) FROM ha.t WHERE v LIKE '$2%'"; }

wait_ready(){ for _ in $(seq 1 60); do [ "$(q "$1" 'SELECT 1')" = "1" ] && return 0; sleep 0.5; done; return 1; }
# poll until node $1 is primary (runtime role flipped, status var refreshed) at epoch >= $2
wait_primary(){ for _ in $(seq 1 30); do
    [ "$(stat "$1" replica_mode_active)" = "0" ] && [ "$(stat "$1" primary_epoch)" -ge "$2" ] 2>/dev/null && return 0
    sleep 0.5; done; return 1; }
# poll until node $1 (replica) has replayed >= $4 rows of prefix $3
wait_converge(){ for _ in $(seq 1 80); do [ "$(rows "$1" "$3")" -ge "$4" ] 2>/dev/null && return 0; sleep 0.5; done; return 1; }

# one batched multi-row INSERT for ids [start..end] with value '<prefix><id>'
insert_rows(){ local port="$1" pfx="$2" s="$3" e="$4" vals="" n
    for n in $(seq "$s" "$e"); do vals+="($n,'$pfx$n'),"; done
    x "$port" "INSERT INTO ha.t VALUES ${vals%,}"; }

start_node(){ local i="$1" dir="$SCRATCH/n$1" port; port="$(port_of "$1")"
    rm -rf "$dir"; mkdir -p "$dir/data" "$dir/tdb"
    "$INSTALL_DB" --no-defaults --datadir="$dir/data" --auth-root-authentication-method=normal >/dev/null 2>&1
    cat > "$dir/my.cnf" <<EOF
[mysqld]
datadir=$dir/data
port=$port
socket=$dir/mysql.sock
pid-file=$dir/mariadb.pid
skip-grant-tables
plugin-maturity=gamma
plugin-load-add=ha_tidesdb.so
default_storage_engine=TidesDB
tidesdb_data_home_dir=$dir/tdb
tidesdb_object_store_backend=FS
tidesdb_objstore_fs_path=$BUCKET
tidesdb_replica_mode=ON
tidesdb_replica_sync_interval=$SYNC_US
tidesdb_unified_memtable=ON
tidesdb_unified_memtable_write_buffer_size=$WBUF
tidesdb_log_level=ERROR
EOF
    LD_PRELOAD="$JEMALLOC" "$MARIADBD" --defaults-file="$dir/my.cnf" >"$dir/err.log" 2>&1 &
    PIDS+=($!); }

# clean slate -- kill any nodes, wipe the bucket AND node datadirs, start fresh nodes
fresh(){ kill_nodes; rm -rf "$BUCKET" "$SCRATCH"/n*; mkdir -p "$BUCKET"
    for i in "$@"; do start_node "$i"; done
    for i in "$@"; do wait_ready "$i" || { echo "  FAIL node $i not ready (see $SCRATCH/n$i/err.log)"; fail=1; return 1; }; done; }

# scenario 1 -- a lagging replica is promoted and catches up
scenario_failover_catchup(){
    echo "== scenario failover_catchup =="
    fresh 0 1 || return
    x 0 "SET GLOBAL tidesdb_promote_primary=ON"          # node 0 -> initial primary
    wait_primary 0 1 || { echo "  FAIL node0 did not become primary"; fail=1; return; }
    check "node0 is primary"   "$(stat 0 replica_mode_active)" 0
    ge    "node0 holds a lease" "$(stat 0 primary_epoch)" 1
    x 0 "CREATE DATABASE ha; CREATE TABLE ha.t (id INT PRIMARY KEY, v VARCHAR(32)) ENGINE=TidesDB"
    insert_rows 0 k 1 400
    x 0 "OPTIMIZE TABLE ha.t"                             # flush+compact so the bucket has the data
    wait_converge 1 "" k 1 || { echo "  FAIL replica never discovered the table"; fail=1; return; }
    check "promote replica node1" "$(xrc 1 'SET GLOBAL tidesdb_promote_primary=ON')" 0
    wait_primary 1 2 || { echo "  FAIL node1 did not become primary"; fail=1; return; }
    ge    "epoch advanced on node1" "$(stat 1 primary_epoch)" 2
    sleep 2
    check "all rows present after catch-up" "$(rows 1 k)" 400
    # the promoted-from-replica node must be able to DDL+write (runtime-role fix)
    check "promoted primary can DDL+write" \
        "$(xrc 1 'CREATE TABLE ha.t2 (id INT PRIMARY KEY) ENGINE=TidesDB; INSERT INTO ha.t2 VALUES (1)')" 0
}

# scenario 2 -- a live old primary is fenced after a replica is promoted
scenario_zombie_fence(){
    echo "== scenario zombie_fence =="
    fresh 0 1 || return
    x 0 "SET GLOBAL tidesdb_promote_primary=ON"
    wait_primary 0 1 || { echo "  FAIL node0 did not become primary"; fail=1; return; }
    x 0 "CREATE DATABASE ha; CREATE TABLE ha.t (id INT PRIMARY KEY, v VARCHAR(32)) ENGINE=TidesDB"
    insert_rows 0 a 1 200
    x 0 "OPTIMIZE TABLE ha.t"
    wait_converge 1 "" a 1 || { echo "  FAIL replica did not converge"; fail=1; return; }
    check "promote replica node1" "$(xrc 1 'SET GLOBAL tidesdb_promote_primary=ON')" 0
    wait_primary 1 2 || { echo "  FAIL node1 did not become primary"; fail=1; return; }
    # the zombie (node 0, old epoch) keeps writing and publishes -> must be fenced
    insert_rows 0 z 1001 1200
    x 0 "OPTIMIZE TABLE ha.t"                             # forces the fenced manifest publish
    for _ in $(seq 1 20); do [ "$(stat 0 replica_mode_active)" = "1" ] && break; sleep 0.5; done
    check "zombie self-demoted" "$(stat 0 replica_mode_active)" 1
    insert_rows 1 b 2001 2200                             # real primary writes
    x 1 "OPTIMIZE TABLE ha.t"; sleep 2
    check "pre-failover data preserved" "$(rows 1 a)" 200
    check "new primary writes visible"  "$(rows 1 b)" 200
    check "zombie writes fenced out"    "$(rows 1 z)" 0
}

echo "TideSQL SQL-level failover (FS backend, bucket=$BUCKET)"
mkdir -p "$SCRATCH"
scenario_failover_catchup
scenario_zombie_fence
echo
if [ "$fail" = 0 ]; then echo "ALL HA REPLICATION SCENARIOS PASSED"; else echo "SOME SCENARIOS FAILED"; fi
exit "$fail"
