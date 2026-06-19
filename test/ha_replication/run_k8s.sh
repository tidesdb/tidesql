#!/bin/bash
# Cloud-simulation SQL failover test: real mariadbd pods on a kind cluster fencing
# against a real MinIO bucket over the TidesDB S3 connector's conditional writes.
# The analogue of tidesdb's test/failover/run_k8s.sh, one layer up -- each node is
# a MariaDB server and the driver speaks SQL via `kubectl exec`.
#
# Assumes: a kind cluster is up, and the image tidesql-node:ci has been built and
# `kind load`ed (the CI workflow does both). Run from the repo root.
set -u
HERE="$(cd "$(dirname "$0")" && pwd)"
NODES=(tidesql-node-0 tidesql-node-1 tidesql-node-2)
fail=0

log()   { echo "[$(date -u +%FT%TZ)] $*"; }
check() { if [ "$2" = "$3" ]; then echo "  ok   $1"; else echo "  FAIL $1: got [$2] want [$3]"; fail=1; fi; }
ge()    { if [ "$2" -ge "$3" ] 2>/dev/null; then echo "  ok   $1 ($2)"; else echo "  FAIL $1: got [$2] want >= [$3]"; fail=1; fi; }

# SQL helpers -- run inside a pod against its local mariadbd
ksql(){ kubectl exec "$1" -- mariadb -h127.0.0.1 -uroot -N -B -e "$2" 2>/dev/null; }
kx(){   kubectl exec "$1" -- mariadb -h127.0.0.1 -uroot -e "$2" >/dev/null 2>&1; }
kxrc(){ kubectl exec "$1" -- mariadb -h127.0.0.1 -uroot -e "$2" >/dev/null 2>&1; echo $?; }
kstat(){ ksql "$1" "SHOW GLOBAL STATUS LIKE 'Tidesdb_$2'" | awk '{print $2}'; }
krows(){ ksql "$1" "SELECT COUNT(*) FROM ha.t WHERE v LIKE '$2%'"; }
promote(){ kx "$1" "SET GLOBAL tidesdb_promote_primary=ON"; }
insert_rows(){ local pod="$1" pfx="$2" s="$3" e="$4" vals="" n
    for n in $(seq "$s" "$e"); do vals+="($n,'$pfx$n'),"; done
    kx "$pod" "INSERT INTO ha.t VALUES ${vals%,}"; }

# sever a pod's egress to MinIO from inside its own netns (NET_ADMIN)
partition_from_minio(){ local ip; ip="$(kubectl get svc minio -o jsonpath='{.spec.clusterIP}')"
    kubectl exec "$1" -- iptables -A OUTPUT -d "$ip" -p tcp --dport 9000 -j REJECT; }
heal_partition(){ local ip; ip="$(kubectl get svc minio -o jsonpath='{.spec.clusterIP}')"
    kubectl exec "$1" -- iptables -D OUTPUT -d "$ip" -p tcp --dport 9000 -j REJECT 2>/dev/null || true; }

wait_primary(){ for _ in $(seq 1 40); do
    [ "$(kstat "$1" replica_mode_active)" = "0" ] && [ "$(kstat "$1" primary_epoch)" -ge "$2" ] 2>/dev/null && return 0
    sleep 1; done; return 1; }
wait_converge(){ for _ in $(seq 1 90); do [ "$(krows "$1" "$3")" -ge "$4" ] 2>/dev/null && return 0; sleep 1; done; return 1; }

deploy(){
    kubectl apply -f "$HERE/k8s/minio.yaml" >/dev/null
    kubectl rollout status deploy/minio --timeout=120s >/dev/null
    # wipe and recreate the bucket so each scenario boots fresh nodes against an
    # empty store -- otherwise a later scenario inherits the previous one's table,
    # schema cf and lease epoch and never converges
    kubectl run mc --image=minio/mc:latest --restart=Never --rm -i --quiet --command -- \
        sh -c "mc alias set m http://minio:9000 minioadmin minioadmin >/dev/null && \
               mc rb --force m/tidesql-failover >/dev/null 2>&1; \
               mc mb -p m/tidesql-failover >/dev/null && echo made" >/dev/null 2>&1
    kubectl apply -f "$HERE/k8s/nodes.yaml" >/dev/null
    for n in "${NODES[@]}"; do kubectl wait --for=condition=ready "pod/$n" --timeout=180s >/dev/null || {
        echo "  FAIL $n not ready"; kubectl logs "$n" --tail=40; fail=1; return 1; }; done
}
teardown(){ kubectl delete -f "$HERE/k8s/nodes.yaml" --ignore-not-found --grace-period=0 --force >/dev/null 2>&1; }
# dump the relevant node log lines before teardown removes the pods, so a failure is diagnosable
dump_logs(){ for n in "${NODES[@]}"; do
    echo "----- logs $n -----"
    kubectl logs "$n" --tail=80 2>&1 | grep -iE "tidesdb|schema|discover|replica|primary|epoch|fenc|object store|s3|error|fail|abort" || echo "  (no matching log lines)"
  done; }

bootstrap_primary(){   # promote node-0, create the table, load baseline rows of prefix $1
    promote tidesql-node-0
    wait_primary tidesql-node-0 1 || { echo "  FAIL node-0 did not become primary"; fail=1; return 1; }
    kx tidesql-node-0 "CREATE DATABASE ha; CREATE TABLE ha.t (id INT PRIMARY KEY, v VARCHAR(32)) ENGINE=TidesDB"
    insert_rows tidesql-node-0 "$1" 1 200
    kx tidesql-node-0 "OPTIMIZE TABLE ha.t"
}

# scenario 1: promote a replica, surviving replica re-follows
scenario_failover_refollow(){
    echo "== k8s scenario failover_refollow =="
    deploy || return; bootstrap_primary k
    wait_converge tidesql-node-1 "" k 1 || { echo "  FAIL replica never converged"; dump_logs; fail=1; teardown; return; }
    check "promote node-1" "$(kxrc tidesql-node-1 'SET GLOBAL tidesdb_promote_primary=ON')" 0
    wait_primary tidesql-node-1 2 || { echo "  FAIL node-1 not primary"; fail=1; teardown; return; }
    check "promoted node has all rows" "$(krows tidesql-node-1 k)" 200
    insert_rows tidesql-node-1 m 1001 1100; kx tidesql-node-1 "OPTIMIZE TABLE ha.t"
    wait_converge tidesql-node-2 m m 1 >/dev/null 2>&1
    sleep 3
    check "node-2 re-followed new primary" "$(krows tidesql-node-2 m)" 100
    teardown
}

# scenario 2: a live old primary is fenced (zombie)
scenario_zombie_fence(){
    echo "== k8s scenario zombie_fence =="
    deploy || return; bootstrap_primary a
    wait_converge tidesql-node-1 "" a 1 || { echo "  FAIL replica did not converge"; dump_logs; fail=1; teardown; return; }
    check "promote node-1" "$(kxrc tidesql-node-1 'SET GLOBAL tidesdb_promote_primary=ON')" 0
    wait_primary tidesql-node-1 2 || { echo "  FAIL node-1 not primary"; fail=1; teardown; return; }
    insert_rows tidesql-node-0 z 1001 1200; kx tidesql-node-0 "OPTIMIZE TABLE ha.t"   # zombie publishes
    for _ in $(seq 1 25); do [ "$(kstat tidesql-node-0 replica_mode_active)" = "1" ] && break; sleep 1; done
    check "zombie self-demoted"        "$(kstat tidesql-node-0 replica_mode_active)" 1
    insert_rows tidesql-node-1 b 2001 2200; kx tidesql-node-1 "OPTIMIZE TABLE ha.t"; sleep 3
    check "pre-failover data preserved" "$(krows tidesql-node-1 a)" 200
    check "new primary writes visible"  "$(krows tidesql-node-1 b)" 200
    check "zombie writes fenced out"    "$(krows tidesql-node-1 z)" 0
    teardown
}

# scenario 3: partition the old primary from MinIO, then promote
scenario_partition_fence(){
    echo "== k8s scenario partition_fence =="
    deploy || return; bootstrap_primary a
    wait_converge tidesql-node-1 "" a 1 || { echo "  FAIL replica did not converge"; dump_logs; fail=1; teardown; return; }
    partition_from_minio tidesql-node-0          # old primary is now cut off from the bucket
    check "promote node-1 while node-0 partitioned" "$(kxrc tidesql-node-1 'SET GLOBAL tidesdb_promote_primary=ON')" 0
    wait_primary tidesql-node-1 2 || { echo "  FAIL node-1 not primary"; fail=1; heal_partition tidesql-node-0; teardown; return; }
    insert_rows tidesql-node-0 z 1001 1200 || true   # writes locally, cannot reach the bucket
    kx tidesql-node-0 "OPTIMIZE TABLE ha.t" || true
    heal_partition tidesql-node-0                # partition heals; node-0 now sees the higher epoch
    for _ in $(seq 1 30); do [ "$(kstat tidesql-node-0 replica_mode_active)" = "1" ] && break; sleep 1; done
    insert_rows tidesql-node-1 b 2001 2200; kx tidesql-node-1 "OPTIMIZE TABLE ha.t"; sleep 3
    check "partitioned old primary fenced" "$(kstat tidesql-node-0 replica_mode_active)" 1
    check "new primary data intact"        "$(krows tidesql-node-1 b)" 200
    check "partitioned writes fenced out"  "$(krows tidesql-node-1 z)" 0
    teardown
}

echo "TideSQL k8s SQL-level failover (kind + MinIO, S3 backend)"
scenario_failover_refollow
scenario_zombie_fence
scenario_partition_fence
echo
if [ "$fail" = 0 ]; then echo "ALL K8S HA SCENARIOS PASSED"; else echo "SOME K8S SCENARIOS FAILED"; fi
exit "$fail"
