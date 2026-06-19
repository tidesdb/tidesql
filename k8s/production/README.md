# TideSQL HA on Kubernetes (production)

A production-oriented HA deployment for TideSQL on Kubernetes: a single identical
pool of MariaDB pods with a lease-elected primary and object-store-fenced
single-writer safety, so a failed primary is replaced and rejoins as a replica
without split-brain.

## Design in one paragraph

One `StatefulSet` of N identical pods, all booting read-only
(`tidesdb_replica_mode=ON`). Exactly one pod holds the `tidesql-primary` Lease
and is the writer; a per-pod agent sidecar runs the election. When a pod wins the
lease it promotes its local engine (`SET GLOBAL tidesdb_promote_primary=ON`) and
labels itself `role=primary`, which is how the `tidesql-write` Service finds it.
When the primary dies the lease expires, another pod wins and promotes; the dead
pod, when k8s reschedules it, boots as a replica again (it doesn't hold the
lease) and re-syncs from S3 -- the topology self-heals.

## Why split-brain can't happen

Two things, layered:

1. **The engine epoch fence (the real guarantee).** Promotion claims a new lease
   epoch in the object store via a conditional write; from then on a superseded
   primary's manifest publish fails its precondition and its writes can never
   become authoritative. This is what makes correctness independent of the agent
   and of any kill timing. It is exercised end to end -- promote, zombie, and
   network-partition fencing -- by the failover suites: `test/ha_replication/`
   here at the SQL layer, and `tidesdb`'s `test/failover/` at the library layer.
2. **The k8s Lease (availability + routing).** Decides who *should* be primary
   and where writes route. Because the engine fence is the safety net, this layer
   only needs to be good enough for availability -- it does not have to be a
   perfect leader election to be *safe*.

That layering is deliberate: the agent does optimistic-concurrency election (it
`kubectl replace`s the Lease carrying the observed `resourceVersion`, so a stale
write gets a 409 and loses -- two pods can't both win), and the engine fence
guarantees safety regardless of how the election resolves. The two together give
a correct single writer.

## Observability (the contract the agent uses)

The agent and any operator read these status vars (added in the plugin):

| Question | Check |
|---|---|
| Am I primary? | `Tidesdb_replica_mode_active = 0` and `Tidesdb_primary_epoch > 0` |
| Did my promotion take? | `Tidesdb_primary_epoch` advanced and `SET … promote_primary` returned no error |
| Was I fenced? | `Tidesdb_replica_mode_active` flipped back to `1` |

`SET GLOBAL tidesdb_promote_primary=ON` now **returns an error** when it loses the
lease race (engine fence), so the agent doesn't falsely believe it became primary.

## Before you deploy

- **Secret.** Fill `tidesql-s3` with real keys, or (better) delete it and use IRSA
  / workload identity so there are no static keys. The keys are substituted into
  the cnf at startup and never live in the ConfigMap.
- **Agent image.** The `agent` sidecar needs `kubectl`, `jq`, the `mariadb`
  client, and **GNU `date`** -- the script parses RFC3339 times and uses `%N`,
  which BusyBox `date` (Alpine's default) cannot, so a BusyBox base makes the
  agent treat the lease as permanently expired and churn. Use a glibc/coreutils
  base: `debian:stable-slim` plus those tools, or `alpine` with `apk add
  coreutils`. The manifest references `tidesdb/tidesql-agent:latest` as a
  placeholder.
- **mariadb image contract.** The `mariadb` container renders the cnf to
  `/etc/mysql/custom/tidesdb-k8s.cnf`, so the `tidesdb/tidesql` image must include
  that directory in its config search path. The probes connect as a passwordless
  root over the local socket, so don't set a root password on the image (or adjust
  the probes).
- **StorageClass.** `storageClassName: gp3` is AWS EKS-specific -- change it for
  your platform (e.g. `standard`, `premium-rwo`).
- **Engine auth.** The agent connects as `root` over localhost. Wire real auth
  (a dedicated `repl`/agent user with only the needed grants) before production.
- **RPO.** `tidesdb_objstore_wal_sync_on_commit` is `OFF` (async upload, small
  RPO). Set it `ON` for RPO≈0 at a commit-latency cost. Make it a conscious call.
- **Lease TTL.** `leaseDurationSeconds` (15) and the agent's `RENEW` (5) set how
  fast failover is and how tolerant it is of blips. Tune together; keep
  `RENEW < TTL/2`.

## Deploy

```bash
kubectl apply -f tidesql-ha.yaml
# watch who becomes primary
kubectl -n tidesql get pods -L role -w
# confirm exactly one primary
kubectl -n tidesql get pods -l role=primary
```

Connect for writes via `tidesql-write.tidesql.svc`, for reads via
`tidesql-read.tidesql.svc`.
