# Image for the SQL-level failover harness node (test infrastructure only).
# One container = one mariadbd cluster node pointed at a shared MinIO bucket over
# the TidesDB S3 object-store backend. The analogue of test/failover's tidesdb_node
# image, one layer up. Build context is the repo root:
#   docker build -f test/ha_replication/Dockerfile -t tidesql-node:ci .
#
# The build stage runs install.sh to build the S3-enabled libtidesdb + MariaDB +
# the ha_tidesdb plugin (heavy, like a full MariaDB build); the runtime stage just
# copies the install in.

FROM ubuntu:24.04 AS build
ENV DEBIAN_FRONTEND=noninteractive
RUN apt-get update && apt-get install -y --no-install-recommends \
        ca-certificates git cmake ninja-build build-essential bison \
        libssl-dev libncurses-dev pkg-config \
        liblz4-dev libzstd-dev libsnappy-dev libcurl4-openssl-dev \
        libjemalloc-dev \
    && rm -rf /var/lib/apt/lists/*
COPY . /src/tidesql
WORKDIR /src/tidesql
# --s3 builds libtidesdb with the S3 connector (conditional writes / fencing over S3).
RUN ./install.sh --s3 --mariadb-prefix /opt/mariadb --build-dir /tmp/tidesql-build \
        --jobs "$(nproc)"

FROM ubuntu:24.04
ENV DEBIAN_FRONTEND=noninteractive
# iptables: the partition scenario severs this pod's egress to MinIO from inside
# its own netns (needs NET_ADMIN on the pod). bash drives nothing here -- the
# scenario driver runs `kubectl exec ... mariadb -e <SQL>` from the runner.
RUN apt-get update && apt-get install -y --no-install-recommends \
        bash iptables ca-certificates \
        libssl3 libcurl4 liblz4-1 libzstd1 libsnappy1v5 libjemalloc2 libncurses6 \
    && rm -rf /var/lib/apt/lists/*
COPY --from=build /opt/mariadb /opt/mariadb
COPY --from=build /usr/local/lib/libtidesdb.so* /usr/local/lib/
RUN ldconfig
COPY test/ha_replication/k8s/entrypoint.sh /entrypoint.sh
RUN chmod +x /entrypoint.sh && mkdir -p /data
# the mariadb client lives in the image too, so `kubectl exec node -- mariadb ...` works
ENV PATH="/opt/mariadb/bin:${PATH}"
ENTRYPOINT ["/entrypoint.sh"]
