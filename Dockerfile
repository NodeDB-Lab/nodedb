# syntax=docker/dockerfile:1
# Production image for NodeDB Origin server
# Requires Linux kernel >= 5.1 (io_uring)

# ── Stage 1: Chef base (rust + build deps + cargo-chef) ──────────────────────
FROM rust:1.94-bookworm AS chef

RUN apt-get update && apt-get install -y --no-install-recommends \
    cmake \
    clang \
    libclang-dev \
    pkg-config \
    protobuf-compiler \
    perl \
    && rm -rf /var/lib/apt/lists/*

RUN cargo install cargo-chef --locked
WORKDIR /build

# ── Stage 2: Dependency plan ──────────────────────────────────────────────────
FROM chef AS planner
COPY . .
RUN cargo chef prepare --recipe-path recipe.json --bin nodedb

# ── Stage 3: Build dependencies (cached — only reruns if Cargo.lock changes) ──
FROM chef AS builder
COPY --from=planner /build/recipe.json recipe.json
RUN cargo chef cook --release --recipe-path recipe.json --bin nodedb

# ── Stage 4: Build server binary ─────────────────────────────────────────────
COPY . .
RUN cargo build --release -p nodedb

# ── Stage 5: Minimal runtime ──────────────────────────────────────────────────
FROM debian:bookworm-slim AS runtime

# ca-certificates: needed for JWKS fetch, OTLP export, S3 archival
# curl: needed for HEALTHCHECK
# gosu: drop privileges from root after fixing data-dir ownership in entrypoint
RUN apt-get update && apt-get install -y --no-install-recommends \
    ca-certificates \
    curl \
    gosu \
    && rm -rf /var/lib/apt/lists/*

# Non-root user
RUN groupadd --system --gid 10001 nodedb \
    && useradd --system --uid 10001 --gid 10001 --no-create-home nodedb

# Data and config directories
RUN mkdir -p /var/lib/nodedb /etc/nodedb \
    && chown nodedb:nodedb /var/lib/nodedb

COPY --from=builder /build/target/release/nodedb /usr/local/bin/nodedb

# Entrypoint: when started as root, fix data-dir ownership and drop to the
# nodedb user. When already started as a non-root user (e.g. `--user 10001`),
# exec directly. This makes `-v <named-volume>:/var/lib/nodedb` work even
# when Docker initialises the volume as root-owned (common on Linux hosts).
COPY docker-entrypoint.sh /usr/local/bin/docker-entrypoint.sh
RUN chmod +x /usr/local/bin/docker-entrypoint.sh

# Bind to all interfaces (required for Docker port mapping)
# Point data dir at the declared volume
ENV NODEDB_HOST=0.0.0.0 \
    NODEDB_DATA_DIR=/var/lib/nodedb

WORKDIR /var/lib/nodedb

# pgwire | native protocol | HTTP API | WebSocket sync | OTLP gRPC | OTLP HTTP
EXPOSE 6432 6433 6480 9090 4317 4318

VOLUME ["/var/lib/nodedb"]

HEALTHCHECK --interval=10s --timeout=3s --start-period=5s \
    CMD curl -f http://localhost:6480/health || exit 1

ENTRYPOINT ["/usr/local/bin/docker-entrypoint.sh"]
CMD ["/usr/local/bin/nodedb"]
