# syntax=docker/dockerfile:1
# Production image for NodeDB Origin server
# Requires Linux kernel >= 5.1 (io_uring)

# ── Stage 1: Chef base (rust + build deps + cargo-chef) ──────────────────────
FROM rust:1.96-bookworm AS chef

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

# ── Stage 5: Binary handoff ──────────────────────────────────────────────────
# The runtime pulls the binary from this stage rather than from `builder`, so
# the whole compile can be replaced with an already-built binary:
#
#   docker buildx build --build-context binary=<dir holding ./nodedb> .
#
# A `--build-context` of the same name overrides the stage, and Buildx then
# prunes every stage above it — the release pipeline uses this to reuse the
# binary it already built instead of compiling the workspace again. A plain
# `docker build .` still compiles from source exactly as before.
FROM scratch AS binary
COPY --from=builder /build/target/release/nodedb /nodedb

# ── Stage 6: Minimal runtime (Chainguard glibc-dynamic) ──────────────────────
# Wolfi-based, daily-rebuilt against patched packages — typically 0 CVEs.
# Ships only glibc + libgcc + ca-certificates + tzdata. No shell, no package
# manager, no `curl` — that's why the binary has a built-in `healthcheck`
# subcommand (see ctl/healthcheck.rs).
FROM cgr.dev/chainguard/glibc-dynamic:latest AS runtime

# `nonroot` user (uid/gid 65532) is built into the Chainguard base. The
# declared VOLUME below inherits this ownership, so named volumes work
# out of the box without an entrypoint chown step.
USER nonroot:nonroot

COPY --from=binary --chown=nonroot:nonroot /nodedb /usr/local/bin/nodedb

# Bind to all interfaces (required for Docker port mapping). Does not move the
# sync listener, which is loopback-only.
# Point data dir at the declared volume
ENV NODEDB_HOST=0.0.0.0 \
    NODEDB_DATA_DIR=/var/lib/nodedb

WORKDIR /var/lib/nodedb

# pgwire | native protocol | HTTP API | OTLP gRPC | OTLP HTTP
# Sync (9090) is omitted: it binds to loopback, so a mapping cannot reach it.
EXPOSE 6432 6433 6480 4317 4318

VOLUME ["/var/lib/nodedb"]

# Probe local /health via the binary's built-in subcommand. No curl needed.
HEALTHCHECK --interval=10s --timeout=3s --start-period=5s \
    CMD ["/usr/local/bin/nodedb", "healthcheck"]

ENTRYPOINT ["/usr/local/bin/nodedb"]
