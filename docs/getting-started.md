# Getting Started

This guide walks you through starting a NodeDB server and running your first queries. NodeDB requires Linux kernel ≥ 5.1 (for io_uring) regardless of how you run it.

There are three ways to install NodeDB:

1. [Prebuilt binary](#run-a-prebuilt-binary-linux) — **recommended on Linux.** Direct kernel access to io_uring, no virtualization overhead, best raw performance.
2. [Docker](#run-with-docker) — **recommended on macOS / Windows / WSL2**, or when you want a one-command setup with zero host configuration.
3. [Build from source](#build-from-source) — for development or custom features.

All three share the same [configuration](#configuration), [connection](#connect), and [query](#first-queries) sections below.

## Run with Docker

The quickest start, and the right choice on macOS and Windows. Also the right choice on any host where you avoid managing a binary. On native Linux, the [prebuilt binary](#run-a-prebuilt-binary-linux) gives you better performance.

### Docker Compose

```bash
docker compose up -d
```

That's it. NodeDB starts on the default ports with data persisted to a named volume.

To stop:

```bash
docker compose down
```

To stop and wipe all data:

```bash
docker compose down -v
```

### Plain `docker run`

If you'd rather not use Compose:

```bash
docker run -d --name nodedb \
  -p 6432:6432 \
  -p 6433:6433 \
  -p 6480:6480 \
  -v nodedb-data:/var/lib/nodedb \
  farhansyah/nodedb
```

Sync (9090) is omitted: it binds to loopback, so a mapping cannot reach it.
See [Protocols](protocols.md).

The container entrypoint runs as root only to fix ownership on the data volume. It then drops privileges to the `nodedb` user (uid 10001). To skip the root step, pass `--user 10001:10001` and pre-create the volume with matching ownership.

### Default ports

- **6432** — PostgreSQL wire protocol (pgwire)
- **6433** — Native MessagePack protocol
- **6480** — HTTP API (REST, SSE, WebSocket)
- **9090** — WebSocket sync (NodeDB-Lite clients). Loopback only.

### Verify it's running

```bash
curl http://localhost:6480/healthz
# {"status":"ok", ...}
```

### Custom port mapping

Edit `docker-compose.yml` to remap any port. The container always listens internally on the same ports — only the host-side mapping changes:

```yaml
ports:
  - "5432:6432" # expose pgwire on host port 5432 instead
  - "6433:6433"
  - "6480:6480"
```

### Common env vars

| Variable                  | Default    | Description                  |
| ------------------------- | ---------- | ---------------------------- |
| `NODEDB_MEMORY_LIMIT`     | 1 GiB      | e.g. `4GiB`                  |
| `NODEDB_DATA_PLANE_CORES` | CPUs - 1   | number of Data Plane threads |
| `NODEDB_LOG_FORMAT`       | `text`     | `text` or `json`             |

Set them under `environment:` in `docker-compose.yml` or pass with `-e` to `docker run`.

---

## Run a prebuilt binary (Linux)

Each tagged release ships a static `nodedb` tarball on GitHub for `linux-x64` and `linux-arm64`. macOS and Windows users run Docker until those targets ship.

```bash
# Resolve the latest tag and your architecture
TAG=$(curl -fsSL https://api.github.com/repos/NodeDB-Lab/nodedb/releases/latest \
        | grep '"tag_name"' | cut -d'"' -f4)
ARCH=$(uname -m | sed 's/aarch64/arm64/; s/x86_64/x64/')

# Download and extract
curl -L -o nodedb.tar.gz \
  "https://github.com/NodeDB-Lab/nodedb/releases/download/${TAG}/nodedb-${TAG#v}-linux-${ARCH}.tar.gz"
tar -xzf nodedb.tar.gz

# Optional: install system-wide
sudo mv nodedb /usr/local/bin/

# Run with all defaults (data goes to ~/.nodedb/data)
nodedb
```

If you have the [GitHub CLI](https://cli.github.com/) installed, this is one command:

```bash
gh release download --repo NodeDB-Lab/nodedb --pattern 'nodedb-*-linux-x64.tar.gz' \
  && tar -xzf nodedb-*-linux-x64.tar.gz
```

To run with a config file or a custom data directory:

```bash
# Point at an explicit data dir
NODEDB_DATA_DIR=/var/lib/nodedb nodedb

# Or load a config file (env vars still override TOML keys)
nodedb --config /etc/nodedb/nodedb.toml
```

For a long-running server, drop a unit file at `/etc/systemd/system/nodedb.service`:

```ini
[Unit]
Description=NodeDB
After=network.target

[Service]
Type=simple
User=nodedb
Group=nodedb
ExecStart=/usr/local/bin/nodedb --config /etc/nodedb/nodedb.toml
Restart=on-failure
LimitNOFILE=1048576

[Install]
WantedBy=multi-user.target
```

Then `sudo systemctl enable --now nodedb`. The user/group must be able to read the config file and write `data_dir`.

> For a specific version or to browse changelogs, see the release page: <https://github.com/NodeDB-Lab/nodedb/releases>. The SQL surface is still pre-1.0 and changes between tags, so pin a version in production.

---

## Build from Source

```bash
git clone https://github.com/NodeDB-Lab/nodedb.git
cd nodedb

# Release build (all crates)
cargo build --release

# Run tests (use nextest — see .config/nextest.toml)
cargo install cargo-nextest --locked  # one-time
cargo nextest run --all-features
```

Requires Rust 1.94+ and Linux (the Data Plane uses io_uring). The build produces two binaries:

- `target/release/nodedb` — the database server
- `target/release/ndb` — the terminal client

## Start the Server (from source)

```bash
# Single-node, default ports
./target/release/nodedb

# Or with a config file
./target/release/nodedb --config nodedb.toml
```

---

## Configuration

This section applies to **every** install method. Docker, prebuilt binary, and source builds read the same TOML schema and the same environment variables. Pick whichever is convenient:

- **TOML file** — pass `--config /path/to/nodedb.toml` on the command line. Best for production / systemd / pre-baked images. A value can read the environment with `${VAR}` — see [Placeholders in the config file](#placeholders-in-the-config-file).
- **Environment variables** — prefix `NODEDB_*`. Best for Docker (`-e`), Compose (`environment:`), and Kubernetes. Env vars **override** values from the TOML file when both are set.

A value NodeDB cannot apply stops startup. See [Startup validation](#startup-validation).

### Default ports

By default, NodeDB listens on:

- **6432** — PostgreSQL wire protocol (pgwire)
- **6433** — Native MessagePack protocol
- **6480** — HTTP API (REST, SSE, WebSocket)
- **9090** — WebSocket sync (NodeDB-Lite clients). Loopback only.

Two additional protocols are available but **disabled by default**:

- **RESP** (Redis-compatible KV protocol) — `GET`/`SET`/`DEL`/`EXPIRE`/`SCAN`/`SUBSCRIBE`
- **ILP** (InfluxDB Line Protocol) — high-throughput timeseries ingest

Enable them by setting a listen address in the config or via env var (see below).

### Example config file

All protocols share one bind address (`host`). Only the port differs per protocol.

```toml
# nodedb.toml
[server]
host = "127.0.0.1"               # Shared bind address (use 0.0.0.0 for all interfaces)
data_dir = "/var/lib/nodedb"
memory_limit = "4GiB"
data_plane_cores = 4
max_connections = 1024
log_format = "text"               # "text" or "json"

[server.ports]
native = 6433                     # Always-on protocols have defaults
pgwire = 6432
http = 6480
sync = 9090                       # Always on
resp = 6381                       # Optional: set to enable, omit to disable
ilp = 8086                        # Optional: set to enable, omit to disable

[server.tls]
cert_path = "/etc/nodedb/tls/server.crt"
key_path = "/etc/nodedb/tls/server.key"
native = true                     # Per-protocol TLS toggle (all default true)
pgwire = true
http = true
resp = true
ilp = false                       # Example: disable TLS for ILP ingest
```

Every listener binds during startup, before the server accepts any
connection. If a configured port is already in use, startup fails with that
port named — the server never comes up missing a protocol.

**Server settings:**

| Config field       | Environment variable      | Default                                               |
| ------------------ | ------------------------- | ----------------------------------------------------- |
| `host`             | `NODEDB_HOST`             | `127.0.0.1`                                           |
| `sync_host`        | `NODEDB_SYNC_HOST`        | follows `host` when `host` is loopback                |
| `ports.native`     | `NODEDB_PORT_NATIVE`      | `6433`                                                |
| `ports.pgwire`     | `NODEDB_PORT_PGWIRE`      | `6432`                                                |
| `ports.http`       | `NODEDB_PORT_HTTP`        | `6480`                                                |
| `ports.sync`       | `NODEDB_PORT_SYNC`        | `9090`                                                |
| `ports.resp`       | `NODEDB_PORT_RESP`        | disabled                                              |
| `ports.ilp`        | `NODEDB_PORT_ILP`         | disabled                                              |
| `data_dir`         | `NODEDB_DATA_DIR`         | `~/.nodedb/data` (binary), `/var/lib/nodedb` (Docker) |
| `memory_limit`     | `NODEDB_MEMORY_LIMIT`     | `1 GiB`                                               |
| `data_plane_cores` | `NODEDB_DATA_PLANE_CORES` | CPUs - 1                                              |
| `max_connections`  | `NODEDB_MAX_CONNECTIONS`  | `4096`                                                |
| `log_format`       | `NODEDB_LOG_FORMAT`       | `text`                                                |

**TLS certificate material:**

| Config field    | Environment variable     | Default |
| --------------- | ------------------------ | ------- |
| `tls.cert_path` | `NODEDB_TLS_CERT_PATH`   | none    |
| `tls.key_path`  | `NODEDB_TLS_KEY_PATH`    | none    |

Setting both creates a `[server.tls]` section when the config file has none.
Every protocol then starts with TLS on. Setting one without the other stops
startup.

**Per-protocol TLS** (only applies when `[server.tls]` is configured):

| Config field | Environment variable | Default |
| ------------ | -------------------- | ------- |
| `tls.native` | `NODEDB_TLS_NATIVE`  | `true`  |
| `tls.pgwire` | `NODEDB_TLS_PGWIRE`  | `true`  |
| `tls.http`   | `NODEDB_TLS_HTTP`    | `true`  |
| `tls.resp`   | `NODEDB_TLS_RESP`    | `true`  |
| `tls.ilp`    | `NODEDB_TLS_ILP`     | `true`  |

Every toggle on this page accepts `true`, `1`, `yes`, `false`, `0`, and `no`,
in any case. Turning a listener on without certificate material stops startup.
Turning one off with no `[server.tls]` section is accepted, because the
listener is already plaintext.

**Checkpoint & WAL settings:**

| Config field                    | Environment variable              | Default |
| -------------------------------- | ---------------------------------- | ------- |
| `checkpoint.interval_secs`       | `NODEDB_CHECKPOINT_INTERVAL_SECS`  | `300`   |
| `checkpoint.wal_segment_target_mb` | `NODEDB_WAL_SEGMENT_TARGET_MB`   | `64`    |
| `checkpoint.wal_archive_interval_secs` | none                         | `10`    |
| `tuning.wal.direct_io`           | `NODEDB_WAL_DIRECT_IO`             | `true`  |
| `tuning.wal.write_buffer_size`   | `NODEDB_WAL_WRITE_BUFFER_SIZE`     | `2MiB`  |

All intervals must be positive. A zero interval stops startup.
`write_buffer_size` accepts a memory size and must be at least `64KiB`. Turn
`direct_io` off only on a filesystem that rejects `O_DIRECT`.

`wal_archive_interval_secs` applies only when `[cold_storage]` is configured.
Each interval, every sealed WAL segment the archive does not hold is uploaded
to cold storage. A sealed segment therefore reaches the archive within one
interval. The segment still being written is uploaded once it is sealed.
Checkpoints never delete a WAL segment the archive does not hold.

**Point-in-time recovery:**

| Config field                       | Environment variable | Default |
| ---------------------------------- | -------------------- | ------- |
| `pitr.enabled`                     | none                 | `false` |
| `pitr.base_snapshot_interval_secs` | none                 | `86400` |
| `pitr.base_snapshot_retention`     | none                 | `7`     |
| `pitr.restore_point_interval_secs` | none                 | `0`     |

```toml
[pitr]
enabled = true
base_snapshot_interval_secs = 86400
base_snapshot_retention = 7
restore_point_interval_secs = 3600

[cold_storage]
bucket = "my-nodedb-cold"

[encryption]
key_path = "/etc/nodedb/keys/wal.key"
```

With `pitr.enabled = true`, startup stops unless `[cold_storage]` is
configured and opens, and unless `[encryption]` is configured. The WAL archive
is the only copy of a segment once a checkpoint deletes it locally. Base
snapshots are encrypted with the WAL key. With `pitr.enabled = false` and no
`[cold_storage]`, checkpoints delete WAL segments without archiving them.

Archived segments are stored under `{prefix}wal/{node_id}/{incarnation}/`.
The incarnation is a random id stored in the data directory. A node whose
data directory is wiped gets a new incarnation, so it never mixes its
segments with those of its earlier life.

Each `base_snapshot_interval_secs`, the node takes a base snapshot of every
Data Plane core and of its catalogs. Bases are stored in `[snapshot_storage]`
under `{node_id}/{incarnation}/`. The first base is taken once startup
completes when the node has none. A base must finish within one interval.

Bases are incremental. Each image is cut into chunks of 256 KiB to 4 MiB at
content-defined boundaries. A chunk is stored once, under `chunks/{id}` in
the same directory. The id is an HMAC of the chunk content under a key
derived from the WAL key, so it reveals nothing about the content. A new base
uploads only the chunks the store lacks. Every base manifest still lists
every chunk it needs, so a restore reads one base and never its parent.

After each base, the node keeps the newest `base_snapshot_retention` bases
and deletes older ones. It never deletes the base it just took. It then
deletes archived WAL segments whose records all lie below the oldest kept
base, so every kept base can replay forward. The segment holding that base's
start is always kept. It also deletes every chunk no kept base lists. A chunk
a base still being written relies on is never deleted. Both values must be
positive. A zero value stops startup.

A failed run is retried at the next interval. These metrics report the task:

- `nodedb_pitr_base_last_success_timestamp_seconds`
- `nodedb_pitr_base_snapshots`
- `nodedb_pitr_base_last_failure_timestamp_seconds`
- `nodedb_pitr_base_failures_total`
- `nodedb_pitr_wal_segments_collected_total`
- `nodedb_pitr_base_chunks_uploaded_total`
- `nodedb_pitr_base_chunk_bytes_uploaded_total`
- `nodedb_pitr_base_chunks_reused_total`
- `nodedb_pitr_base_chunks_collected_total`

**Restoring one node:**

```bash
nodedb restore --config /etc/nodedb/nodedb.toml --target-time 2026-09-01T12:00:00Z --dry-run
nodedb restore --config /etc/nodedb/nodedb.toml --target-lsn 48213
```

The restore runs offline, with the server stopped. It writes the newest base
at or below the target, plus the archived WAL cut at the target, into the
data directory of the config. The data directory must be empty. A time
target is RFC 3339 or epoch seconds, milliseconds or microseconds.
`--dry-run` prints the plan and writes nothing. `--incarnation` picks the
node life when several hold bases. Start the server afterwards. It needs no
flag.

**Cluster restore points:**

A cluster rewinds only to a restore point: one instant every Raft group of
the cluster agrees on.

```sql
CREATE RESTORE POINT;
SHOW RESTORE POINTS;
```

- `CREATE RESTORE POINT` takes a point now and returns its row.
- `SHOW RESTORE POINTS` lists every point, oldest first.
- Both are superuser only, and both refuse on a node outside a cluster.
- A row holds `id`, `hlc` (the point's watermark, HLC nanoseconds) and
  `created_at_ms`.

With `restore_point_interval_secs` above `0`, the cluster also takes a point
each interval. A point cuts every data group and the Calvin sequencer at its
watermark. Each node records every group's place at the point in its WAL.
Every write a group's log places after its cut records a commit time above
the watermark, on every replica.

Each node also copies the metadata group's committed log to
`{prefix}raft/{node_id}/{incarnation}/` in cold storage. The metadata log
never compacts past the entries that copy holds.

**Restoring a cluster:**

1. Pick the point with `SHOW RESTORE POINTS`.
2. Stop every node of the cluster.
3. On every node, empty the data directory except `tls/`.
4. On every node, run the restore:

   ```bash
   nodedb restore --config /etc/nodedb/nodedb.toml --cluster --restore-point 5812
   ```

5. Start every node.

The restore needs the point in the node's archived WAL. After a node records
a point, it seals the WAL segment that holds the records, so they reach the
archive within one `wal_archive_interval_secs`. A point taken less than one
interval before the stop can be missing from the archive. The restore then
refuses and names the node and the point.

Each node's restore writes:

- The newest base holding nothing the restore drops, in data or metadata.
- The archived WAL through its last kept record. Each record is kept or
  dropped by one rule:
  - A write keeps its commit HLC. It is kept when that HLC is below the
    watermark, wherever the WAL placed it.
  - A record that a metadata entry's apply appended carries the entry's
    stamp, and follows the entry.
  - Any other record (a checkpoint, a tombstone) carries no clock. It is
    kept when it lies before this node's cut of its vShard's group in the
    WAL, or before the metadata group's cut when no group homes the vShard.
- Each data group and the sequencer, started at its place at the point. The
  sequencer resumes at the first epoch after the point.
- The metadata group, started at the base's catalogs, with the archived
  metadata log entries after the base, through the point. The first boot
  applies them. A range this node's archive lacks is read from another
  node's archive.

Every metadata entry a node proposes carries the node's HLC. The restore
keeps an entry only when that stamp is below the watermark, so the catalog
matches the data exactly. A DDL issued after the watermark is dropped even
when it applied before the point's entry, and so is every write that
depends on it. A dropped entry keeps its index as an empty entry.

The restore also sets two counters at the point:

- The surrogate high-water mark rises to the highest surrogate any node
  ever reserved, after the point too. No surrogate is issued twice.
- Each tenant write mark is the newest kept write. A mark the base holds at
  or above the watermark stops just below it.

The generation claim needs a cold store with conditional create (put if
absent). A store without one refuses the restore and names the store.

Every group of every node starts at one new term, and the cluster epoch
starts at the same value. The value comes from a generation the restore
claims in cold storage under `{prefix}raft/restore/`. Every node restoring
the same point before the cluster starts takes the same generation. Each
node's first boot seals the generation, and the next restore takes a newer
one. A node the restore missed holds lower terms and a lower epoch. Raft
refuses its log, and the epoch fence stands it down. A restored data
directory holds the file `restore_generation` until its first boot. That
boot needs `[cold_storage]` to seal the generation.

A restore that fails empties the data directory except `tls/`. Run it
again. A group a node hosts that recorded no place at the point starts with
no log and catches up from its leader. The report lists such groups.

**Scheduled backups:**

| Config field               | Environment variable | Default |
| -------------------------- | -------------------- | ------- |
| `backup.schedule.database` | none                 | none    |
| `backup.schedule.target`   | none                 | none    |
| `backup.schedule.cron`     | none                 | none    |
| `backup.schedule.keep`     | none                 | none    |

```toml
[[backup.schedule]]
database = "sales"
target = "s3://my-backups/nightly/sales"
cron = "0 3 * * *"
keep = 7

[backup_encryption]
key_path = "/etc/nodedb/keys/backup.key"
```

Each `[[backup.schedule]]` entry runs `BACKUP DATABASE <database>` on its
cron schedule. The cron is 5-field and uses `scheduler.cron_timezone`. A run
writes one envelope named `<database>-<unix_ms>.ndbb` under `target`.
`<unix_ms>` is the scheduled minute, not the time the run started. The run
then deletes the oldest envelopes of that database under `target` beyond
`keep`. Other objects under `target` are never deleted.

- `target` is `s3://<bucket>/<prefix>` or `file:///<dir>`. It resolves
  against `[backup_storage]`, like a `BACKUP DATABASE ... TO` URI.
- A `file://` target must name a directory inside `[backup_storage]
  local_root`.
- Every field is required. `keep` must be positive.
- Two entries with the same `database` and `target` stop startup.
- A schedule needs `[backup_encryption]`. Without it, startup stops.

In a cluster, only the leader of vShard 0 runs scheduled backups, and only
while its leader lease is valid. A leader cut off from its peers stops once
its lease lapses, before another node can take over. The lease is checked
again before the envelope write and before the record below. After each
completed run, the leader records the scheduled minute through the metadata
group, so every node holds the same record. A node reads that record only
after it has applied the metadata group through a read index its leader
confirmed. When that read cannot be confirmed, the tick is skipped. A new
leader of vShard 0 runs any due minute with no completed record. A minute that runs twice across a leader
change writes the same envelope again. Several missed minutes fire one run.
A new entry, or an entry whose `database`, `target`, or `cron` changes,
starts fresh from the current minute. A failed run retries after 60 seconds.
A run never overlaps an earlier run of the same entry. Each run is recorded
in the job history under `backup:<database>:<target>`. These metrics report
the runs:

- `nodedb_backup_schedule_runs_total`
- `nodedb_backup_schedule_failures_total`
- `nodedb_backup_schedule_last_success_timestamp_seconds`
- `nodedb_backup_schedule_last_failure_timestamp_seconds`
- `nodedb_backup_schedule_envelopes_deleted_total`
- `nodedb_backup_schedule_ticks_skipped_total`

**Timeseries memtable settings:**

| Config field                              | Environment variable                   | Default |
| ----------------------------------------- | -------------------------------------- | ------- |
| `tuning.timeseries.memtable_budget_bytes` | `NODEDB_TS_MEMTABLE_BUDGET_BYTES`      | `67108864` (64 MiB) |
| `tuning.timeseries.memtable_hard_limit_bytes` | `NODEDB_TS_MEMTABLE_HARD_LIMIT_BYTES` | `83886080` (80 MiB) |
| `tuning.timeseries.max_tag_cardinality`   | `NODEDB_TS_MAX_TAG_CARDINALITY`        | `100000` |

`memtable_budget_bytes` is the soft budget that schedules a flush.
`memtable_hard_limit_bytes` is the ceiling that forces one before the next
write. A single write always applies whole, so it can carry the memtable past
the hard limit by its own size. The next flush then drains it.
`max_tag_cardinality` bounds the distinct values a text/tag column holds
between flushes.

**Cluster settings** (each needs a `[cluster]` section in the config file):

| Config field                          | Environment variable                 | Default                     |
| ------------------------------------- | ------------------------------------ | --------------------------- |
| `cluster.node_id`                     | `NODEDB_NODE_ID`                     | none                        |
| `cluster.seed_nodes`                  | `NODEDB_SEED_NODES`                  | none                        |
| `cluster.swim_listen`                 | `NODEDB_SWIM_LISTEN`                 | `cluster.listen` port + 1   |
| `cluster.join_retry_max_attempts`     | `NODEDB_JOIN_RETRY_MAX_ATTEMPTS`     | `8`                         |
| `cluster.join_retry_max_backoff_secs` | `NODEDB_JOIN_RETRY_MAX_BACKOFF_SECS` | `32`                        |

`NODEDB_SEED_NODES` takes a comma-separated `host:port` list. Both join-retry
values must be positive. Setting any of these without a `[cluster]` section
stops startup.

`cluster.swim_listen` is the UDP `host:port` of the SWIM failure detector.
By default it uses the `cluster.listen` IP, one port above the `cluster.listen`
port. Each node advertises its bound address to its peers, so nodes can set it
independently. Startup fails if the address cannot be bound. Open this UDP port
between every pair of nodes. Without it, nodes cannot detect a failed peer, and
a crashed node's descriptor leases block DDL until they expire.

**Maintenance loop settings:**

| Config field                                          | Environment variable                      | Default |
| ----------------------------------------------------- | ----------------------------------------- | ------- |
| `tuning.maintenance.clone_sweep_interval_ms`          | `NODEDB_CLONE_SWEEP_INTERVAL_MS`          | `30000` |
| `tuning.maintenance.constraint_reconcile_interval_ms` | `NODEDB_CONSTRAINT_RECONCILE_INTERVAL_MS` | `1000`  |
| `tuning.maintenance.scope_expiry_interval_secs`       | `NODEDB_SCOPE_EXPIRY_INTERVAL_SECS`       | `60`    |

All three must be positive. `scope_expiry_interval_secs` has a floor of `10`.
Below that the sweep costs more than the resolution it buys.

**Startup bounds:**

Boot waits for the metadata raft group to apply its first entry, and for the
locally hosted data raft groups to replay their retained logs. Both waits are
bounded, and both are configurable. The defaults are sized for a cold restart of
a data dir that ran for weeks. The backlog takes minutes to apply, not seconds.

| Config field                                    | Default  |
| ----------------------------------------------- | -------- |
| `tuning.startup.raft_ready_timeout_ms`          | `300000` |
| `tuning.startup.data_group_recovery_timeout_ms` | `600000` |

The metadata bound resets on every applied-index advance, so a large replay
finishes. Only a group that applies nothing fails. The data-group recovery bound
is a hard deadline. It does not reset, and a group still recovering when it
expires fails the boot. Both accept `1` to `86400000` ms (one day). A value
outside that range is rejected at load with the key, the value and the accepted
range named. A bound written under `[server]` is rejected with the path that is
read named instead. A key under `[tuning.startup]` that is not in the table is
rejected too. Two other boot waits keep fixed bounds and do not read this
section: Data Plane WAL replay (300 s) and the first authorization lease (30 s).

**Observability settings:**

| Config field                                      | Environment variable              | Default        |
| ------------------------------------------------- | --------------------------------- | -------------- |
| `observability.promql.enabled`                    | `NODEDB_PROMQL_ENABLED`           | `true`         |
| `observability.otlp.receiver.enabled`             | `NODEDB_OTLP_RECEIVER_ENABLED`    | `false`        |
| `observability.otlp.receiver.http_listen`         | `NODEDB_OTLP_HTTP_LISTEN`         | `0.0.0.0:4318` |
| `observability.otlp.receiver.grpc_listen`         | `NODEDB_OTLP_GRPC_LISTEN`         | `0.0.0.0:4317` |
| `observability.otlp.export.enabled`               | `NODEDB_OTLP_EXPORT_ENABLED`      | `false`        |
| `observability.otlp.export.endpoint`              | `NODEDB_OTLP_EXPORT_ENDPOINT`     | none           |
| `observability.otlp.export.metrics_interval_secs` | `NODEDB_OTLP_EXPORT_INTERVAL`     | `15`           |
| `observability.debug_endpoints_enabled`           | `NODEDB_DEBUG_ENDPOINTS_ENABLED`  | `false`        |

`NODEDB_OTLP_EXPORT_ENDPOINT` takes an `http://` or `https://` URL. The debug
endpoints expose raft internals, so they stay off until you enable them.

### Startup validation

A `NODEDB_*` value NodeDB cannot apply stops startup. The server reports every
bad value at once and exits non-zero.

Three kinds fail:

- **Unparseable** — `NODEDB_DATA_PLANE_CORES=abc`.
- **Out of domain** — a zero core count, or a WAL buffer under `64KiB`.
- **Unsatisfiable here** — TLS on with no certificate material, or
  `NODEDB_NODE_ID` with no `[cluster]` section.

```
Error: configuration error: invalid value 'abc' for NODEDB_DATA_PLANE_CORES: expected a positive integer; invalid value '4096' for NODEDB_WAL_WRITE_BUFFER_SIZE: expected a memory size of at least 64KiB
```

A request the server already satisfies is honoured. `NODEDB_TLS_PGWIRE=false`
with no `[server.tls]` section is accepted, because that listener is already
plaintext.

An empty value is always a violation. `NODEDB_DATA_DIR=` is a failed template
substitution, not a request.

### Placeholders in the config file

A config value can read the environment with `${VAR}`. NodeDB substitutes
before it parses the TOML.

```toml
[server]
data_dir = "${DATA_DIR}"

[server.ports]
pgwire = ${PGWIRE_PORT}
```

```yaml
services:
  nodedb:
    environment:
      DATA_DIR: /var/lib/nodedb
      PGWIRE_PORT: "6432"
```

- **Quoting is yours** — substitution is textual. Quote for a string, leave
  bare for a number or a bool.
- **Escape** — `$${NAME}` produces the literal `${NAME}` and reads nothing.
- **Comments are skipped** — a commented-out example never requires its
  variable.
- **Names** match `[A-Za-z_][A-Za-z0-9_]*`.
- **Unset fails** — the error names the file and the variable.
- **No shell syntax** — no command substitution, no recursion, no
  `${VAR:-default}`.
- **Never logged** — NodeDB logs the variable name, never the value.

An env override still wins over an expanded value for the same field.

## Connect

### With the `ndb` CLI

```bash
./target/release/ndb
```

You get a full TUI with syntax highlighting, tab completion, and history search. See the [CLI guide](cli.md) for details.

### With psql

```bash
psql -h localhost -p 6432
```

NodeDB speaks PostgreSQL's wire protocol, so standard tools like `psql`, ORMs, and BI tools work out of the box.

### With the Rust SDK or FFI

The `nodedb-client` crate connects over the NDB protocol (port 6433). One connection carries both SQL and native modes:

```rust
// SQL — same as psql/HTTP, full query support
let rows = client.sql("SELECT * FROM users WHERE age > 30").await?;

// Native — typed methods, skip SQL parsing for hot paths
let user = client.get("users", "u1").await?;
client.put("users", "u1", &doc).await?;
```

Use SQL for complex queries and rapid prototyping. Use native methods for high-throughput CRUD and ingest where parsing overhead matters. The same dual-mode access is available via `nodedb-lite-ffi` (iOS/Android) and `nodedb-lite-wasm` (WASM/browser).

## First Queries

### Documents (schemaless)

```sql
-- Create a schemaless collection
CREATE COLLECTION users;

-- Insert some data (standard SQL)
INSERT INTO users (id, name, email, age) VALUES ('u1', 'Alice', 'alice@example.com', 30);
INSERT INTO users (id, name, email, role) VALUES ('u2', 'Bob', 'bob@example.com', 'admin');

-- Object literal syntax also works
INSERT INTO users { name: 'Alice', email: 'alice@example.com', age: 30 };
INSERT INTO users { name: 'Bob', email: 'bob@example.com', role: 'admin' };

-- Query
SELECT * FROM users WHERE name = 'Alice';
SELECT name, email FROM users WHERE age > 25;
```

### Strict Documents (schema-enforced)

```sql
-- Create a strict collection with a defined schema
CREATE COLLECTION orders (
    id TEXT PRIMARY KEY,
    customer_id TEXT,
    total FLOAT,
    status TEXT,
    created_at TIMESTAMP
) WITH (engine='document_strict');

INSERT INTO orders (id, customer_id, total, status)
VALUES ('o1', 'u1', 99.99, 'pending');

SELECT * FROM orders WHERE status = 'pending' ORDER BY created_at DESC;
```

### Vector Search

```sql
-- Create a collection with a vector index
CREATE COLLECTION articles;
CREATE VECTOR INDEX idx_articles_embedding ON articles METRIC cosine DIM 384;

-- Insert documents with embeddings (standard SQL)
INSERT INTO articles (id, title, embedding) VALUES ('a1', 'Intro to AI', ARRAY[0.1, 0.2, 0.3]);
-- Or:
INSERT INTO articles { id: 'a1', title: 'Intro to AI', embedding: [0.1, 0.2, 0.3] };

-- Search by similarity
SEARCH articles USING VECTOR(embedding, ARRAY[0.1, 0.3, ...], 10);
```

### Graph

```sql
-- Graph is an overlay on document collections, not a separate collection type
CREATE COLLECTION social;

-- Insert nodes
INSERT INTO social (id, name) VALUES ('alice', 'Alice');
INSERT INTO social (id, name) VALUES ('bob', 'Bob');

-- Add edges
-- JSON string form:
GRAPH INSERT EDGE FROM 'alice' TO 'bob' TYPE 'knows' PROPERTIES '{"since": 2020}';
-- Object literal form (equivalent):
GRAPH INSERT EDGE FROM 'alice' TO 'bob' TYPE 'knows' PROPERTIES { since: 2020 };

-- Traverse
GRAPH TRAVERSE FROM 'alice' DEPTH 2;

-- Run an algorithm
GRAPH ALGO PAGERANK ON social DAMPING 0.85 ITERATIONS 20 TOLERANCE 1e-7;
```

### Key-Value

```sql
-- Create a KV collection
CREATE COLLECTION sessions (key TEXT PRIMARY KEY) WITH (engine='kv');

-- Set a key-value pair (standard SQL)
INSERT INTO sessions (key, value) VALUES ('sess_abc', 'token-abc');
-- Or:
INSERT INTO sessions { key: 'sess_abc', value: 'token-abc' };

-- Get by key
SELECT * FROM sessions WHERE key = 'sess_abc';
```

### Columnar (Analytics)

Columnar storage compresses by column with block-level skip. Three peer engines share the same storage core: `columnar` (general analytics), `timeseries` (append-only time data), and `spatial` (geo-primary).

```sql
-- Plain columnar: general analytics
CREATE COLLECTION web_events (
    ts TIMESTAMP,
    user_id UUID,
    page VARCHAR,
    duration_ms INT
) WITH (engine='columnar');

SELECT page, AVG(duration_ms), COUNT(*)
FROM web_events
WHERE ts > now() - INTERVAL '7 days'
GROUP BY page
ORDER BY COUNT(*) DESC;

-- Timeseries: TIME_KEY column drives partition-by-time and block skip
CREATE COLLECTION cpu_metrics (
    ts TIMESTAMP TIME_KEY,
    host VARCHAR,
    cpu FLOAT
) WITH (engine='timeseries', partition_by='1h');

-- CREATE TIMESERIES is a convenience alias equivalent to engine='timeseries'
-- CREATE TIMESERIES cpu_metrics;

SELECT time_bucket('5 minutes', ts) AS bucket, host, AVG(cpu)
FROM cpu_metrics
WHERE ts > now() - INTERVAL '1 hour'
GROUP BY bucket, host;

-- Spatial: SPATIAL_INDEX column gets an automatic R*-tree
CREATE COLLECTION locations (
    geom GEOMETRY SPATIAL_INDEX,
    name VARCHAR
) WITH (engine='spatial');

SELECT name, ST_Distance(geom, ST_Point(-73.98, 40.75)) AS dist
FROM locations
WHERE ST_DWithin(geom, ST_Point(-73.98, 40.75), 1000)
ORDER BY dist;
```

### Triggers

```sql
-- Fire asynchronously after each insert (default, zero write-latency impact)
CREATE TRIGGER notify_on_order AFTER INSERT ON orders FOR EACH ROW $$
BEGIN
    INSERT INTO notifications (id, user_id, message)
    VALUES (NEW.id || '_notif', NEW.customer_id, 'Order received');
END;
$$;

-- Fire synchronously in the same transaction (ACID, adds trigger latency to writes)
CREATE SYNC TRIGGER enforce_balance AFTER UPDATE ON accounts FOR EACH ROW $$
BEGIN
    IF NEW.balance < 0 THEN
        RAISE EXCEPTION 'Balance cannot go negative';
    END IF;
END;
$$;
```

### User-Defined Functions

```sql
-- SQL expression function (inlined into query plans by the optimizer)
CREATE FUNCTION full_name(first VARCHAR, last VARCHAR) RETURNS VARCHAR
LANGUAGE SQL IMMUTABLE AS $$ first || ' ' || last $$;

-- Use in queries
SELECT full_name(first_name, last_name) AS name FROM users;

-- Procedural function with BEGIN...END body
CREATE FUNCTION tier_label(amount DECIMAL) RETURNS VARCHAR
LANGUAGE SQL STABLE
BEGIN
    IF amount > 1000 THEN
        RETURN 'premium';
    ELSIF amount > 100 THEN
        RETURN 'standard';
    ELSE
        RETURN 'basic';
    END IF;
END;
```

### Change Streams

```sql
-- Create a change stream with webhook delivery
CREATE CHANGE STREAM order_changes
ON orders
WITH (
    WEBHOOK_URL = 'https://hooks.example.com/orders',
    WEBHOOK_SECRET = 'whsec_abc123'
);

-- Create a consumer group to track read offsets
CREATE CONSUMER GROUP processors ON order_changes;

-- Commit offsets after processing (batch: all partitions at latest)
COMMIT OFFSETS ON order_changes CONSUMER GROUP processors;

-- Show all streams
SHOW CHANGE STREAMS;
```

### Backup, Restore, and Purge

```sql
-- Backup all tenant data across all engines (encrypted with AES-256-GCM).
-- Bytes stream over the pgwire COPY framing; the client redirects to disk.
COPY (BACKUP TENANT acme) TO STDOUT;

-- Validate a backup without restoring
COPY tenant_restore(acme) FROM STDIN DRY RUN;

-- Restore
COPY tenant_restore(acme) FROM STDIN;

-- Permanently delete all tenant data (GDPR erasure) — requires CONFIRM
PURGE TENANT acme CONFIRM;

-- Inspect resource usage and limits
SHOW TENANT USAGE FOR acme IN DATABASE prod;
SHOW TENANT QUOTA FOR acme IN DATABASE prod;
```

## What's Next

- [Architecture](architecture.md) — understand how the three-plane execution model works
- Engine deep dives: [Vectors](vectors.md) | [Graph](graph.md) | [Documents](documents.md) | [Columnar](columnar.md) | [Timeseries](timeseries.md) | [Spatial](spatial.md) | [KV](kv.md) | [Full-Text](full-text-search.md)
- [NodeDB-Lite](lite.md) — embed NodeDB in your app (mobile, WASM, desktop)
- [Security](security/README.md) — set up authentication and access control

[Back to docs](README.md)
