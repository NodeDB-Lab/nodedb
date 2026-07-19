// SPDX-License-Identifier: BUSL-1.1

//! Per-connection session state types.

use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::sync::atomic::{AtomicU32, AtomicU64};
use std::time::{SystemTime, UNIX_EPOCH};

/// Milliseconds since the Unix epoch. Falls back to `0` instead of panicking
/// if the system clock is set before the epoch (`duration_since` errors).
pub(crate) fn now_unix_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
}

use crate::types::{DatabaseId, Lsn, TenantId, TxnId, VShardId};
use nodedb_physical::physical_task::PhysicalTask;

/// One entry on the transaction's savepoint stack.
///
/// A savepoint captures the write buffer length AND, for each vShard that had
/// staged writes when the savepoint was established, that vShard's value/TTL and
/// graph overlay undo-journal markers. On ROLLBACK TO, the buffer is truncated
/// to `buffer_len` and every currently-staged vShard's overlays are rewound —
/// to its saved marker if present, else to `(0, 0)` (a vShard first staged
/// AFTER the savepoint must have ALL of its staged writes rewound).
pub struct SavepointEntry {
    /// User-visible savepoint name.
    pub name: String,
    /// `tx_buffer` length captured when the savepoint was established.
    pub buffer_len: usize,
    /// Per-vShard `(value_marker, graph_marker)` overlay journal markers.
    pub markers: BTreeMap<VShardId, (usize, usize)>,
}

/// PostgreSQL transaction state for ReadyForQuery status byte.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TransactionState {
    /// 'I' — not in a transaction block.
    Idle,
    /// 'T' — in a transaction block (after BEGIN).
    InBlock,
    /// 'E' — in a failed transaction block (error occurred after BEGIN).
    Failed,
}

/// Server-side cursor state.
pub struct CursorState {
    /// Pre-fetched result rows as JSON strings.
    pub rows: Vec<String>,
    /// Current position (next row to return).
    pub position: usize,
    /// Whether this cursor supports backward fetching (SCROLL).
    pub scrollable: bool,
    /// Whether this cursor survives transaction commit (WITH HOLD).
    pub with_hold: bool,
}

/// Per-connection session state.
pub struct ConnSession {
    pub tx_state: TransactionState,
    /// Database bound to this connection session.
    ///
    /// Set at startup from the `database` parameter in the PostgreSQL StartupMessage
    /// (i.e. `psql -d <name>` or `dbname=<name>` in the connection string). If the
    /// client sends no database parameter, falls back to the resolution chain:
    /// user-default → tenant-default → `DatabaseId::DEFAULT` ("default").
    ///
    /// Mutable only via `USE DATABASE <name>`, which issues a full session reset.
    pub current_database: Option<DatabaseId>,
    /// Per-session tenant override applied only to superuser connections.
    ///
    /// `None` means queries route to the identity-bound tenant
    /// (`AuthenticatedIdentity::tenant_id`). When set — only ever by
    /// `SET TENANT = '<name>' | <id> | DEFAULT` / `SET nodedb.tenant_id = <id>`
    /// from a superuser session — `resolve_identity` overlays this value onto
    /// the resolved identity for every subsequent request on the connection.
    /// Cleared by `RESET TENANT`, `SET TENANT = DEFAULT`, or `DISCARD ALL`.
    ///
    /// Non-superuser sessions never carry an override (the SET handler rejects
    /// with `42501` before this field is written), so the identity-bound
    /// invariant continues to hold for tenant-scoped users.
    pub effective_tenant_id: Option<TenantId>,
    /// Authenticated identity resolved for queries on this connection.
    ///
    /// Stashed by `resolve_identity` (the per-query auth chokepoint) so that a
    /// connection torn down mid-transaction can reclaim its Data-Plane staging
    /// overlays without a live query in flight — `run_rollback` requires the
    /// identity (tenant + username) to dispatch `MetaOp::DropTxnOverlay` and to
    /// audit any GAP_FREE reservation rollback. `None` until the first query
    /// resolves an identity on this connection.
    pub identity: Option<crate::control::security::identity::AuthenticatedIdentity>,
    /// Session parameters set via SET commands.
    pub parameters: HashMap<String, String>,
    /// Buffered write tasks accumulated between BEGIN and COMMIT.
    /// Dispatched atomically on COMMIT, discarded on ROLLBACK.
    pub tx_buffer: Vec<PhysicalTask>,
    /// Snapshot LSN captured at BEGIN for snapshot isolation.
    /// All reads within the transaction see data as of this LSN.
    /// Concurrent writes after this point are invisible to the transaction.
    pub tx_snapshot_lsn: Option<Lsn>,
    /// Snapshot epoch captured at BEGIN: the last globally-applied Calvin epoch,
    /// read from `SharedState::last_applied_calvin_epoch`. The cross-shard-valid
    /// version anchor for the transaction (0 in single-node / no-Calvin). `None`
    /// outside a transaction block.
    pub tx_snapshot_epoch: Option<u64>,
    /// Identity of the current session transaction block, minted on `BEGIN`
    /// and cleared on `COMMIT`/`ROLLBACK`. Keys the per-transaction staging
    /// overlay. `None` outside a transaction block.
    pub tx_id: Option<TxnId>,
    /// Set of vShards this transaction has staged writes to, recorded on every
    /// staged/buffered write. A transaction can stage to multiple vShards/cores
    /// (e.g. two INSERTs to collections homed on different cores), so overlay
    /// teardown (`MetaOp::DropTxnOverlay` at ROLLBACK) and per-vShard savepoint
    /// mark/rewind must fan over ALL of them. Ordered (BTree) for deterministic
    /// teardown. Empty until the first staged write.
    pub tx_vshards: BTreeSet<VShardId>,
    /// Read-set: LSN-versioned, predicate-aware entries for write conflict
    /// detection, captured on the shared read seam by every transport. At
    /// COMMIT, each entry is checked — if the entry's collection has a current
    /// write-LSN past `read_lsn`, a concurrent write occurred and the
    /// transaction is rejected with SERIALIZATION_FAILURE.
    pub tx_read_set: Vec<super::read_set::ReadSetEntry>,
    /// Distinct vShards this transaction took a SHARED read reservation on. A
    /// hot-key read reserves under the transaction's single `tx_reservation_owner`
    /// and routes to the key's owning vShard; release at every graceful txn exit
    /// only needs the OWNER plus the SET of vShards touched (one sequenced
    /// `ReleaseReservation` per distinct vShard), so the per-key lock identity is
    /// not retained. Ordered (BTree) for deterministic release. Cleared alongside
    /// `tx_read_set` at transaction boundaries.
    pub tx_reservation_vshards: BTreeSet<u32>,
    /// The single reservation owner id minted for this transaction, set on the
    /// FIRST hot-key read and reused for every subsequent reservation so one
    /// `lock_owner` covers the whole transaction. `None` until the first hot-key
    /// read reserves, and reset at transaction boundaries.
    pub tx_reservation_owner: Option<nodedb_cluster::calvin::types::TxnIdWire>,
    /// Savepoint stack. On ROLLBACK TO, truncate tx_buffer to the saved length
    /// AND rewind each staged vShard's two Data-Plane staging overlays (value/TTL
    /// and GRAPH) to their saved journal markers. See [`SavepointEntry`].
    pub savepoints: Vec<SavepointEntry>,
    /// Pending consumer offset commits deferred until COMMIT.
    /// Each entry: (tenant_id, stream_name, group_name, partition_id, lsn).
    /// Flushed atomically on COMMIT, discarded on ROLLBACK.
    pub pending_offset_commits: Vec<(u64, String, String, u32, u64)>,
    /// Server-side cursors: name → (cached result rows as JSON strings, current position).
    pub cursors: HashMap<String, CursorState>,
    /// LIVE SELECT subscriptions: active change stream subscriptions for this connection.
    /// Each subscription receives filtered change events from the broadcast channel.
    /// Drained between queries to deliver pgwire NotificationResponse messages.
    pub live_subscriptions: Vec<(String, crate::control::change_stream::Subscription)>,
    /// Active LISTEN subscriptions for this session: (channel, session_id, receiver).
    /// Drained between queries to deliver pgwire NotificationResponse messages.
    pub listen_handles: Vec<crate::control::notify_bus::ListenHandle>,
    /// NOTIFY messages buffered inside an open transaction (COMMIT fires them).
    /// Each entry is (channel, payload).
    pub pending_notifies: Vec<(String, String)>,
    /// Pending pgwire NOTICE messages queued during query execution.
    /// Drained between query and response delivery so the client receives a
    /// `NoticeResponse` for warnings raised by the response shaper (e.g. an
    /// array slice request whose `system_as_of` fell below the oldest tile
    /// version). Populated by `payload_to_response` when the decoded
    /// `ArraySliceResponse` carries `truncated_before_horizon = true`.
    pub pending_notices: Vec<String>,
    /// SQL-level prepared statements: PREPARE name(types) AS query.
    /// Separate from pgwire wire-level prepared statements (managed by pgwire crate).
    pub prepared_stmts: super::prepared_cache::PreparedStatementCache,
    /// Temporary tables: per-session, auto-dropped on disconnect.
    pub temp_tables: super::temp_tables::TempTableRegistry,
    /// Per-session plan cache for prepared statement execution.
    /// Keyed by (sql_hash, schema_version) — auto-invalidates on DDL.
    pub plan_cache: crate::control::server::shared::session::plan_cache::PlanCache,
    /// GAP_FREE sequence reservations pending commit/rollback.
    /// On COMMIT: each reservation is finalized. On ROLLBACK: counter decremented.
    pub pending_sequence_reservations: Vec<crate::control::sequence::gap_free::ReservationHandle>,
    /// Millis-since-epoch of the last statement COMPLETION on this connection
    /// (also set to "now" at connection start). Read by the pgwire listener
    /// watchdog to decide idle eligibility: a connection is idle only when it
    /// has been silent (no statement completing) for the idle window.
    pub last_activity_ms: AtomicU64,
    /// Count of currently-executing statements on this connection. A connection
    /// is idle-eligible only when this is zero — a legitimately long-running
    /// statement (in flight) must never be idle-killed.
    pub in_flight: AtomicU32,
    /// Highest committed write-version this session has observed for each
    /// `(database, tenant, collection)` it has written, keyed identically to
    /// the read/write namespace. Used to floor a later transaction's captured
    /// `read_version_lsn` at the session's OWN prior committed writes — a
    /// read-your-writes floor that removes cross-shard OCC self-aborts on a
    /// collection the session itself last wrote, without ever masking a
    /// concurrent OTHER-session write (whose higher `coll_write_lsn` still
    /// exceeds the floor). Persists for the life of the session — a prior
    /// autocommit write must still floor a later transaction's read — and is
    /// therefore NOT cleared at transaction boundaries.
    pub own_write_versions: HashMap<(DatabaseId, TenantId, String), Lsn>,
}

pub(super) fn default_parameters() -> HashMap<String, String> {
    let mut parameters = HashMap::new();
    // Default session parameters (PostgreSQL compatibility).
    parameters.insert("application_name".into(), String::new());
    parameters.insert("client_encoding".into(), "UTF8".into());
    parameters.insert("client_min_messages".into(), "notice".into());
    parameters.insert("server_encoding".into(), "UTF8".into());
    parameters.insert("DateStyle".into(), "ISO, MDY".into());
    parameters.insert("TimeZone".into(), "UTC".into());
    parameters.insert(
        "default_transaction_isolation".into(),
        "read committed".into(),
    );
    parameters.insert("default_transaction_read_only".into(), "off".into());
    parameters.insert("extra_float_digits".into(), "1".into());
    parameters.insert("IntervalStyle".into(), "postgres".into());
    parameters.insert("lc_collate".into(), "C".into());
    parameters.insert("lc_ctype".into(), "C".into());
    parameters.insert("lc_messages".into(), "C".into());
    parameters.insert("lc_monetary".into(), "C".into());
    parameters.insert("lc_numeric".into(), "C".into());
    parameters.insert("lc_time".into(), "C".into());
    parameters.insert("standard_conforming_strings".into(), "on".into());
    parameters.insert("integer_datetimes".into(), "on".into());
    parameters.insert("search_path".into(), "public".into());
    parameters.insert("statement_timeout".into(), "0".into());
    parameters.insert("transaction_isolation".into(), "read committed".into());
    parameters.insert("transaction_read_only".into(), "off".into());
    // Version info (PostgreSQL compatibility — tools like psql check this).
    parameters.insert(
        "server_version".into(),
        nodedb_types::pg_compat::server_version_string(crate::version::VERSION),
    );
    parameters.insert(
        "server_version_num".into(),
        nodedb_types::pg_compat::PG_COMPAT_VERSION_NUM.into(),
    );
    // NodeDB-specific defaults.
    parameters.insert("nodedb.consistency".into(), "strong".into());
    parameters.insert("default_read_consistency".into(), "strong".into());
    parameters.insert("cross_shard_txn".into(), "strict".into());
    parameters.insert("rounding_mode".into(), "HALF_EVEN".into());
    parameters
}

impl ConnSession {
    pub(super) fn new() -> Self {
        Self {
            parameters: default_parameters(),
            tx_state: TransactionState::Idle,
            current_database: None,
            effective_tenant_id: None,
            identity: None,
            tx_buffer: Vec::new(),
            tx_snapshot_lsn: None,
            tx_snapshot_epoch: None,
            tx_id: None,
            tx_vshards: BTreeSet::new(),
            tx_read_set: Vec::new(),
            tx_reservation_vshards: BTreeSet::new(),
            tx_reservation_owner: None,
            savepoints: Vec::new(),
            pending_offset_commits: Vec::new(),
            cursors: HashMap::new(),
            live_subscriptions: Vec::new(),
            listen_handles: Vec::new(),
            pending_notifies: Vec::new(),
            pending_notices: Vec::new(),
            prepared_stmts: super::prepared_cache::PreparedStatementCache::new(256),
            temp_tables: super::temp_tables::TempTableRegistry::new(),
            plan_cache: crate::control::server::shared::session::plan_cache::PlanCache::new(128),
            pending_sequence_reservations: Vec::new(),
            last_activity_ms: AtomicU64::new(now_unix_ms()),
            in_flight: AtomicU32::new(0),
            own_write_versions: HashMap::new(),
        }
    }
}
