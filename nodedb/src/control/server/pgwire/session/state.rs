//! Per-connection session state types.

use std::collections::HashMap;

use crate::control::planner::physical::PhysicalTask;
use crate::types::Lsn;

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

impl TransactionState {
    /// PostgreSQL ReadyForQuery status byte.
    pub fn status_byte(&self) -> u8 {
        match self {
            TransactionState::Idle => b'I',
            TransactionState::InBlock => b'T',
            TransactionState::Failed => b'E',
        }
    }
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
pub struct PgSession {
    pub tx_state: TransactionState,
    /// Session parameters set via SET commands.
    pub parameters: HashMap<String, String>,
    /// Buffered write tasks accumulated between BEGIN and COMMIT.
    /// Dispatched atomically on COMMIT, discarded on ROLLBACK.
    pub tx_buffer: Vec<PhysicalTask>,
    /// Snapshot LSN captured at BEGIN for snapshot isolation.
    /// All reads within the transaction see data as of this LSN.
    /// Concurrent writes after this point are invisible to the transaction.
    pub tx_snapshot_lsn: Option<Lsn>,
    /// Read-set: (collection, document_id, read_lsn) tuples for write
    /// conflict detection. At COMMIT, each entry is checked — if the
    /// document's current LSN > read_lsn, a concurrent write occurred
    /// and the transaction is rejected with SERIALIZATION_FAILURE.
    pub tx_read_set: Vec<(String, String, Lsn)>,
    /// Savepoint stack: each entry is (name, tx_buffer_len_at_savepoint).
    /// On ROLLBACK TO, truncate tx_buffer to the saved length.
    pub savepoints: Vec<(String, usize)>,
    /// Pending consumer offset commits deferred until COMMIT.
    /// Each entry: (tenant_id, stream_name, group_name, partition_id, lsn).
    /// Flushed atomically on COMMIT, discarded on ROLLBACK.
    pub pending_offset_commits: Vec<(u32, String, String, u16, u64)>,
    /// Server-side cursors: name → (cached result rows as JSON strings, current position).
    pub cursors: HashMap<String, CursorState>,
    /// LIVE SELECT subscriptions: active change stream subscriptions for this connection.
    /// Each subscription receives filtered change events from the broadcast channel.
    /// Drained between queries to deliver pgwire NotificationResponse messages.
    pub live_subscriptions: Vec<(String, crate::control::change_stream::Subscription)>,
    /// SQL-level prepared statements: PREPARE name(types) AS query.
    /// Separate from pgwire wire-level prepared statements (managed by pgwire crate).
    pub prepared_stmts: super::prepared_cache::PreparedStatementCache,
    /// Temporary tables: per-session, auto-dropped on disconnect.
    pub temp_tables: super::temp_tables::TempTableRegistry,
    /// Per-session plan cache for prepared statement execution.
    /// Keyed by (sql_hash, schema_version) — auto-invalidates on DDL.
    pub plan_cache: crate::control::server::pgwire::handler::prepared::plan_cache::PlanCache,
    /// GAP_FREE sequence reservations pending commit/rollback.
    /// On COMMIT: each reservation is finalized. On ROLLBACK: counter decremented.
    pub pending_sequence_reservations: Vec<crate::control::sequence::gap_free::ReservationHandle>,
}

impl PgSession {
    pub(super) fn new() -> Self {
        let mut parameters = HashMap::new();
        // Default session parameters (PostgreSQL compatibility).
        parameters.insert("client_encoding".into(), "UTF8".into());
        parameters.insert("server_encoding".into(), "UTF8".into());
        parameters.insert("DateStyle".into(), "ISO, MDY".into());
        parameters.insert("TimeZone".into(), "UTC".into());
        parameters.insert("standard_conforming_strings".into(), "on".into());
        parameters.insert("integer_datetimes".into(), "on".into());
        parameters.insert("search_path".into(), "public".into());
        parameters.insert("transaction_isolation".into(), "read committed".into());
        // Version info (PostgreSQL compatibility — tools like psql check this).
        parameters.insert(
            "server_version".into(),
            format!("NodeDB {}", crate::version::VERSION),
        );
        // NodeDB-specific defaults.
        parameters.insert("nodedb.consistency".into(), "strong".into());
        parameters.insert("rounding_mode".into(), "HALF_EVEN".into());
        Self {
            tx_state: TransactionState::Idle,
            parameters,
            tx_buffer: Vec::new(),
            tx_snapshot_lsn: None,
            tx_read_set: Vec::new(),
            savepoints: Vec::new(),
            pending_offset_commits: Vec::new(),
            cursors: HashMap::new(),
            live_subscriptions: Vec::new(),
            prepared_stmts: super::prepared_cache::PreparedStatementCache::new(256),
            temp_tables: super::temp_tables::TempTableRegistry::new(),
            plan_cache:
                crate::control::server::pgwire::handler::prepared::plan_cache::PlanCache::new(128),
            pending_sequence_reservations: Vec::new(),
        }
    }
}
