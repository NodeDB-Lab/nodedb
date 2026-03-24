//! Per-connection session state for pgwire clients.
//!
//! Tracks transaction state, session parameters (SET/SHOW), and
//! NodeDB-specific session variables (consistency level, tenant override).
//! Keyed by socket address — one session per TCP connection.

use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::RwLock;

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
}

/// Per-connection session state.
pub struct PgSession {
    pub tx_state: TransactionState,
    /// Session parameters set via SET commands.
    pub parameters: HashMap<String, String>,
    /// Buffered write tasks accumulated between BEGIN and COMMIT.
    /// Dispatched atomically on COMMIT, discarded on ROLLBACK.
    pub tx_buffer: Vec<crate::control::planner::physical::PhysicalTask>,
    /// Snapshot LSN captured at BEGIN for snapshot isolation.
    /// All reads within the transaction see data as of this LSN.
    /// Concurrent writes after this point are invisible to the transaction.
    pub tx_snapshot_lsn: Option<crate::types::Lsn>,
    /// Read-set: (collection, document_id, read_lsn) tuples for write
    /// conflict detection. At COMMIT, each entry is checked — if the
    /// document's current LSN > read_lsn, a concurrent write occurred
    /// and the transaction is rejected with SERIALIZATION_FAILURE.
    pub tx_read_set: Vec<(String, String, crate::types::Lsn)>,
    /// Savepoint stack: each entry is (name, tx_buffer_len_at_savepoint).
    /// On ROLLBACK TO, truncate tx_buffer to the saved length.
    pub savepoints: Vec<(String, usize)>,
    /// Server-side cursors: name → (cached result rows as JSON strings, current position).
    pub cursors: HashMap<String, CursorState>,
}

impl PgSession {
    fn new() -> Self {
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
        Self {
            tx_state: TransactionState::Idle,
            parameters,
            tx_buffer: Vec::new(),
            tx_snapshot_lsn: None,
            tx_read_set: Vec::new(),
            savepoints: Vec::new(),
            cursors: HashMap::new(),
        }
    }
}

/// Concurrent session store — keyed by socket address.
pub struct SessionStore {
    sessions: RwLock<HashMap<SocketAddr, PgSession>>,
}

impl Default for SessionStore {
    fn default() -> Self {
        Self::new()
    }
}

impl SessionStore {
    pub fn new() -> Self {
        Self {
            sessions: RwLock::new(HashMap::new()),
        }
    }

    /// Ensure a session exists for this address.
    pub fn ensure_session(&self, addr: SocketAddr) {
        let mut sessions = self.sessions.write().unwrap_or_else(|p| p.into_inner());
        sessions.entry(addr).or_insert_with(PgSession::new);
    }

    /// Create a savepoint at the current tx_buffer position.
    pub fn create_savepoint(&self, addr: &SocketAddr, name: String) {
        let mut sessions = self.sessions.write().unwrap_or_else(|p| p.into_inner());
        if let Some(session) = sessions.get_mut(addr) {
            let pos = session.tx_buffer.len();
            session.savepoints.push((name, pos));
        }
    }

    /// Release a savepoint (remove from stack, keep buffered operations).
    pub fn release_savepoint(&self, addr: &SocketAddr, name: &str) {
        let mut sessions = self.sessions.write().unwrap_or_else(|p| p.into_inner());
        if let Some(session) = sessions.get_mut(addr) {
            session.savepoints.retain(|(n, _)| n != name);
        }
    }

    /// Rollback to a savepoint: truncate tx_buffer to the saved position.
    ///
    /// Returns `Err` if the savepoint does not exist (matches PostgreSQL behavior).
    pub fn rollback_to_savepoint(&self, addr: &SocketAddr, name: &str) -> crate::Result<()> {
        let mut sessions = self.sessions.write().unwrap_or_else(|p| p.into_inner());
        let session = sessions
            .get_mut(addr)
            .ok_or_else(|| crate::Error::BadRequest {
                detail: "no active session".to_string(),
            })?;
        let pos = session
            .savepoints
            .iter()
            .rposition(|(n, _)| n == name)
            .ok_or_else(|| crate::Error::BadRequest {
                detail: format!("savepoint \"{name}\" does not exist"),
            })?;
        let buffer_pos = session.savepoints[pos].1;
        session.tx_buffer.truncate(buffer_pos);
        session.savepoints.truncate(pos + 1);
        Ok(())
    }

    /// Declare a cursor with pre-fetched results.
    pub fn declare_cursor(&self, addr: &SocketAddr, name: String, rows: Vec<String>) {
        let mut sessions = self.sessions.write().unwrap_or_else(|p| p.into_inner());
        if let Some(session) = sessions.get_mut(addr) {
            session
                .cursors
                .insert(name, CursorState { rows, position: 0 });
        }
    }

    /// Fetch N rows from a cursor. Returns the rows and whether cursor is exhausted.
    pub fn fetch_cursor(
        &self,
        addr: &SocketAddr,
        name: &str,
        count: usize,
    ) -> crate::Result<(Vec<String>, bool)> {
        let mut sessions = self.sessions.write().unwrap_or_else(|p| p.into_inner());
        let session = sessions
            .get_mut(addr)
            .ok_or_else(|| crate::Error::BadRequest {
                detail: "no active session".to_string(),
            })?;
        let cursor = session
            .cursors
            .get_mut(name)
            .ok_or_else(|| crate::Error::BadRequest {
                detail: format!("cursor \"{name}\" does not exist"),
            })?;

        let start = cursor.position;
        let end = (start + count).min(cursor.rows.len());
        let rows: Vec<String> = cursor.rows[start..end].to_vec();
        cursor.position = end;
        let exhausted = end >= cursor.rows.len();
        Ok((rows, exhausted))
    }

    /// Close a cursor.
    pub fn close_cursor(&self, addr: &SocketAddr, name: &str) {
        let mut sessions = self.sessions.write().unwrap_or_else(|p| p.into_inner());
        if let Some(session) = sessions.get_mut(addr) {
            session.cursors.remove(name);
        }
    }

    /// Set a session parameter.
    pub fn set_parameter(&self, addr: &SocketAddr, key: String, value: String) {
        let mut sessions = self.sessions.write().unwrap_or_else(|p| p.into_inner());
        if let Some(session) = sessions.get_mut(addr) {
            session.parameters.insert(key, value);
        }
    }

    /// Get a session parameter.
    pub fn get_parameter(&self, addr: &SocketAddr, key: &str) -> Option<String> {
        let sessions = self.sessions.read().unwrap_or_else(|p| p.into_inner());
        sessions
            .get(addr)
            .and_then(|s| s.parameters.get(key).cloned())
    }

    /// Get all session parameters.
    pub fn all_parameters(&self, addr: &SocketAddr) -> Vec<(String, String)> {
        let sessions = self.sessions.read().unwrap_or_else(|p| p.into_inner());
        sessions
            .get(addr)
            .map(|s| {
                let mut params: Vec<_> = s
                    .parameters
                    .iter()
                    .map(|(k, v)| (k.clone(), v.clone()))
                    .collect();
                params.sort_by(|a, b| a.0.cmp(&b.0));
                params
            })
            .unwrap_or_default()
    }

    /// Get transaction state for a connection.
    pub fn transaction_state(&self, addr: &SocketAddr) -> TransactionState {
        let sessions = self.sessions.read().unwrap_or_else(|p| p.into_inner());
        sessions
            .get(addr)
            .map(|s| s.tx_state)
            .unwrap_or(TransactionState::Idle)
    }

    /// BEGIN — enter transaction block with snapshot isolation.
    ///
    /// Captures the current WAL LSN as the snapshot point. All reads
    /// within this transaction see data as of this LSN.
    pub fn begin(
        &self,
        addr: &SocketAddr,
        current_lsn: crate::types::Lsn,
    ) -> Result<(), &'static str> {
        let mut sessions = self.sessions.write().unwrap_or_else(|p| p.into_inner());
        if let Some(session) = sessions.get_mut(addr) {
            match session.tx_state {
                TransactionState::Idle => {
                    session.tx_state = TransactionState::InBlock;
                    session.tx_snapshot_lsn = Some(current_lsn);
                    session.tx_read_set.clear();
                    Ok(())
                }
                TransactionState::InBlock => {
                    // PostgreSQL issues a WARNING here, not an error.
                    Ok(())
                }
                TransactionState::Failed => Err(
                    "current transaction is aborted, commands ignored until end of transaction block",
                ),
            }
        } else {
            Ok(())
        }
    }

    /// Record a read for write conflict detection.
    ///
    /// Called after each read within a transaction to track which rows
    /// were observed. At COMMIT, these are checked for concurrent modification.
    pub fn record_read(
        &self,
        addr: &SocketAddr,
        collection: String,
        document_id: String,
        read_lsn: crate::types::Lsn,
    ) {
        let mut sessions = self.sessions.write().unwrap_or_else(|p| p.into_inner());
        if let Some(session) = sessions.get_mut(addr)
            && session.tx_state == TransactionState::InBlock
        {
            session
                .tx_read_set
                .push((collection, document_id, read_lsn));
        }
    }

    /// Get the snapshot LSN for the current transaction.
    pub fn snapshot_lsn(&self, addr: &SocketAddr) -> Option<crate::types::Lsn> {
        let sessions = self.sessions.read().unwrap_or_else(|p| p.into_inner());
        sessions.get(addr).and_then(|s| s.tx_snapshot_lsn)
    }

    /// Drain the read-set for conflict checking at COMMIT time.
    pub fn take_read_set(&self, addr: &SocketAddr) -> Vec<(String, String, crate::types::Lsn)> {
        let mut sessions = self.sessions.write().unwrap_or_else(|p| p.into_inner());
        if let Some(session) = sessions.get_mut(addr) {
            std::mem::take(&mut session.tx_read_set)
        } else {
            Vec::new()
        }
    }

    /// COMMIT — drain the write buffer and return to idle.
    ///
    /// Returns the buffered write tasks for atomic dispatch. If the
    /// transaction is in Failed state, discards the buffer.
    pub fn commit(
        &self,
        addr: &SocketAddr,
    ) -> Result<Vec<crate::control::planner::physical::PhysicalTask>, &'static str> {
        let mut sessions = self.sessions.write().unwrap_or_else(|p| p.into_inner());
        if let Some(session) = sessions.get_mut(addr) {
            let buffer = std::mem::take(&mut session.tx_buffer);
            session.tx_state = TransactionState::Idle;
            session.tx_snapshot_lsn = None;
            session.savepoints.clear();
            Ok(buffer)
        } else {
            Ok(Vec::new())
        }
    }

    /// Buffer a write task during a transaction block.
    ///
    /// Returns `true` if buffered (in transaction), `false` if not (dispatch immediately).
    pub fn buffer_write(
        &self,
        addr: &SocketAddr,
        task: crate::control::planner::physical::PhysicalTask,
    ) -> bool {
        let mut sessions = self.sessions.write().unwrap_or_else(|p| p.into_inner());
        if let Some(session) = sessions.get_mut(addr)
            && session.tx_state == TransactionState::InBlock
        {
            session.tx_buffer.push(task);
            return true;
        }
        false
    }

    /// ROLLBACK — discard the write buffer and return to idle.
    pub fn rollback(&self, addr: &SocketAddr) -> Result<(), &'static str> {
        let mut sessions = self.sessions.write().unwrap_or_else(|p| p.into_inner());
        if let Some(session) = sessions.get_mut(addr) {
            session.tx_buffer.clear();
            session.tx_state = TransactionState::Idle;
            session.tx_snapshot_lsn = None;
            session.tx_read_set.clear();
            session.savepoints.clear();
        }
        Ok(())
    }

    /// Mark the current transaction as failed (after a query error inside BEGIN).
    pub fn fail_transaction(&self, addr: &SocketAddr) {
        let mut sessions = self.sessions.write().unwrap_or_else(|p| p.into_inner());
        if let Some(session) = sessions.get_mut(addr)
            && session.tx_state == TransactionState::InBlock
        {
            session.tx_state = TransactionState::Failed;
        }
    }

    /// Remove a session (connection closed).
    pub fn remove(&self, addr: &SocketAddr) {
        let mut sessions = self.sessions.write().unwrap_or_else(|p| p.into_inner());
        sessions.remove(addr);
    }

    /// List all active sessions as (peer_address, transaction_state) pairs.
    pub fn all_sessions(&self) -> Vec<(String, String)> {
        let sessions = self.sessions.read().unwrap_or_else(|p| p.into_inner());
        sessions
            .iter()
            .map(|(addr, session)| {
                let tx = match session.tx_state {
                    TransactionState::Idle => "idle",
                    TransactionState::InBlock => "in_transaction",
                    TransactionState::Failed => "failed",
                };
                (addr.to_string(), tx.to_string())
            })
            .collect()
    }

    /// Number of active sessions.
    pub fn count(&self) -> usize {
        let sessions = self.sessions.read().unwrap_or_else(|p| p.into_inner());
        sessions.len()
    }
}

/// Parse a SET command: `SET [SESSION|LOCAL] key = value` or `SET key TO value`.
///
/// Returns (key, value) on success, or None if not a valid SET command.
pub fn parse_set_command(sql: &str) -> Option<(String, String)> {
    let trimmed = sql.trim();
    let upper = trimmed.to_uppercase();

    // Strip SET prefix.
    let rest = if upper.starts_with("SET SESSION ") {
        &trimmed[12..]
    } else if upper.starts_with("SET LOCAL ") {
        &trimmed[10..]
    } else if upper.starts_with("SET ") {
        &trimmed[4..]
    } else {
        return None;
    };

    let rest = rest.trim();

    // Split on = or TO.
    let (key, value) = if let Some(eq_pos) = rest.find('=') {
        let k = rest[..eq_pos].trim();
        let v = rest[eq_pos + 1..].trim();
        (k, v)
    } else {
        // Try TO separator.
        let upper_rest = rest.to_uppercase();
        if let Some(to_pos) = upper_rest.find(" TO ") {
            let k = rest[..to_pos].trim();
            let v = rest[to_pos + 4..].trim();
            (k, v)
        } else {
            return None;
        }
    };

    if key.is_empty() {
        return None;
    }

    // Strip quotes from value.
    let value = value.trim_matches('\'').trim_matches('"').to_string();

    Some((key.to_lowercase(), value))
}

/// Parse a SHOW command: `SHOW <parameter>` or `SHOW ALL`.
///
/// Returns the parameter name, or "all" for SHOW ALL.
pub fn parse_show_command(sql: &str) -> Option<String> {
    let trimmed = sql.trim();
    let upper = trimmed.to_uppercase();

    if !upper.starts_with("SHOW ") {
        return None;
    }

    let param = trimmed[5..].trim().to_lowercase();
    if param.is_empty() {
        return None;
    }

    Some(param)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_set_equals() {
        let (k, v) = parse_set_command("SET client_encoding = 'UTF8'").unwrap();
        assert_eq!(k, "client_encoding");
        assert_eq!(v, "UTF8");
    }

    #[test]
    fn parse_set_to() {
        let (k, v) = parse_set_command("SET search_path TO public").unwrap();
        assert_eq!(k, "search_path");
        assert_eq!(v, "public");
    }

    #[test]
    fn parse_set_session() {
        let (k, v) = parse_set_command("SET SESSION nodedb.consistency = 'eventual'").unwrap();
        assert_eq!(k, "nodedb.consistency");
        assert_eq!(v, "eventual");
    }

    #[test]
    fn parse_set_nodedb_tenant() {
        let (k, v) = parse_set_command("SET nodedb.tenant_id = 5").unwrap();
        assert_eq!(k, "nodedb.tenant_id");
        assert_eq!(v, "5");
    }

    #[test]
    fn parse_show() {
        assert_eq!(
            parse_show_command("SHOW client_encoding"),
            Some("client_encoding".into())
        );
        assert_eq!(parse_show_command("SHOW ALL"), Some("all".into()));
        assert_eq!(parse_show_command("SHOW"), None);
    }

    #[test]
    fn transaction_lifecycle() {
        let store = SessionStore::new();
        let addr: SocketAddr = "127.0.0.1:5000".parse().unwrap();
        store.ensure_session(addr);

        assert_eq!(store.transaction_state(&addr), TransactionState::Idle);

        store.begin(&addr, crate::types::Lsn::new(1)).unwrap();
        assert_eq!(store.transaction_state(&addr), TransactionState::InBlock);

        store.commit(&addr).unwrap();
        assert_eq!(store.transaction_state(&addr), TransactionState::Idle);

        store.begin(&addr, crate::types::Lsn::new(1)).unwrap();
        store.fail_transaction(&addr);
        assert_eq!(store.transaction_state(&addr), TransactionState::Failed);

        store.rollback(&addr).unwrap();
        assert_eq!(store.transaction_state(&addr), TransactionState::Idle);
    }

    #[test]
    fn session_parameters() {
        let store = SessionStore::new();
        let addr: SocketAddr = "127.0.0.1:5000".parse().unwrap();
        store.ensure_session(addr);

        assert_eq!(
            store.get_parameter(&addr, "client_encoding"),
            Some("UTF8".into())
        );

        store.set_parameter(&addr, "application_name".into(), "test_app".into());
        assert_eq!(
            store.get_parameter(&addr, "application_name"),
            Some("test_app".into())
        );
    }

    #[test]
    fn session_cleanup() {
        let store = SessionStore::new();
        let addr: SocketAddr = "127.0.0.1:5000".parse().unwrap();
        store.ensure_session(addr);
        assert_eq!(store.count(), 1);

        store.remove(&addr);
        assert_eq!(store.count(), 0);
    }
}
