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

/// Per-connection session state.
pub struct PgSession {
    pub tx_state: TransactionState,
    /// Session parameters set via SET commands.
    pub parameters: HashMap<String, String>,
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
        parameters.insert(
            "transaction_isolation".into(),
            "read committed".into(),
        );
        // NodeDB-specific defaults.
        parameters.insert("nodedb.consistency".into(), "strong".into());
        Self {
            tx_state: TransactionState::Idle,
            parameters,
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
                let mut params: Vec<_> = s.parameters.iter().map(|(k, v)| (k.clone(), v.clone())).collect();
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

    /// BEGIN — enter transaction block.
    pub fn begin(&self, addr: &SocketAddr) -> Result<(), &'static str> {
        let mut sessions = self.sessions.write().unwrap_or_else(|p| p.into_inner());
        if let Some(session) = sessions.get_mut(addr) {
            match session.tx_state {
                TransactionState::Idle => {
                    session.tx_state = TransactionState::InBlock;
                    Ok(())
                }
                TransactionState::InBlock => {
                    // PostgreSQL issues a WARNING here, not an error.
                    Ok(())
                }
                TransactionState::Failed => {
                    Err("current transaction is aborted, commands ignored until end of transaction block")
                }
            }
        } else {
            Ok(())
        }
    }

    /// COMMIT — commit and return to idle.
    pub fn commit(&self, addr: &SocketAddr) -> Result<(), &'static str> {
        let mut sessions = self.sessions.write().unwrap_or_else(|p| p.into_inner());
        if let Some(session) = sessions.get_mut(addr) {
            match session.tx_state {
                TransactionState::InBlock => {
                    session.tx_state = TransactionState::Idle;
                    Ok(())
                }
                TransactionState::Failed => {
                    // COMMIT in failed state acts like ROLLBACK per PostgreSQL spec.
                    session.tx_state = TransactionState::Idle;
                    Ok(())
                }
                TransactionState::Idle => {
                    // PostgreSQL issues a WARNING here.
                    Ok(())
                }
            }
        } else {
            Ok(())
        }
    }

    /// ROLLBACK — abort and return to idle.
    pub fn rollback(&self, addr: &SocketAddr) -> Result<(), &'static str> {
        let mut sessions = self.sessions.write().unwrap_or_else(|p| p.into_inner());
        if let Some(session) = sessions.get_mut(addr) {
            session.tx_state = TransactionState::Idle;
        }
        Ok(())
    }

    /// Mark the current transaction as failed (after a query error inside BEGIN).
    pub fn fail_transaction(&self, addr: &SocketAddr) {
        let mut sessions = self.sessions.write().unwrap_or_else(|p| p.into_inner());
        if let Some(session) = sessions.get_mut(addr) {
            if session.tx_state == TransactionState::InBlock {
                session.tx_state = TransactionState::Failed;
            }
        }
    }

    /// Remove a session (connection closed).
    pub fn remove(&self, addr: &SocketAddr) {
        let mut sessions = self.sessions.write().unwrap_or_else(|p| p.into_inner());
        sessions.remove(addr);
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
    let value = value
        .trim_matches('\'')
        .trim_matches('"')
        .to_string();

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
        assert_eq!(parse_show_command("SHOW client_encoding"), Some("client_encoding".into()));
        assert_eq!(parse_show_command("SHOW ALL"), Some("all".into()));
        assert_eq!(parse_show_command("SHOW"), None);
    }

    #[test]
    fn transaction_lifecycle() {
        let store = SessionStore::new();
        let addr: SocketAddr = "127.0.0.1:5000".parse().unwrap();
        store.ensure_session(addr);

        assert_eq!(store.transaction_state(&addr), TransactionState::Idle);

        store.begin(&addr).unwrap();
        assert_eq!(store.transaction_state(&addr), TransactionState::InBlock);

        store.commit(&addr).unwrap();
        assert_eq!(store.transaction_state(&addr), TransactionState::Idle);

        store.begin(&addr).unwrap();
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

        assert_eq!(store.get_parameter(&addr, "client_encoding"), Some("UTF8".into()));

        store.set_parameter(&addr, "application_name".into(), "test_app".into());
        assert_eq!(store.get_parameter(&addr, "application_name"), Some("test_app".into()));
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
