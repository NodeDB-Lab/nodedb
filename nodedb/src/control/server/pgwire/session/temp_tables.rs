//! Per-session temporary table registry.
//!
//! Temporary tables are stored as DataFusion MemTables, registered in
//! the session's DataFusion context. They are invisible to other sessions
//! and auto-dropped on disconnect.

use std::collections::HashMap;
use std::net::SocketAddr;

use datafusion::arrow::datatypes::SchemaRef;

use super::store::SessionStore;

/// Metadata about a temporary table in this session.
#[derive(Debug, Clone)]
pub struct TempTableMeta {
    /// Arrow schema of the table.
    pub schema: SchemaRef,
    /// ON COMMIT behavior.
    pub on_commit: OnCommitAction,
}

/// What happens to temp table data on COMMIT.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OnCommitAction {
    /// Keep rows (default).
    PreserveRows,
    /// Delete all rows but keep the table.
    DeleteRows,
    /// Drop the entire table.
    Drop,
}

/// Per-session temp table registry.
pub struct TempTableRegistry {
    /// Table name → metadata.
    tables: HashMap<String, TempTableMeta>,
}

impl TempTableRegistry {
    pub fn new() -> Self {
        Self {
            tables: HashMap::new(),
        }
    }

    /// Register a temp table.
    pub fn register(&mut self, name: String, meta: TempTableMeta) {
        self.tables.insert(name, meta);
    }

    /// Check if a temp table exists.
    pub fn exists(&self, name: &str) -> bool {
        self.tables.contains_key(name)
    }

    /// Remove a temp table.
    pub fn remove(&mut self, name: &str) -> bool {
        self.tables.remove(name).is_some()
    }

    /// List all temp table names.
    pub fn names(&self) -> Vec<String> {
        self.tables.keys().cloned().collect()
    }

    /// Get metadata for a temp table.
    pub fn get(&self, name: &str) -> Option<&TempTableMeta> {
        self.tables.get(name)
    }

    /// Apply ON COMMIT actions. Returns names of tables to drop.
    pub fn on_commit(&mut self) -> Vec<String> {
        let mut to_drop = Vec::new();
        for (name, meta) in &self.tables {
            match meta.on_commit {
                OnCommitAction::Drop => {
                    to_drop.push(name.clone());
                }
                OnCommitAction::DeleteRows => {
                    // Data clearing would need to re-register an empty MemTable.
                    // For now, mark for the caller to handle.
                }
                OnCommitAction::PreserveRows => {}
            }
        }
        for name in &to_drop {
            self.tables.remove(name);
        }
        to_drop
    }

    /// Clear all temp tables (session disconnect).
    pub fn clear(&mut self) {
        self.tables.clear();
    }
}

impl Default for TempTableRegistry {
    fn default() -> Self {
        Self::new()
    }
}

// ── SessionStore methods for temp tables ───────────────────────────

impl SessionStore {
    /// Register a temporary table in the session.
    pub fn register_temp_table(&self, addr: &SocketAddr, name: String, meta: TempTableMeta) {
        self.write_session(addr, |session| {
            session.temp_tables.register(name, meta);
        });
    }

    /// Check if a temp table exists in the session.
    pub fn has_temp_table(&self, addr: &SocketAddr, name: &str) -> bool {
        self.read_session(addr, |s| s.temp_tables.exists(name))
            .unwrap_or(false)
    }

    /// Remove a temp table from the session.
    pub fn remove_temp_table(&self, addr: &SocketAddr, name: &str) -> bool {
        self.write_session(addr, |session| session.temp_tables.remove(name))
            .unwrap_or(false)
    }

    /// Get all temp table names for the session.
    pub fn temp_table_names(&self, addr: &SocketAddr) -> Vec<String> {
        self.read_session(addr, |s| s.temp_tables.names())
            .unwrap_or_default()
    }
}
