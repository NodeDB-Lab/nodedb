//! Per-session temporary table registry.
//!
//! Temporary tables store data as Arrow RecordBatches in memory.
//! Auto-dropped on disconnect.

use std::collections::HashMap;
use std::net::SocketAddr;

use arrow::datatypes::SchemaRef;
use arrow::record_batch::RecordBatch;

use super::store::SessionStore;

/// A session-local temporary table entry.
pub struct TempTableEntry {
    /// Arrow schema of the table columns.
    pub schema: SchemaRef,
    /// Transaction-end behavior for this temp table's data.
    pub on_commit: OnCommitAction,
    /// Data stored as Arrow RecordBatches.
    pub batches: Vec<RecordBatch>,
}

impl std::fmt::Debug for TempTableEntry {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TempTableEntry")
            .field("schema", &self.schema)
            .field("on_commit", &self.on_commit)
            .field("batches", &self.batches.len())
            .finish()
    }
}

/// Transaction-end behavior for temporary table data.
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
    tables: HashMap<String, TempTableEntry>,
}

impl TempTableRegistry {
    pub fn new() -> Self {
        Self {
            tables: HashMap::new(),
        }
    }

    /// Register a temp table.
    pub fn register(&mut self, name: String, entry: TempTableEntry) {
        self.tables.insert(name, entry);
    }

    /// Check if a temp table exists.
    pub fn exists(&self, name: &str) -> bool {
        self.tables.contains_key(name)
    }

    /// Get a temp table entry.
    pub fn get(&self, name: &str) -> Option<&TempTableEntry> {
        self.tables.get(name)
    }

    /// Get a mutable temp table entry (for INSERT INTO).
    pub fn get_mut(&mut self, name: &str) -> Option<&mut TempTableEntry> {
        self.tables.get_mut(name)
    }

    /// Remove a temp table.
    pub fn remove(&mut self, name: &str) -> bool {
        self.tables.remove(name).is_some()
    }

    /// List all temp table names.
    pub fn names(&self) -> Vec<String> {
        self.tables.keys().cloned().collect()
    }

    /// Apply ON COMMIT actions. Returns names of tables to drop.
    pub fn on_commit(&mut self) -> Vec<String> {
        let mut to_drop = Vec::new();
        for (name, entry) in &self.tables {
            if entry.on_commit == OnCommitAction::Drop {
                to_drop.push(name.clone());
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
    pub fn register_temp_table(&self, addr: &SocketAddr, name: String, entry: TempTableEntry) {
        self.write_session(addr, |session| {
            session.temp_tables.register(name, entry);
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
