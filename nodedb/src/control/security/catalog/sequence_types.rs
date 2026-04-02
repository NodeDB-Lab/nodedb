//! Sequence type definitions for catalog storage.

use serde::{Deserialize, Serialize};

/// Persisted sequence definition in the system catalog.
///
/// Stored in redb under `_system.sequences` with key `"{tenant_id}:{name}"`.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StoredSequence {
    pub tenant_id: u32,
    pub name: String,
    /// Owner username (creator).
    pub owner: String,
    /// Starting value (default 1).
    pub start_value: i64,
    /// Increment per nextval call (default 1, can be negative for descending).
    pub increment: i64,
    /// Minimum allowed value (default 1 for ascending, i64::MIN for descending).
    pub min_value: i64,
    /// Maximum allowed value (default i64::MAX for ascending, -1 for descending).
    pub max_value: i64,
    /// Whether to wrap around on reaching max/min (default false).
    pub cycle: bool,
    /// Number of values to pre-allocate per session for reduced contention.
    /// Default 1 (no pre-allocation). Higher values improve throughput for bulk inserts.
    pub cache_size: i64,
    /// Epoch for Raft range fencing. Incremented on ALTER SEQUENCE ... RESTART WITH.
    pub epoch: u64,
    /// Timestamp of creation (milliseconds since epoch).
    pub created_at: u64,
    /// Format template tokens (e.g. `[Literal("INV-"), Year2, Literal("-"), Seq{5}]`).
    /// When `Some`, `nextval()` returns a formatted string instead of raw i64.
    #[serde(default)]
    pub format_template: Option<Vec<crate::control::sequence::FormatToken>>,
    /// Reset scope — when the counter auto-resets to START.
    #[serde(default)]
    pub reset_scope: crate::control::sequence::ResetScope,
    /// GAP_FREE mode: serialize nextval through a mutex, recycle on rollback.
    #[serde(default)]
    pub gap_free: bool,
}

impl StoredSequence {
    /// Create a new sequence definition with default values for unspecified options.
    pub fn new(tenant_id: u32, name: String, owner: String) -> Self {
        Self {
            tenant_id,
            name,
            owner,
            start_value: 1,
            increment: 1,
            min_value: 1,
            max_value: i64::MAX,
            cycle: false,
            cache_size: 1,
            epoch: 1,
            created_at: 0,
            format_template: None,
            reset_scope: crate::control::sequence::ResetScope::Never,
            gap_free: false,
        }
    }

    /// Validate the sequence definition for consistency.
    pub fn validate(&self) -> Result<(), String> {
        if self.name.is_empty() {
            return Err("sequence name is empty".into());
        }
        if self.increment == 0 {
            return Err("INCREMENT must not be zero".into());
        }
        if self.min_value > self.max_value {
            return Err(format!(
                "MINVALUE ({}) must be less than or equal to MAXVALUE ({})",
                self.min_value, self.max_value
            ));
        }
        if self.start_value < self.min_value || self.start_value > self.max_value {
            return Err(format!(
                "START value ({}) is outside range [{}, {}]",
                self.start_value, self.min_value, self.max_value
            ));
        }
        if self.cache_size < 1 {
            return Err("CACHE must be at least 1".into());
        }
        Ok(())
    }
}

/// Runtime state of a sequence on this node.
///
/// Persisted to catalog periodically and on shutdown. Not synced via CRDT —
/// each node maintains its own counter within an allocated range.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SequenceState {
    pub tenant_id: u32,
    pub name: String,
    /// Current value (last returned by nextval on this node).
    pub current_value: i64,
    /// Whether nextval has been called at least once on this node.
    pub is_called: bool,
    /// Epoch of the range allocation. Must match the sequence definition's epoch.
    pub epoch: u64,
    /// Current period key for reset scope (e.g. "2026-04", "2026-Q2").
    /// Empty string means no period tracking (ResetScope::Never).
    #[serde(default)]
    pub period_key: String,
}

impl SequenceState {
    pub fn new(tenant_id: u32, name: String, start_value: i64, epoch: u64) -> Self {
        Self {
            tenant_id,
            name,
            // Start one step before start_value so the first nextval returns start_value.
            current_value: start_value,
            is_called: false,
            epoch,
            period_key: String::new(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn validate_ok() {
        let seq = StoredSequence::new(1, "s1".into(), "admin".into());
        assert!(seq.validate().is_ok());
    }

    #[test]
    fn validate_zero_increment() {
        let mut seq = StoredSequence::new(1, "s1".into(), "admin".into());
        seq.increment = 0;
        assert!(seq.validate().is_err());
    }

    #[test]
    fn validate_min_gt_max() {
        let mut seq = StoredSequence::new(1, "s1".into(), "admin".into());
        seq.min_value = 100;
        seq.max_value = 10;
        assert!(seq.validate().is_err());
    }

    #[test]
    fn validate_start_out_of_range() {
        let mut seq = StoredSequence::new(1, "s1".into(), "admin".into());
        seq.start_value = 0;
        seq.min_value = 1;
        assert!(seq.validate().is_err());
    }
}
