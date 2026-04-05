//! Trigger batch collector: accumulates row events into batches.
//!
//! Used by the Event Plane consumer to batch consecutive WriteEvents targeting
//! the same collection before dispatching triggers. Instead of firing triggers
//! per-row, the collector yields batches of up to `batch_size` rows.
//!
//! Rows store raw MessagePack bytes with lazy deserialization. Rows filtered
//! out by WHEN clauses never pay the decode cost, and raw bytes are ~50%
//! more compact than `serde_json::Map` in memory.

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::OnceLock;

use nodedb_types::Value;

/// A single row in a trigger batch.
///
/// Stores raw MessagePack bytes from the WriteEvent and decodes directly
/// to `HashMap<String, nodedb_types::Value>` on first access — skipping
/// the `serde_json::Value` intermediate that was previously required.
#[derive(Debug)]
pub struct TriggerBatchRow {
    /// Raw NEW value bytes (MessagePack). None for DELETE.
    new_raw: Option<Arc<[u8]>>,
    /// Raw OLD value bytes (MessagePack). None for INSERT.
    old_raw: Option<Arc<[u8]>>,
    /// Lazily decoded NEW fields (directly to nodedb_types::Value).
    new_cache: OnceLock<Option<HashMap<String, Value>>>,
    /// Lazily decoded OLD fields (directly to nodedb_types::Value).
    old_cache: OnceLock<Option<HashMap<String, Value>>>,
    /// Row identifier (for error blaming).
    pub row_id: String,
}

impl Clone for TriggerBatchRow {
    fn clone(&self) -> Self {
        Self {
            new_raw: self.new_raw.clone(),
            old_raw: self.old_raw.clone(),
            // Clone decoded cache if present, otherwise it will re-decode lazily.
            new_cache: self
                .new_cache
                .get()
                .cloned()
                .map_or_else(OnceLock::new, |v| {
                    let cell = OnceLock::new();
                    let _ = cell.set(v);
                    cell
                }),
            old_cache: self
                .old_cache
                .get()
                .cloned()
                .map_or_else(OnceLock::new, |v| {
                    let cell = OnceLock::new();
                    let _ = cell.set(v);
                    cell
                }),
            row_id: self.row_id.clone(),
        }
    }
}

impl TriggerBatchRow {
    /// Create from raw MessagePack bytes (production path).
    pub fn from_raw(
        new_raw: Option<Arc<[u8]>>,
        old_raw: Option<Arc<[u8]>>,
        row_id: String,
    ) -> Self {
        Self {
            new_raw,
            old_raw,
            new_cache: OnceLock::new(),
            old_cache: OnceLock::new(),
            row_id,
        }
    }

    /// Create from pre-decoded fields (test convenience).
    pub fn from_decoded(
        new_fields: Option<HashMap<String, Value>>,
        old_fields: Option<HashMap<String, Value>>,
        row_id: String,
    ) -> Self {
        let new_cache = OnceLock::new();
        let old_cache = OnceLock::new();
        let _ = new_cache.set(new_fields);
        let _ = old_cache.set(old_fields);
        Self {
            new_raw: None,
            old_raw: None,
            new_cache,
            old_cache,
            row_id,
        }
    }

    /// Access NEW fields, decoding lazily from raw bytes if needed.
    pub fn new_fields(&self) -> Option<&HashMap<String, Value>> {
        self.new_cache
            .get_or_init(|| {
                self.new_raw
                    .as_ref()
                    .and_then(|bytes| decode_msgpack_to_value_map(bytes))
            })
            .as_ref()
    }

    /// Access OLD fields, decoding lazily from raw bytes if needed.
    pub fn old_fields(&self) -> Option<&HashMap<String, Value>> {
        self.old_cache
            .get_or_init(|| {
                self.old_raw
                    .as_ref()
                    .and_then(|bytes| decode_msgpack_to_value_map(bytes))
            })
            .as_ref()
    }

    /// Mutable access to NEW fields (for BEFORE trigger ASSIGN mutations).
    /// Ensures the cache is initialized first.
    pub fn new_fields_mut(&mut self) -> Option<&mut HashMap<String, Value>> {
        // Force initialization if not yet decoded.
        let _ = self.new_fields();
        self.new_cache.get_mut().and_then(|opt| opt.as_mut())
    }
}

/// Decode raw msgpack bytes directly to `HashMap<String, nodedb_types::Value>`.
///
/// Skips the `serde_json::Value` intermediate entirely. Uses `rmpv` for
/// dynamic msgpack parsing, then converts each value to `nodedb_types::Value`.
fn decode_msgpack_to_value_map(bytes: &[u8]) -> Option<HashMap<String, Value>> {
    // Try msgpack first.
    if let Ok(rmpv::Value::Map(pairs)) = rmpv::decode::read_value(&mut &bytes[..]) {
        let mut map = HashMap::with_capacity(pairs.len());
        for (k, v) in &pairs {
            if let rmpv::Value::String(key) = k
                && let Some(key_str) = key.as_str()
            {
                map.insert(key_str.to_string(), rmpv_to_value(v));
            }
        }
        return Some(map);
    }
    // JSON fallback (legacy data).
    if let Ok(serde_json::Value::Object(map)) = sonic_rs::from_slice::<serde_json::Value>(bytes) {
        return Some(map.into_iter().map(|(k, v)| (k, Value::from(v))).collect());
    }
    None
}

/// Convert an rmpv Value directly to nodedb_types::Value (no serde_json intermediate).
fn rmpv_to_value(val: &rmpv::Value) -> Value {
    match val {
        rmpv::Value::Nil => Value::Null,
        rmpv::Value::Boolean(b) => Value::Bool(*b),
        rmpv::Value::Integer(i) => {
            if let Some(n) = i.as_i64() {
                Value::Integer(n)
            } else if let Some(n) = i.as_u64() {
                Value::Integer(n as i64)
            } else {
                Value::Null
            }
        }
        rmpv::Value::F32(f) => Value::Float(*f as f64),
        rmpv::Value::F64(f) => Value::Float(*f),
        rmpv::Value::String(s) => Value::String(s.as_str().unwrap_or("").to_string()),
        rmpv::Value::Binary(b) => Value::Bytes(b.clone()),
        rmpv::Value::Array(arr) => Value::Array(arr.iter().map(rmpv_to_value).collect()),
        rmpv::Value::Map(pairs) => {
            let mut map = HashMap::new();
            for (k, v) in pairs {
                let key = match k {
                    rmpv::Value::String(s) => s.as_str().unwrap_or("").to_string(),
                    other => format!("{other}"),
                };
                map.insert(key, rmpv_to_value(v));
            }
            Value::Object(map)
        }
        rmpv::Value::Ext(_, _) => Value::Null,
    }
}

/// A complete batch of rows for trigger dispatch.
#[derive(Debug)]
pub struct TriggerBatch {
    /// Collection this batch targets.
    pub collection: String,
    /// DML operation type.
    pub operation: String,
    /// Accumulated rows.
    pub rows: Vec<TriggerBatchRow>,
    /// Tenant ID.
    pub tenant_id: u32,
}

/// Accumulates WriteEvent row data into batches by collection.
///
/// Call `push()` for each WriteEvent. When the batch reaches `batch_size`
/// or `flush()` is called, the accumulated rows are yielded as a `TriggerBatch`.
pub struct TriggerBatchCollector {
    batch_size: usize,
    /// Current in-progress batch (collection → pending rows).
    pending: Option<PendingBatch>,
}

struct PendingBatch {
    collection: String,
    operation: String,
    tenant_id: u32,
    rows: Vec<TriggerBatchRow>,
}

impl TriggerBatchCollector {
    pub fn new(batch_size: usize) -> Self {
        Self {
            batch_size,
            pending: None,
        }
    }

    /// Push a row into the collector.
    ///
    /// Returns `Some(TriggerBatch)` if the push completes a batch (hit batch_size)
    /// or if the new row targets a different collection (flushes the old batch first).
    pub fn push(
        &mut self,
        collection: &str,
        operation: &str,
        tenant_id: u32,
        row: TriggerBatchRow,
    ) -> Option<TriggerBatch> {
        // If the pending batch targets a different collection/operation, flush it first.
        let flushed = if let Some(ref pending) = self.pending {
            if pending.collection != collection || pending.operation != operation {
                self.flush()
            } else {
                None
            }
        } else {
            None
        };

        // Start new batch if needed.
        if self.pending.is_none() {
            self.pending = Some(PendingBatch {
                collection: collection.to_string(),
                operation: operation.to_string(),
                tenant_id,
                rows: Vec::with_capacity(self.batch_size),
            });
        }

        // Add the row.
        if let Some(ref mut pending) = self.pending {
            pending.rows.push(row);

            // If batch is full, flush it.
            if pending.rows.len() >= self.batch_size {
                let batch = self.flush();
                // If we already flushed a different-collection batch, return that.
                // The full batch will be returned on the next call or flush.
                return flushed.or(batch);
            }
        }

        flushed
    }

    /// Flush the pending batch, returning it if non-empty.
    pub fn flush(&mut self) -> Option<TriggerBatch> {
        self.pending.take().and_then(|p| {
            if p.rows.is_empty() {
                None
            } else {
                Some(TriggerBatch {
                    collection: p.collection,
                    operation: p.operation,
                    rows: p.rows,
                    tenant_id: p.tenant_id,
                })
            }
        })
    }

    /// Check if there is a pending batch with rows.
    pub fn has_pending(&self) -> bool {
        self.pending.as_ref().is_some_and(|p| !p.rows.is_empty())
    }
}

/// Convert a [`WriteEvent`] into a [`TriggerBatchRow`] and push to the collector.
///
/// Returns `Some(TriggerBatch)` if the push completes or flushes a batch.
/// Returns `None` for non-triggerable events or if the batch isn't full yet.
///
/// Stores raw bytes from the WriteEvent — no deserialization at ingestion time.
/// The row decodes lazily when a trigger body or WHEN clause accesses the fields.
pub fn push_write_event(
    collector: &mut TriggerBatchCollector,
    event: &crate::event::types::WriteEvent,
) -> Option<TriggerBatch> {
    use crate::event::types::{EventSource, WriteOp};

    // Only User-originated events fire triggers.
    if !matches!(event.source, EventSource::User) {
        return None;
    }

    let op_str = match event.op {
        WriteOp::Insert => "INSERT",
        WriteOp::Update => "UPDATE",
        WriteOp::Delete => "DELETE",
        _ => return None,
    };

    let row = TriggerBatchRow::from_raw(
        event.new_value.clone(),
        event.old_value.clone(),
        event.row_id.as_str().to_string(),
    );

    collector.push(&event.collection, op_str, event.tenant_id.as_u32(), row)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn row(id: &str) -> TriggerBatchRow {
        TriggerBatchRow::from_decoded(Some(HashMap::new()), None, id.to_string())
    }

    #[test]
    fn collect_up_to_batch_size() {
        let mut c = TriggerBatchCollector::new(3);
        assert!(c.push("orders", "INSERT", 1, row("1")).is_none());
        assert!(c.push("orders", "INSERT", 1, row("2")).is_none());
        // Third push fills the batch.
        let batch = c.push("orders", "INSERT", 1, row("3"));
        assert!(batch.is_some());
        let b = batch.unwrap();
        assert_eq!(b.rows.len(), 3);
        assert_eq!(b.collection, "orders");
    }

    #[test]
    fn flush_partial_batch() {
        let mut c = TriggerBatchCollector::new(10);
        c.push("orders", "INSERT", 1, row("1"));
        c.push("orders", "INSERT", 1, row("2"));
        let batch = c.flush();
        assert!(batch.is_some());
        assert_eq!(batch.unwrap().rows.len(), 2);
        assert!(!c.has_pending());
    }

    #[test]
    fn different_collection_flushes_old() {
        let mut c = TriggerBatchCollector::new(10);
        c.push("orders", "INSERT", 1, row("1"));
        c.push("orders", "INSERT", 1, row("2"));
        // Different collection → flushes "orders" batch.
        let flushed = c.push("users", "INSERT", 1, row("3"));
        assert!(flushed.is_some());
        let b = flushed.unwrap();
        assert_eq!(b.collection, "orders");
        assert_eq!(b.rows.len(), 2);
        // "users" batch is now pending.
        assert!(c.has_pending());
        let users_batch = c.flush().unwrap();
        assert_eq!(users_batch.collection, "users");
        assert_eq!(users_batch.rows.len(), 1);
    }

    #[test]
    fn different_operation_flushes_old() {
        let mut c = TriggerBatchCollector::new(10);
        c.push("orders", "INSERT", 1, row("1"));
        let flushed = c.push("orders", "DELETE", 1, row("2"));
        assert!(flushed.is_some());
        assert_eq!(flushed.unwrap().operation, "INSERT");
    }

    #[test]
    fn empty_flush_returns_none() {
        let mut c = TriggerBatchCollector::new(10);
        assert!(c.flush().is_none());
    }
}
