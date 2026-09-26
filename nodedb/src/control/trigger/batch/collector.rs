// SPDX-License-Identifier: BUSL-1.1

//! Trigger batch collector: accumulates row events into batches.
//!
//! Not currently wired into the Normal-mode consumer loop — per-event
//! dispatch is the sole production path for AFTER-ROW trigger firing (see
//! `event::consumer::pipeline::deliver_events`). This collector batches
//! consecutive WriteEvents targeting the same collection before dispatching
//! triggers, yielding batches of up to `batch_size` rows, and remains
//! available for a future WHEN-clause-batched throughput optimization
//! (paired with `event::trigger::dispatcher::batch::dispatch_trigger_batch`).
//!
//! Rows store raw MessagePack bytes with lazy deserialization. Rows filtered
//! out by WHEN clauses never pay the decode cost, and raw bytes are ~50%
//! more compact than `serde_json::Map` in memory.

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::OnceLock;

use nodedb_types::Value;

use super::super::row_identity::inject_row_identity;
use crate::util::rmpv_value::rmpv_to_value;

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
                    .and_then(|bytes| decode_msgpack_to_value_map(bytes, &self.row_id))
            })
            .as_ref()
    }

    /// Access OLD fields, decoding lazily from raw bytes if needed.
    pub fn old_fields(&self) -> Option<&HashMap<String, Value>> {
        self.old_cache
            .get_or_init(|| {
                self.old_raw
                    .as_ref()
                    .and_then(|bytes| decode_msgpack_to_value_map(bytes, &self.row_id))
            })
            .as_ref()
    }

    /// Raw NEW bytes (MessagePack). Used for binary fast-reject in WHEN filters.
    ///
    /// Returns `None` for rows created via `from_decoded` (test path) or DELETE ops.
    pub fn new_raw(&self) -> Option<&[u8]> {
        self.new_raw.as_deref()
    }

    /// Raw OLD bytes (MessagePack). Used for binary fast-reject in WHEN filters.
    ///
    /// Returns `None` for rows created via `from_decoded` (test path) or INSERT ops.
    pub fn old_raw(&self) -> Option<&[u8]> {
        self.old_raw.as_deref()
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
fn decode_msgpack_to_value_map(bytes: &[u8], row_id: &str) -> Option<HashMap<String, Value>> {
    // Try msgpack first.
    if let Ok(rmpv::Value::Map(pairs)) = crate::util::bounded_msgpack::read_value(bytes) {
        let mut map = HashMap::with_capacity(pairs.len());
        for (k, v) in &pairs {
            if let rmpv::Value::String(key) = k
                && let Some(key_str) = key.as_str()
            {
                map.insert(key_str.to_string(), rmpv_to_value(v));
            }
        }
        inject_row_identity(&mut map, row_id);
        return Some(map);
    }
    // JSON fallback (legacy data).
    if let Ok(serde_json::Value::Object(map)) = sonic_rs::from_slice::<serde_json::Value>(bytes) {
        let mut fields: HashMap<String, Value> =
            map.into_iter().map(|(k, v)| (k, Value::from(v))).collect();
        inject_row_identity(&mut fields, row_id);
        return Some(fields);
    }
    None
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
    /// Database ID.
    pub database_id: nodedb_types::DatabaseId,
    /// Tenant ID.
    pub tenant_id: u64,
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
    database_id: nodedb_types::DatabaseId,
    tenant_id: u64,
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
        database_id: nodedb_types::DatabaseId,
        tenant_id: u64,
        row: TriggerBatchRow,
    ) -> Option<TriggerBatch> {
        // If the pending batch targets a different collection/operation, flush it first.
        let flushed = if let Some(ref pending) = self.pending {
            if pending.collection != collection
                || pending.operation != operation
                || pending.database_id != database_id
                || pending.tenant_id != tenant_id
            {
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
                database_id,
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
                    database_id: p.database_id,
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

    // Only User-originated events fire batched AFTER triggers. Deferred
    // events fire through the deferred dispatcher. A restored row fired its
    // triggers when it was first written.
    match event.source {
        EventSource::User => {}
        EventSource::Trigger
        | EventSource::RaftFollower
        | EventSource::CrdtSync
        | EventSource::Deferred
        | EventSource::Restore => return None,
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

    collector.push(
        &event.collection,
        op_str,
        event.database_id,
        event.tenant_id.as_u64(),
        row,
    )
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
        assert!(
            c.push(
                "orders",
                "INSERT",
                nodedb_types::DatabaseId::DEFAULT,
                1,
                row("1")
            )
            .is_none()
        );
        assert!(
            c.push(
                "orders",
                "INSERT",
                nodedb_types::DatabaseId::DEFAULT,
                1,
                row("2")
            )
            .is_none()
        );
        // Third push fills the batch.
        let batch = c.push(
            "orders",
            "INSERT",
            nodedb_types::DatabaseId::DEFAULT,
            1,
            row("3"),
        );
        assert!(batch.is_some());
        let b = batch.unwrap();
        assert_eq!(b.rows.len(), 3);
        assert_eq!(b.collection, "orders");
    }

    #[test]
    fn flush_partial_batch() {
        let mut c = TriggerBatchCollector::new(10);
        c.push(
            "orders",
            "INSERT",
            nodedb_types::DatabaseId::DEFAULT,
            1,
            row("1"),
        );
        c.push(
            "orders",
            "INSERT",
            nodedb_types::DatabaseId::DEFAULT,
            1,
            row("2"),
        );
        let batch = c.flush();
        assert!(batch.is_some());
        assert_eq!(batch.unwrap().rows.len(), 2);
        assert!(!c.has_pending());
    }

    #[test]
    fn different_collection_flushes_old() {
        let mut c = TriggerBatchCollector::new(10);
        c.push(
            "orders",
            "INSERT",
            nodedb_types::DatabaseId::DEFAULT,
            1,
            row("1"),
        );
        c.push(
            "orders",
            "INSERT",
            nodedb_types::DatabaseId::DEFAULT,
            1,
            row("2"),
        );
        // Different collection → flushes "orders" batch.
        let flushed = c.push(
            "users",
            "INSERT",
            nodedb_types::DatabaseId::DEFAULT,
            1,
            row("3"),
        );
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
        c.push(
            "orders",
            "INSERT",
            nodedb_types::DatabaseId::DEFAULT,
            1,
            row("1"),
        );
        let flushed = c.push(
            "orders",
            "DELETE",
            nodedb_types::DatabaseId::DEFAULT,
            1,
            row("2"),
        );
        assert!(flushed.is_some());
        assert_eq!(flushed.unwrap().operation, "INSERT");
    }

    #[test]
    fn empty_flush_returns_none() {
        let mut c = TriggerBatchCollector::new(10);
        assert!(c.flush().is_none());
    }
}
