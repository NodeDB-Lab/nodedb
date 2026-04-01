//! Trigger batch collector: accumulates row events into batches.
//!
//! Used by the Event Plane consumer to batch consecutive WriteEvents targeting
//! the same collection before dispatching triggers. Instead of firing triggers
//! per-row, the collector yields batches of up to `batch_size` rows.

/// A single row in a trigger batch.
#[derive(Debug, Clone)]
pub struct TriggerBatchRow {
    /// NEW row fields (INSERT/UPDATE). None for DELETE.
    pub new_fields: Option<serde_json::Map<String, serde_json::Value>>,
    /// OLD row fields (UPDATE/DELETE). None for INSERT.
    pub old_fields: Option<serde_json::Map<String, serde_json::Value>>,
    /// Row identifier (for error blaming in future vectorized error handling).
    pub row_id: String,
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
pub fn push_write_event(
    collector: &mut TriggerBatchCollector,
    event: &crate::event::types::WriteEvent,
) -> Option<TriggerBatch> {
    use crate::event::types::{EventSource, WriteOp, deserialize_event_payload};

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

    let new_fields = event
        .new_value
        .as_ref()
        .and_then(|v| deserialize_event_payload(v));
    let old_fields = event
        .old_value
        .as_ref()
        .and_then(|v| deserialize_event_payload(v));

    let row = TriggerBatchRow {
        new_fields,
        old_fields,
        row_id: event.row_id.as_str().to_string(),
    };

    collector.push(&event.collection, op_str, event.tenant_id.as_u32(), row)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn row(id: &str) -> TriggerBatchRow {
        TriggerBatchRow {
            new_fields: Some(serde_json::Map::new()),
            old_fields: None,
            row_id: id.to_string(),
        }
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
