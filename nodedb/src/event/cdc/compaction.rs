//! Background log compaction for change streams.
//!
//! Periodically scans all compaction-enabled streams and deduplicates
//! their buffers by key field, keeping only the latest event per key.
//! DELETE events are retained as tombstones until their grace period expires.

use std::sync::Arc;
use std::time::Duration;

use tokio::sync::watch;
use tracing::{debug, trace};

use super::registry::StreamRegistry;
use super::router::CdcRouter;

/// Compaction interval: how often the background task runs.
const COMPACTION_INTERVAL: Duration = Duration::from_secs(60);

/// Spawn the background compaction task.
pub fn spawn_compaction_task(
    registry: Arc<StreamRegistry>,
    router: Arc<CdcRouter>,
    mut shutdown: watch::Receiver<bool>,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        debug!("CDC compaction task started");

        loop {
            tokio::select! {
                _ = tokio::time::sleep(COMPACTION_INTERVAL) => {}
                _ = shutdown.changed() => {
                    if *shutdown.borrow() {
                        debug!("CDC compaction task shutting down");
                        return;
                    }
                }
            }

            if *shutdown.borrow() {
                return;
            }

            // Scan all streams with compaction enabled, grouped by tenant.
            // Round-robin across tenants: process one stream per tenant in
            // rotation so a tenant with many streams doesn't starve others.
            let stats = router.buffer_stats();

            // Group by tenant_id.
            let mut by_tenant: std::collections::HashMap<u32, Vec<_>> =
                std::collections::HashMap::new();
            for stat in stats {
                by_tenant.entry(stat.tenant_id).or_default().push(stat);
            }

            // Interleave: process one stream from each tenant, then repeat.
            let max_streams = by_tenant.values().map(|v| v.len()).max().unwrap_or(0);
            for round in 0..max_streams {
                for streams in by_tenant.values() {
                    let Some(stat) = streams.get(round) else {
                        continue;
                    };
                    let def = registry.get(stat.tenant_id, &stat.stream_name);
                    let def = match def {
                        Some(d) if d.compaction.enabled => d,
                        _ => continue,
                    };

                    if let Some(buffer) = router.get_buffer(stat.tenant_id, &stat.stream_name) {
                        let removed = buffer.compact(
                            &def.compaction.key_field,
                            def.compaction.tombstone_grace_secs,
                        );
                        if removed > 0 {
                            debug!(
                                stream = %stat.stream_name,
                                tenant = stat.tenant_id,
                                removed,
                                remaining = buffer.len(),
                                key = %def.compaction.key_field,
                                "compacted stream buffer"
                            );
                        } else {
                            trace!(stream = %stat.stream_name, "compaction: nothing to compact");
                        }
                    }
                }
            }
        }
    })
}

#[cfg(test)]
mod tests {
    use super::super::buffer::StreamBuffer;
    use super::super::event::CdcEvent;
    use super::super::stream_def::RetentionConfig;

    fn make_event(seq: u64, key_val: &str, op: &str) -> CdcEvent {
        let now_ms = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_millis() as u64;
        CdcEvent {
            sequence: seq,
            partition: 0,
            collection: "users".into(),
            op: op.into(),
            row_id: format!("row-{seq}"),
            event_time: now_ms,
            lsn: seq * 10,
            tenant_id: 1,
            new_value: Some(serde_json::json!({"id": key_val, "name": format!("v{seq}")})),
            old_value: None,
            schema_version: 0,
        }
    }

    #[test]
    fn compact_deduplicates_by_key() {
        let buf = StreamBuffer::new(
            "test".into(),
            RetentionConfig {
                max_events: 1000,
                max_age_secs: 3600,
            },
        );

        // Three updates to the same key "alice"
        buf.push(make_event(1, "alice", "INSERT"));
        buf.push(make_event(2, "alice", "UPDATE"));
        buf.push(make_event(3, "alice", "UPDATE"));
        // One event for key "bob"
        buf.push(make_event(4, "bob", "INSERT"));

        assert_eq!(buf.len(), 4);

        let removed = buf.compact("id", 86_400);
        assert_eq!(removed, 2); // Two older "alice" events removed.
        assert_eq!(buf.len(), 2); // Latest "alice" + "bob" remain.

        let events = buf.read_from_lsn(0, 10);
        // Latest alice (seq 3) and bob (seq 4) remain.
        assert_eq!(events.len(), 2);
    }

    #[test]
    fn compact_retains_tombstones_within_grace_period() {
        let buf = StreamBuffer::new(
            "test".into(),
            RetentionConfig {
                max_events: 1000,
                max_age_secs: 3600,
            },
        );

        buf.push(make_event(1, "alice", "INSERT"));
        buf.push(make_event(2, "alice", "DELETE")); // Tombstone

        // Grace period is 86400s — tombstone is recent, should be kept.
        let removed = buf.compact("id", 86_400);
        assert_eq!(removed, 1); // Only the INSERT removed (DELETE is latest).
        assert_eq!(buf.len(), 1); // Tombstone remains.
    }
}
