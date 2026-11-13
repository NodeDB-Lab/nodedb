// SPDX-License-Identifier: BUSL-1.1

//! Write events an Event Plane consumer drops because their collection name
//! is not qualified for their database.
//!
//! The Data Plane names a collection as storage keys it: bare in the default
//! database, `{database_id}/{name}` in any other. A name that does not fit
//! its event's database reaches no catalog registry, so every consumer that
//! routes by the bare name drops the event. Such an event is a Data Plane
//! naming defect, so one counter across all consumers records the drops,
//! and one warning at most per [`WARN_INTERVAL_MS`] names an example.

use std::sync::atomic::{AtomicU64, Ordering};

use super::types::WriteEvent;

/// Events dropped for a collection not qualified for their database, across
/// every consumer since startup.
static UNROUTED_EVENTS: AtomicU64 = AtomicU64::new(0);

/// When the last warning went out, in milliseconds since the Unix epoch.
static LAST_WARN_MS: AtomicU64 = AtomicU64::new(0);

/// The shortest gap between two warnings. A naming defect repeats on every
/// event of the collection, so one warning per window names it without
/// flooding the log.
const WARN_INTERVAL_MS: u64 = 10_000;

/// The consumer that dropped an event.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum UnroutedConsumer {
    StreamingMv,
    Pipeline,
    EventTrigger,
    CdcRouter,
    TriggerDispatcher,
    CrdtPackager,
}

impl UnroutedConsumer {
    fn name(self) -> &'static str {
        match self {
            Self::StreamingMv => "streaming_mv",
            Self::Pipeline => "pipeline",
            Self::EventTrigger => "event_trigger",
            Self::CdcRouter => "cdc_router",
            Self::TriggerDispatcher => "trigger_dispatcher",
            Self::CrdtPackager => "crdt_packager",
        }
    }
}

/// Events dropped for a collection not qualified for their database, since
/// startup. Rendered as `nodedb_event_unrouted_collection_total`.
pub fn unrouted_events() -> u64 {
    UNROUTED_EVENTS.load(Ordering::Relaxed)
}

/// Record that `consumer` dropped `event`. Counts every drop. Warns once per
/// [`WARN_INTERVAL_MS`].
pub fn record_unrouted(event: &WriteEvent, consumer: UnroutedConsumer) {
    let total = UNROUTED_EVENTS.fetch_add(1, Ordering::Relaxed) + 1;
    let now = now_ms();
    let last = LAST_WARN_MS.load(Ordering::Relaxed);
    if now.saturating_sub(last) < WARN_INTERVAL_MS
        || LAST_WARN_MS
            .compare_exchange(last, now, Ordering::Relaxed, Ordering::Relaxed)
            .is_err()
    {
        return;
    }
    tracing::warn!(
        consumer = consumer.name(),
        collection = %event.collection,
        database_id = event.database_id.as_u64(),
        total,
        "write event collection is not qualified for its database; event dropped"
    );
}

fn now_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |elapsed| elapsed.as_millis() as u64)
}

impl WriteEvent {
    /// The bare collection name `consumer` routes the event by. `None` when
    /// the name is not qualified for the event's database: the drop is
    /// counted and warned about.
    pub fn routed_collection(&self, consumer: UnroutedConsumer) -> Option<&str> {
        let collection = self.bare_collection();
        if collection.is_none() {
            record_unrouted(self, consumer);
        }
        collection
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use crate::types::DatabaseId;

    use super::*;

    #[test]
    fn an_unqualified_event_counts_as_unrouted() {
        let mut event = crate::event::test_utils::write_event_in(DatabaseId::new(7), "orders");
        let before = unrouted_events();
        assert_eq!(
            event.routed_collection(UnroutedConsumer::Pipeline),
            Some("orders")
        );

        event.collection = Arc::from("orders");
        assert_eq!(event.routed_collection(UnroutedConsumer::Pipeline), None);
        assert!(unrouted_events() > before);
    }
}
