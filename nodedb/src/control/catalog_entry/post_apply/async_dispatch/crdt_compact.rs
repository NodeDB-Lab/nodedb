// SPDX-License-Identifier: BUSL-1.1

//! Post-apply for the COMPACT HISTORY catalog entry.
//!
//! `CompactHistory` deletes the checkpoint rows on every node and
//! dispatches `CrdtOp::CompactAtVersion` to every core on that node. The
//! compaction discards durable oplog entries, so a node that skips it keeps a
//! history its peers reclaimed and answers an old-version read differently.
//!
//! The entry carries `target_version_json` because apply removes the
//! checkpoint row that holds the version vector. This module reads the target
//! from the entry, never from the catalog.
//!
//! Apply records the owed compaction in `_system.pending_history_compaction`.
//! A core answers `Ok` only after it compacted and published a CRDT
//! checkpoint, so the row is removed once every core answered `Ok`. A failed
//! fan-out keeps the row, and the retry worker and the boot drain re-drive
//! it. The row is the durable owner of the retry, so a failure never stops
//! the apply batch.
//!
//! The COMPACT HISTORY handler calls [`compact_async`] directly on a single
//! node, where no applier runs and the post-apply lane never fires.

use tracing::warn;

use crate::bridge::envelope::PhysicalPlan;
use crate::control::state::SharedState;
use crate::types::DatabaseId;
use nodedb_physical::physical_plan::CrdtOp;

use super::core_fanout::{CoreFanout, dispatch_to_every_core};

/// Build the compaction plan every node runs against its own oplog.
fn compact_plan(database_id: u64, collection: &str, target_version_json: String) -> PhysicalPlan {
    PhysicalPlan::Crdt(CrdtOp::CompactAtVersion {
        collection: nodedb_types::QualifiedCollection::new(
            DatabaseId::new(database_id),
            collection,
        ),
        target_version_json,
    })
}

/// Compact this node's oplog to the committed target on every core, then
/// remove the owed-compaction row.
///
/// On a failed fan-out the row stays with the attempt recorded, and the
/// error is returned. Every failure files a `Capture`.
pub async fn compact_async(
    database_id: u64,
    tenant_id: u64,
    collection: &str,
    target_version_json: &str,
    shared: &SharedState,
) -> crate::Result<()> {
    let plan = compact_plan(database_id, collection, target_version_json.to_string());
    let target = CoreFanout {
        database_id,
        tenant_id,
        collection,
        what: "history compaction",
        detail: "",
    };
    let catalog = shared.credentials.catalog();

    if let Err(error) = dispatch_to_every_core(shared, &target, &plan).await {
        crate::diag::history_compaction_not_applied(
            &error,
            "compact_dispatch",
            database_id,
            tenant_id,
            collection,
        );
        if let Err(record_error) = catalog.record_pending_history_compaction_attempt(
            database_id,
            tenant_id,
            collection,
            target_version_json,
            &error.to_string(),
        ) {
            warn!(
                collection,
                tenant = tenant_id,
                error = %record_error,
                "history compaction: failed to record the attempt on the owed row"
            );
        }
        return Err(error);
    }

    catalog
        .remove_pending_history_compaction(database_id, tenant_id, collection, target_version_json)
        .inspect_err(|error| {
            crate::diag::history_compaction_not_applied(
                error,
                "compact_row_remove",
                database_id,
                tenant_id,
                collection,
            );
        })
}

/// Re-drive every owed compaction on this node, and return how many are
/// still owed after the pass.
///
/// A row that failed keeps its place for the next pass. `Err` means the
/// owed rows were not readable.
pub async fn drain_pending_compactions(shared: &SharedState) -> crate::Result<usize> {
    let owed = shared
        .credentials
        .catalog()
        .load_pending_history_compactions()?;
    let mut still_owed = 0usize;
    for row in owed {
        if let Err(error) = compact_async(
            row.database_id,
            row.tenant_id,
            &row.collection,
            &row.target_version_json,
            shared,
        )
        .await
        {
            warn!(
                collection = %row.collection,
                tenant = row.tenant_id,
                attempts = row.attempts.saturating_add(1),
                error = %error,
                "history compaction still owed; the next pass retries it"
            );
            still_owed += 1;
        }
    }
    Ok(still_owed)
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::time::{Duration, Instant};

    use super::*;
    use crate::bridge::dispatch::{BridgeResponse, CoreChannelDataSide, Dispatcher};
    use crate::bridge::envelope::{ErrorCode, Payload, Response, Status};
    use crate::control::catalog_entry::CatalogEntry;
    use crate::control::catalog_entry::apply::apply_to;
    use crate::types::Lsn;
    use crate::wal::WalManager;

    const TARGET: &str = "{\"v\":1,\"vv\":{}}";

    fn fixture() -> (Arc<SharedState>, CoreChannelDataSide, tempfile::TempDir) {
        let directory = tempfile::tempdir().expect("temporary WAL directory");
        let wal = Arc::new(
            WalManager::open_for_testing(&directory.path().join("compact.wal")).expect("test WAL"),
        );
        let (dispatcher, mut sides) = Dispatcher::new(1, 64);
        let side = sides.pop().expect("one data side");
        let state = SharedState::new(dispatcher, wal).expect("shared state");
        (state, side, directory)
    }

    /// Answer every request with `status` until `stop` is set, then hand the
    /// data side back for the next responder.
    async fn answer_all(
        state: Arc<SharedState>,
        mut side: CoreChannelDataSide,
        stop: Arc<AtomicBool>,
        status: Status,
    ) -> CoreChannelDataSide {
        let deadline = Instant::now() + Duration::from_secs(10);
        while !stop.load(Ordering::Relaxed) && Instant::now() < deadline {
            if let Ok(request) = side.request_rx.try_pop() {
                let error_code = (status == Status::Error).then(|| {
                    Box::new(ErrorCode::Internal {
                        detail: "injected compaction refusal".into(),
                    })
                });
                side.response_tx
                    .try_push(BridgeResponse {
                        inner: Response {
                            request_id: request.inner.request_id,
                            status,
                            attempt: 1,
                            partial: false,
                            payload: Payload::empty(),
                            watermark_lsn: Lsn::ZERO,
                            error_code,
                            stage_vote: None,
                            read_versions: crate::types::ReadVersions::new(),
                            write_set: Vec::new(),
                        },
                    })
                    .expect("fake data-plane response queue has capacity");
            }
            state.poll_and_route_responses();
            tokio::task::yield_now().await;
        }
        side
    }

    fn owed(
        state: &SharedState,
    ) -> Vec<crate::control::security::catalog::StoredPendingHistoryCompaction> {
        state
            .credentials
            .catalog()
            .load_pending_history_compactions()
            .expect("load owed compactions")
    }

    /// Apply writes the owed row. A fan-out a core refuses leaves it with the
    /// attempt recorded, as a crash before the fan-out leaves it untouched.
    /// The boot drain then compacts and removes it.
    #[tokio::test(flavor = "multi_thread")]
    async fn an_owed_compaction_survives_a_failed_fan_out_and_the_drain_removes_it() {
        let (state, side, _directory) = fixture();
        apply_to(
            &CatalogEntry::CompactHistory {
                tenant_id: 7,
                database_id: 3,
                collection: "docs".to_string(),
                doc_id: "doc-1".to_string(),
                before_timestamp: 100,
                target_version_json: TARGET.to_string(),
            },
            state.credentials.catalog(),
        )
        .expect("apply CompactHistory");
        assert_eq!(owed(&state).len(), 1, "apply records the owed compaction");

        let stop = Arc::new(AtomicBool::new(false));
        let responder = tokio::spawn(answer_all(
            Arc::clone(&state),
            side,
            Arc::clone(&stop),
            Status::Error,
        ));
        let refused = compact_async(3, 7, "docs", TARGET, &state).await;
        stop.store(true, Ordering::Relaxed);
        let side = responder.await.expect("refusing responder");
        assert!(refused.is_err(), "a refused fan-out reports its error");
        let rows = owed(&state);
        assert_eq!(rows.len(), 1, "a refused fan-out keeps the owed row");
        assert_eq!(rows[0].attempts, 1);

        let stop = Arc::new(AtomicBool::new(false));
        let responder = tokio::spawn(answer_all(
            Arc::clone(&state),
            side,
            Arc::clone(&stop),
            Status::Ok,
        ));
        let still_owed = drain_pending_compactions(&state).await;
        stop.store(true, Ordering::Relaxed);
        responder.await.expect("acknowledging responder");
        assert_eq!(still_owed.expect("drain reads the owed rows"), 0);
        assert!(
            owed(&state).is_empty(),
            "the drain removes the compacted row"
        );
    }

    /// The plan names the qualified collection and carries the committed
    /// target verbatim. A rewritten target compacts a node to a version its
    /// peers never agreed on.
    #[test]
    fn compact_plan_carries_the_committed_target() {
        let PhysicalPlan::Crdt(CrdtOp::CompactAtVersion {
            collection,
            target_version_json,
        }) = compact_plan(7, "documents", "{\"n1\":4}".to_string())
        else {
            panic!("compact_plan must build a CrdtOp::CompactAtVersion");
        };
        assert_eq!(collection.as_str(), "7/documents");
        assert_eq!(target_version_json, "{\"n1\":4}");
    }
}
