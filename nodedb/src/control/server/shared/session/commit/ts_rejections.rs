// SPDX-License-Identifier: BUSL-1.1

//! Timeseries lines the COMMIT-time resolve rejects beyond what the
//! transaction's statements reported.
//!
//! Each staged timeseries ingest reports the lines a stage-time preview
//! rejects. The preview chains the schemas of the transaction's earlier
//! staged ingests into the same collection, the chain the COMMIT-time resolve
//! follows, so both judge each line against the same schema unless a
//! concurrent write changed the live schema in between. The install is
//! authoritative: it rejects the lines the COMMIT-time resolve rejected plus
//! the rows that conflict with the schema at its log position, and the apply
//! answers those counts. When they exceed what the preview reported,
//! COMMIT raises a statement notice per collection, naming the extra rejected
//! count. pgwire sends it as a `NoticeResponse`, and the native protocol adds
//! it to the response's `warnings`.

use std::collections::BTreeMap;

use nodedb_physical::physical_plan::{PhysicalPlan, TimeseriesOp};
use nodedb_physical::physical_task::PhysicalTask;
use nodedb_wal::record::RecordType;

use crate::engine::timeseries::resolved_ingest::{RESOLVED_INGEST_FORMAT, ResolvedTsBatch};
use crate::wal::RedoRecord;

/// Rejected line counts, by collection.
pub(in crate::control::server::shared::session) type RejectedByCollection = BTreeMap<String, u64>;

/// The lines the stage-time previews of `buffered` rejected, by collection.
/// `preview` holds each ingest's count by its index into `buffered`.
pub(in crate::control::server::shared::session) fn preview_by_collection(
    buffered: &[PhysicalTask],
    preview: &BTreeMap<usize, u64>,
) -> RejectedByCollection {
    let mut out = RejectedByCollection::new();
    for (index, rejected) in preview {
        if let Some(PhysicalPlan::Timeseries(TimeseriesOp::Ingest { collection, .. })) =
            buffered.get(*index).map(|task| &task.plan)
        {
            *out.entry(collection.as_str().to_owned()).or_default() += rejected;
        }
    }
    out
}

/// The lines the resolve of every resolved timeseries ingest in `tasks`
/// rejected, by collection.
pub(in crate::control::server::shared::session) fn tasks_rejected_by_collection(
    tasks: &[PhysicalTask],
) -> crate::Result<RejectedByCollection> {
    let mut out = RejectedByCollection::new();
    for task in tasks {
        let PhysicalPlan::Timeseries(TimeseriesOp::Ingest { collection, .. }) = &task.plan else {
            continue;
        };
        let rejected = crate::control::write_resolve::rejected_lines(&task.plan)?;
        if rejected > 0 {
            *out.entry(collection.as_str().to_owned()).or_default() += rejected;
        }
    }
    Ok(out)
}

/// The lines the resolve behind every `ts-resolved` sub-record of `redo`
/// rejected, by collection.
pub(in crate::control::server::shared::session) fn redo_rejected_by_collection(
    redo: &RedoRecord,
) -> RejectedByCollection {
    let mut out = RejectedByCollection::new();
    for op in &redo.ops {
        if op.record_type != RecordType::TimeseriesBatch as u32 {
            continue;
        }
        let Ok(record) = crate::wal::decode_batch_record(&op.payload) else {
            continue;
        };
        if record.format.as_deref() != Some(RESOLVED_INGEST_FORMAT) {
            continue;
        }
        if let Ok(batch) = ResolvedTsBatch::from_bytes(&record.payload)
            && batch.rejected > 0
        {
            *out.entry(record.collection).or_default() += batch.rejected;
        }
    }
    out
}

/// The lines and rows the timeseries installs an applied answer reports
/// rejected, by collection. Empty when `payload` carries no install counts.
pub(in crate::control::server::shared::session) fn applied_rejected_by_collection(
    payload: &[u8],
) -> RejectedByCollection {
    crate::engine::timeseries::install_counts::TsInstallCounts::from_payload(payload)
        .map(|counts| {
            counts
                .by_collection()
                .into_iter()
                .map(|(collection, (_, rejected))| (collection, rejected))
                .collect()
        })
        .unwrap_or_default()
}

/// `resolved` with each collection an apply reported taking the larger
/// count. An install's count is its resolve's plus the rows it rejected at
/// its log position, so it never falls below the resolve's.
pub(in crate::control::server::shared::session) fn with_applied(
    mut resolved: RejectedByCollection,
    applied: &RejectedByCollection,
) -> RejectedByCollection {
    for (collection, rejected) in applied {
        let held = resolved.entry(collection.clone()).or_default();
        *held = (*held).max(*rejected);
    }
    resolved
}

/// The notices a committed transaction owes: one per collection whose
/// COMMIT-time resolve rejected more lines than its statements reported.
pub(in crate::control::server::shared::session) fn commit_rejection_notices(
    preview: &RejectedByCollection,
    committed: &RejectedByCollection,
) -> Vec<String> {
    committed
        .iter()
        .filter_map(|(collection, rejected)| {
            let reported = preview.get(collection).copied().unwrap_or(0);
            let extra = rejected.saturating_sub(reported);
            (extra > 0).then(|| {
                format!(
                    "COMMIT rejected {extra} line(s) of timeseries collection '{collection}' \
                     beyond the {reported} its statements reported: a concurrent write \
                     changed the collection schema after they were staged"
                )
            })
        })
        .collect()
}

/// Raise the notices [`commit_rejection_notices`] names for the statement
/// now running.
pub(in crate::control::server::shared::session) fn raise_commit_rejections(
    preview: &RejectedByCollection,
    committed: &RejectedByCollection,
) {
    for notice in commit_rejection_notices(preview, committed) {
        super::super::statement_notice::raise(notice);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn counts(entries: &[(&str, u64)]) -> RejectedByCollection {
        entries
            .iter()
            .map(|(collection, rejected)| ((*collection).to_owned(), *rejected))
            .collect()
    }

    /// A COMMIT that rejects more lines than the statements reported names
    /// the collection and the extra count. One that rejects no more raises
    /// nothing.
    #[test]
    fn the_apply_count_wins_over_the_resolve_count() {
        use crate::engine::timeseries::install_counts::{TsInstallCount, TsInstallCounts};
        let payload = TsInstallCounts::new(vec![TsInstallCount {
            collection: "metrics".into(),
            accepted: 1,
            rejected: 3,
        }])
        .to_bytes()
        .expect("encode install counts");
        let applied = applied_rejected_by_collection(&payload);
        let committed = with_applied(counts(&[("metrics", 1), ("cpu", 2)]), &applied);
        assert_eq!(committed, counts(&[("metrics", 3), ("cpu", 2)]));
        assert!(applied_rejected_by_collection(&[]).is_empty());
    }

    #[test]
    fn only_lines_rejected_beyond_the_preview_raise_a_notice() {
        let preview = counts(&[("metrics", 1), ("cpu", 2)]);
        let committed = counts(&[("metrics", 3), ("cpu", 2)]);
        let notices = commit_rejection_notices(&preview, &committed);
        assert_eq!(notices.len(), 1, "{notices:?}");
        assert!(notices[0].contains("'metrics'"), "{}", notices[0]);
        assert!(notices[0].contains("rejected 2 line(s)"), "{}", notices[0]);

        assert!(commit_rejection_notices(&committed, &committed).is_empty());
    }

    /// A resolved ingest in a sequenced transaction's tasks counts its
    /// resolve's rejected lines against its collection.
    #[test]
    fn resolved_tasks_count_their_rejected_lines() {
        let batch = ResolvedTsBatch {
            measurement: "metrics".into(),
            columns: Vec::new(),
            timestamp_idx: 0,
            drift: crate::engine::timeseries::resolved_ingest::TsDriftPolicy::ApplyByName,
            now_ms: 0,
            resolved_bytes: 0,
            emits_events: false,
            rows: Vec::new(),
            rejected: 2,
            first_rejection: Some("type conflict".into()),
            carried_system: false,
        };
        let task = PhysicalTask {
            tenant_id: crate::types::TenantId::new(1),
            vshard_id: crate::types::VShardId::new(0),
            database_id: crate::types::DatabaseId::DEFAULT,
            plan: PhysicalPlan::Timeseries(TimeseriesOp::Ingest {
                collection: nodedb_types::QualifiedCollection::new(
                    crate::types::DatabaseId::DEFAULT,
                    "metrics",
                ),
                payload: batch.to_bytes().expect("encode batch"),
                format: RESOLVED_INGEST_FORMAT.to_owned(),
                wal_lsn: None,
                surrogates: Vec::new(),
                provenance: None,
                rls_write_check: nodedb_types::RlsWriteCheck::decided_earlier_in_request(),
                returning: None,
                rls_filters: Vec::new(),
            }),
            post_set_op: nodedb_physical::physical_task::PostSetOp::None,
            txn_id: None,
        };
        let committed = tasks_rejected_by_collection(std::slice::from_ref(&task))
            .expect("count rejected lines");
        let collection =
            nodedb_types::QualifiedCollection::new(crate::types::DatabaseId::DEFAULT, "metrics");
        let collection = collection.as_str();
        assert_eq!(committed.get(collection), Some(&2));
        assert_eq!(
            preview_by_collection(
                std::slice::from_ref(&task),
                &BTreeMap::from([(0usize, 1u64)])
            )
            .get(collection),
            Some(&1)
        );
    }
}
