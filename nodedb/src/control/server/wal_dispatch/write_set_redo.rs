// SPDX-License-Identifier: BUSL-1.1

//! The record group of a document or edge write.
//!
//! A write whose stored rows are decided at apply — an update's post-image, a
//! predicate's row set, a versioned row's stamp, an edge version's ordinal —
//! reports every row it stored or removed in
//! `Response::write_set`. The Control Plane journals those rows as the parts
//! of the write's record group (see `wal::redo::group`), in entry order,
//! while the write's order fence is held (see `write_order_fence`), so WAL
//! order matches the order the rows reached storage.
//!
//! Before dispatch the write appends its group's origin and opens the group:
//! its forward record followed by an announcing `WriteGroup` record, or an
//! opening `WriteGroup` record when it has no forward record or when its
//! forward record belongs inside the group. The origin's LSN is the write's
//! LSN on its core, so every event the write emits names it. Every group
//! gets at least one part, so a group whose parts never arrived shows in the
//! WAL.

use crate::bridge::envelope::{EdgeImage, PhysicalPlan, RowEffect, WriteSetEntry};
use crate::types::{DatabaseId, Lsn, TenantId, VShardId};
use crate::wal::manager::WalAppender;
use crate::wal::{RedoSubRecord, WriteGroup, WriteGroupRecord};
use nodedb_physical::physical_plan::{DocumentOp, GraphOp, MetaOp};
use nodedb_wal::record::RecordType;

use super::core::{WalAppendRequest, wal_append};
use super::document::{
    encode_document_delete_record, encode_document_put_record, opening_forward_sub_record,
};
use super::graph::{encode_edge_delete, encode_edge_put};

/// The bytes of a part's sub-records, when one WAL record holds at most
/// `max_payload` bytes. A larger write set spreads over more parts. One
/// sub-record never splits.
fn part_budget(max_payload: usize) -> usize {
    max_payload.saturating_sub(2 * crate::wal::redo::continuation::RECORD_OVERHEAD)
}

/// Return `Some(collection)` when `plan` journals rows *after* the Data Plane
/// applies it, from `Response::write_set`. `None` for a plan that stores no
/// document row and no edge. The collection is the fallback an entry without
/// its own names.
pub fn plan_post_apply_redo(plan: &PhysicalPlan) -> Option<String> {
    match plan {
        PhysicalPlan::Document(op) => document_post_apply_collection(op),
        // An autocommit edge write's version ordinal is decided at apply, so
        // the edge versions it wrote are journalled after apply. Their
        // entries home to the write's own vShard.
        PhysicalPlan::Graph(
            GraphOp::EdgePut { collection, .. } | GraphOp::EdgeDelete { collection, .. },
        ) => Some(collection.to_string()),
        PhysicalPlan::Graph(
            GraphOp::EdgePutBatch { edges } | GraphOp::EdgeDeleteBatch { edges },
        ) => edges.first().map(|edge| edge.collection.to_string()),
        // A committed transaction's materialized-sum folds write rows no
        // redo sub-record names. Each write-set entry names its own target
        // collection, so the transaction's first collection is only the
        // fallback an entry without one will use.
        PhysicalPlan::Meta(MetaOp::ApplyTransactionRedo { collections, .. }) => {
            collections.first().cloned()
        }
        _ => None,
    }
}

/// [`plan_post_apply_redo`] for a document op.
fn document_post_apply_collection(op: &DocumentOp) -> Option<String> {
    match op {
        DocumentOp::PointUpdate { collection, .. }
        | DocumentOp::PointPut { collection, .. }
        | DocumentOp::PointInsert { collection, .. }
        | DocumentOp::PointDelete { collection, .. }
        | DocumentOp::Upsert { collection, .. }
        | DocumentOp::BatchUpsert { collection, .. }
        | DocumentOp::BulkUpdate { collection, .. }
        | DocumentOp::BulkDelete { collection, .. }
        | DocumentOp::BatchInsert { collection, .. }
        | DocumentOp::Truncate { collection, .. }
        | DocumentOp::ApplyBalanceDelta { collection, .. } => Some(collection.to_string()),
        DocumentOp::UpdateFromJoin {
            target_collection, ..
        }
        | DocumentOp::Merge {
            target_collection, ..
        } => Some(target_collection.to_string()),
        // Every entry of a resolved write names its own collection.
        DocumentOp::ResolvedWrite { mutations, .. } => mutations
            .first()
            .map(|mutation| mutation.collection().to_string()),
        // Reads, index DDL and the Control-Plane-expanded INSERT ... SELECT
        // write no row the post-apply redo journals.
        DocumentOp::PointGet { .. }
        | DocumentOp::Scan { .. }
        | DocumentOp::RangeScan { .. }
        | DocumentOp::Register { .. }
        | DocumentOp::IndexLookup { .. }
        | DocumentOp::IndexedFetch { .. }
        | DocumentOp::DropIndex { .. }
        | DocumentOp::BackfillIndex { .. }
        | DocumentOp::EstimateCount { .. }
        | DocumentOp::InsertSelect { .. }
        | DocumentOp::MaterializeScan { .. }
        | DocumentOp::ResolveWrite(_) => None,
    }
}

/// The origin of a write's record group.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct GroupOrigin {
    /// LSN of the origin record. The write runs at this LSN on its core, and
    /// every event it emits names it.
    pub lsn: Lsn,
}

/// What the pre-dispatch append of a grouped write produced.
#[derive(Debug, Clone, Copy)]
pub struct GroupOriginAppend {
    pub origin: GroupOrigin,
    /// See `WalAppendOutcome::resolved_now_ms`.
    pub resolved_now_ms: Option<u64>,
}

/// Append the origin of the group of `req.plan`, a write that journals rows
/// after apply.
///
/// - A point write whose apply always reports rows beyond its own (a delete,
///   a put or insert that folds sum targets) journals its forward record as
///   an opening `WriteGroup` record, so a restore that lacks the parts drops
///   the forward record with them.
/// - Any other plan with a forward record journals it as usual, and that
///   record is the origin. An announcing record follows it at once.
/// - A plan with no forward record journals an opening record with no rows.
pub fn append_group_origin(req: WalAppendRequest<'_>) -> crate::Result<GroupOriginAppend> {
    let wal = req.wal.with_event_source(req.event_source);
    let (tenant_id, vshard_id, database_id) = (req.tenant_id, req.vshard_id, req.database_id);
    let starting = |group: WriteGroup, ops: Vec<RedoSubRecord>| {
        wal.append_write_group(
            tenant_id,
            vshard_id,
            database_id,
            &WriteGroupRecord {
                group,
                ops,
                redo: None,
            },
        )
    };
    if let PhysicalPlan::Document(op) = req.plan
        && let Some(forward) = opening_forward_sub_record(op)?
    {
        let lsn = starting(WriteGroup::OPENING, vec![forward])?;
        return Ok(GroupOriginAppend {
            origin: GroupOrigin { lsn },
            resolved_now_ms: None,
        });
    }
    let outcome = wal_append(req)?;
    let lsn = match outcome.lsn {
        Some(lsn) => {
            starting(WriteGroup::announcing(lsn.as_u64()), Vec::new())?;
            lsn
        }
        None => starting(WriteGroup::OPENING, Vec::new())?,
    };
    Ok(GroupOriginAppend {
        origin: GroupOrigin { lsn },
        resolved_now_ms: outcome.resolved_now_ms,
    })
}

/// Where [`append_group_parts`] journals a write's rows.
#[derive(Debug, Clone, Copy)]
pub struct WriteSetTarget<'a> {
    pub tenant_id: TenantId,
    /// The vShard of the write, and of every entry without its own collection.
    pub vshard_id: VShardId,
    pub database_id: DatabaseId,
    /// The collection of every entry without its own.
    pub collection: &'a str,
    /// The write's group origin.
    pub origin: GroupOrigin,
}

/// The parts a write set journals as, in entry order.
#[derive(Debug)]
pub struct GroupParts {
    /// Each part's vShard and sub-records. Never empty: a write set with no
    /// row to journal gets one part with no rows, which closes the group.
    pub runs: Vec<(VShardId, Vec<RedoSubRecord>)>,
    /// Whether an entry cancels the origin.
    pub cancels_origin: bool,
}

impl GroupParts {
    /// The part count every part announces.
    pub fn count(&self) -> crate::Result<u32> {
        u32::try_from(self.runs.len()).map_err(|_| crate::Error::Internal {
            detail: format!(
                "a write set spreads over {} parts, more than a group counts",
                self.runs.len()
            ),
        })
    }
}

/// Split `write_set` into the parts of the write's group.
///
/// Consecutive entries of one vShard share a part, as long as the part stays
/// within a WAL record of `max_payload` bytes. The split depends on the write
/// set and `max_payload` alone, so boot rebuilds the same parts from a stored
/// write set and the limit stored with it.
///
/// Each sub-record journals `entry.identity` as its `document_id` and
/// `entry.surrogate` in the `u32` slot, so the Event Plane replay names the
/// row the way a live event does and the Data Plane replay keys on the
/// surrogate.
pub fn plan_group_parts(
    target: &WriteSetTarget<'_>,
    write_set: &[WriteSetEntry],
    max_payload: usize,
) -> crate::Result<GroupParts> {
    let budget = part_budget(max_payload);
    let mut runs: Vec<(VShardId, Vec<RedoSubRecord>)> = Vec::new();
    let mut run_bytes = 0usize;
    let mut cancels_origin = false;
    for entry in write_set {
        let Some(sub) = sub_record(entry, target.collection)? else {
            cancels_origin = true;
            continue;
        };
        let vshard = entry_vshard(entry, target)?;
        let cost = sub
            .payload
            .len()
            .saturating_add(crate::wal::redo::continuation::SUB_RECORD_OVERHEAD);
        if cost > budget {
            return Err(crate::Error::Internal {
                detail: format!(
                    "a write-set row of {} bytes exceeds the WAL record limit of \
                     {max_payload} bytes",
                    sub.payload.len()
                ),
            });
        }
        match runs.last_mut() {
            Some((run_vshard, ops))
                if *run_vshard == vshard && run_bytes.saturating_add(cost) <= budget =>
            {
                run_bytes += cost;
                ops.push(sub);
            }
            _ => {
                run_bytes = cost;
                runs.push((vshard, vec![sub]));
            }
        }
    }
    if runs.is_empty() {
        runs.push((target.vshard_id, Vec::new()));
    }
    Ok(GroupParts {
        runs,
        cancels_origin,
    })
}

/// Append the parts of `parts` whose number `present` does not name, and
/// return the last LSN appended. A part number counts from 1.
pub fn append_planned_parts(
    wal: WalAppender<'_>,
    target: &WriteSetTarget<'_>,
    parts: GroupParts,
    present: impl Fn(u32) -> bool,
) -> crate::Result<Option<Lsn>> {
    let count = parts.count()?;
    let origin = target.origin.lsn.as_u64();
    let mut last = None;
    for (part, (vshard, ops)) in (1..=count).zip(parts.runs) {
        if present(part) {
            continue;
        }
        let record = WriteGroupRecord {
            group: WriteGroup::part_of(origin, part, count),
            ops,
            redo: None,
        };
        last =
            Some(wal.append_write_group(target.tenant_id, vshard, target.database_id, &record)?);
    }
    Ok(last)
}

/// Append `write_set` as the parts of the write's group, in entry order, and
/// return the last LSN appended.
///
/// When an entry cancels the origin, a `WriteAborted` marker naming it
/// follows every part, so replay never applies the origin's own rows and a
/// crash before the marker leaves the parts in place.
pub fn append_group_parts(
    wal: WalAppender<'_>,
    target: WriteSetTarget<'_>,
    write_set: &[WriteSetEntry],
) -> crate::Result<Option<Lsn>> {
    let parts = plan_group_parts(&target, write_set, wal.max_payload())?;
    let cancels_origin = parts.cancels_origin;
    let mut last = append_planned_parts(wal, &target, parts, |_| false)?;
    if cancels_origin {
        last = Some(wal.append_write_aborted(
            target.tenant_id,
            target.vshard_id,
            target.database_id,
            target.origin.lsn,
        )?);
    }
    Ok(last)
}

/// The sub-record `entry` journals, `None` for a cancel of the origin.
fn sub_record(entry: &WriteSetEntry, fallback: &str) -> crate::Result<Option<RedoSubRecord>> {
    let collection = entry.collection.as_deref().unwrap_or(fallback);
    let (record_type, payload) = match &entry.effect {
        RowEffect::Put { value, version } => (
            RecordType::Put,
            encode_document_put_record(
                collection,
                entry.identity.as_str(),
                value,
                entry.surrogate,
                *version,
            )?,
        ),
        RowEffect::Delete { system_from } => (
            RecordType::Delete,
            encode_document_delete_record(
                collection,
                entry.identity.as_str(),
                entry.surrogate,
                *system_from,
            )?,
        ),
        RowEffect::CancelForward => return Ok(None),
        RowEffect::Edge(EdgeImage::Put(put)) => (RecordType::Put, encode_edge_put(put.clone())?),
        RowEffect::Edge(EdgeImage::Delete(delete)) => {
            (RecordType::Delete, encode_edge_delete(delete.clone())?)
        }
    };
    Ok(Some(RedoSubRecord {
        record_type: record_type as u32,
        payload,
    }))
}

/// The vShard `entry`'s row homes to. A cross-collection entry homes to its
/// own collection's vShard.
fn entry_vshard(entry: &WriteSetEntry, target: &WriteSetTarget<'_>) -> crate::Result<VShardId> {
    match &entry.collection {
        // A write-set entry names its storage collection, the qualified name.
        Some(collection) => Ok(nodedb_types::CollectionKey::from_qualified_str(
            target.database_id,
            collection,
        )?
        .vshard()),
        None => Ok(target.vshard_id),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::wal::manager::{NO_APPLY_KEY, WalManager};
    use nodedb_physical::physical_plan::ReturningSpec;
    use nodedb_types::sync::wire::SyncProvenance;
    use nodedb_types::{QualifiedCollection, RowIdentity, Surrogate};

    fn open_wal(dir: &std::path::Path) -> WalManager {
        WalManager::open_for_testing(&dir.join("test.wal")).expect("open wal")
    }

    fn appender(wal: &WalManager) -> WalAppender<'_> {
        wal.appender(NO_APPLY_KEY)
            .with_event_source(crate::event::EventSource::User)
    }

    /// Every `WriteGroup` record replay reads, with its LSN.
    fn groups(wal: &WalManager) -> Vec<(u64, WriteGroupRecord)> {
        wal.sync().expect("sync wal");
        wal.replay()
            .expect("read wal")
            .into_iter()
            .filter(|r| {
                RecordType::from_raw(r.logical_record_type()) == Some(RecordType::WriteGroup)
            })
            .map(|r| {
                (
                    r.header.lsn,
                    WriteGroupRecord::from_bytes(&r.payload).expect("decode group record"),
                )
            })
            .collect()
    }

    /// The origin of a write whose forward record is at `lsn`.
    fn forward(lsn: Lsn) -> GroupOrigin {
        GroupOrigin { lsn }
    }

    fn docs_target(origin: GroupOrigin) -> WriteSetTarget<'static> {
        WriteSetTarget {
            tenant_id: TenantId::new(1),
            vshard_id: VShardId::new(0),
            database_id: DatabaseId::DEFAULT,
            collection: "docs",
            origin,
        }
    }

    fn put_forward(wal: &WalManager) -> Lsn {
        appender(wal)
            .append_put(
                TenantId::new(1),
                VShardId::new(0),
                DatabaseId::DEFAULT,
                b"forward",
            )
            .expect("append forward")
    }

    #[test]
    fn point_update_is_post_apply_redo() {
        let plan = PhysicalPlan::Document(DocumentOp::PointUpdate {
            collection: QualifiedCollection::new(DatabaseId::DEFAULT, "docs"),
            document_id: "d1".to_string(),
            surrogate: Some(Surrogate::new(1)),
            pk_bytes: Vec::new(),
            updates: Vec::new(),
            returning: None::<ReturningSpec>,
            rls_filters: Vec::new(),
            rls_write_check: nodedb_types::RlsWriteCheck::pending_injection(),
            resolved_sum_targets: Vec::new(),
            declared_primary_key: None,
        });
        assert_eq!(plan_post_apply_redo(&plan).as_deref(), Some("docs"));
    }

    #[test]
    fn bulk_update_is_post_apply_redo() {
        let plan = PhysicalPlan::Document(DocumentOp::BulkUpdate {
            collection: QualifiedCollection::new(DatabaseId::DEFAULT, "docs"),
            filters: Vec::new(),
            updates: Vec::new(),
            returning: None::<ReturningSpec>,
            ollp_predicted_surrogates: None,
            ollp_predicted_edges: None,
            rls_filters: Vec::new(),
            rls_write_check: nodedb_types::RlsWriteCheck::pending_injection(),
            resolved_sum_targets: Vec::new(),
            declared_primary_key: None,
        });
        assert_eq!(plan_post_apply_redo(&plan).as_deref(), Some("docs"));
    }

    #[test]
    fn point_get_is_not_post_apply_redo() {
        let plan = PhysicalPlan::Document(DocumentOp::PointGet {
            collection: QualifiedCollection::new(DatabaseId::DEFAULT, "docs"),
            document_id: "d1".to_string(),
            surrogate: None,
            pk_bytes: Vec::new(),
            rls_filters: Vec::new(),
            system_time: nodedb_types::SystemTimeScope::Current,
            valid_at_ms: None,
        });
        assert!(plan_post_apply_redo(&plan).is_none());
    }

    /// A put and a delete journal in one part, in the shapes the document
    /// replay decodes, naming the forward record as origin.
    #[test]
    fn row_entries_journal_as_one_part_of_the_forward_record() {
        let dir = tempfile::tempdir().expect("tempdir");
        let wal = open_wal(dir.path());
        let origin = put_forward(&wal);
        let entries = vec![
            WriteSetEntry::put(9, RowIdentity::from_user_key("order-1"), vec![1, 2, 3]),
            WriteSetEntry::delete(10, RowIdentity::for_surrogate(Surrogate::new(10))),
        ];
        append_group_parts(appender(&wal), docs_target(forward(origin)), &entries)
            .expect("append")
            .expect("a part is appended");

        let parts = groups(&wal);
        assert_eq!(parts.len(), 1);
        let (_, part) = &parts[0];
        assert_eq!(part.group, WriteGroup::part_of(origin.as_u64(), 1, 1));
        let (collection, document_id, value, _prov, surrogate) =
            zerompk::from_msgpack::<(String, String, Vec<u8>, Option<SyncProvenance>, u32)>(
                &part.ops[0].payload,
            )
            .expect("decode put");
        assert_eq!(
            (collection.as_str(), document_id.as_str(), value, surrogate),
            ("docs", "order-1", vec![1, 2, 3], 9),
            "a declared key journals verbatim"
        );
        let (_, document_id, _prov, surrogate) =
            zerompk::from_msgpack::<(String, String, Option<SyncProvenance>, u32)>(
                &part.ops[1].payload,
            )
            .expect("decode delete");
        assert_eq!((document_id.as_str(), surrogate), ("10", 10));
    }

    /// An empty write set closes its group with one part with no rows.
    #[test]
    fn an_empty_write_set_closes_its_group() {
        let dir = tempfile::tempdir().expect("tempdir");
        let wal = open_wal(dir.path());
        let origin = put_forward(&wal);
        append_group_parts(appender(&wal), docs_target(forward(origin)), &[])
            .expect("append")
            .expect("the closing part is appended");
        let records = groups(&wal);
        assert_eq!(records.len(), 1);
        assert_eq!(
            records[0].1.group,
            WriteGroup::part_of(origin.as_u64(), 1, 1)
        );
        assert!(records[0].1.ops.is_empty());
    }

    /// A plan with a forward record journals it, then announces its group. A
    /// point put that folds no sum target carries its row in its forward
    /// record.
    #[test]
    fn a_forward_origin_is_announced() {
        let dir = tempfile::tempdir().expect("tempdir");
        let wal = open_wal(dir.path());
        let plan = PhysicalPlan::Document(DocumentOp::PointPut {
            collection: QualifiedCollection::new(DatabaseId::DEFAULT, "docs"),
            document_id: "d1".to_string(),
            value: vec![0x80],
            surrogate: Surrogate::new(1),
            pk_bytes: Vec::new(),
            returning: None::<ReturningSpec>,
            rls_filters: Vec::new(),
            resolved_sum_targets: Vec::new(),
        });
        let appended = append_group_origin(WalAppendRequest {
            wal: appender(&wal),
            event_source: crate::event::EventSource::User,
            tenant_id: TenantId::new(1),
            vshard_id: VShardId::new(0),
            database_id: DatabaseId::DEFAULT,
            plan: &plan,
            credentials: None,
            now_override: None,
        })
        .expect("origin");
        let records = groups(&wal);
        assert_eq!(
            records.len(),
            1,
            "the announcement follows the forward record"
        );
        assert!(records[0].0 > appended.origin.lsn.as_u64());
        assert_eq!(
            records[0].1.group,
            WriteGroup::announcing(appended.origin.lsn.as_u64())
        );
        let mut membership = crate::wal::GroupMembership::default();
        membership.observe(records[0].0, records[0].1.group);
        assert_eq!(
            membership.broken_through(u64::MAX),
            vec![appended.origin.lsn.as_u64(), records[0].0],
            "an announced group without parts is broken, its announcement with it"
        );
    }

    /// Appending a planned group again with its present parts skipped adds
    /// only the missing ones, with the same numbering.
    #[test]
    fn only_missing_parts_are_appended_again() {
        let dir = tempfile::tempdir().expect("tempdir");
        let wal = open_wal(dir.path());
        let origin = put_forward(&wal);
        let target = docs_target(forward(origin));
        let entries = vec![
            WriteSetEntry::put(1, RowIdentity::from_user_key("a"), vec![1; 8]),
            WriteSetEntry::put(2, RowIdentity::from_user_key("t"), vec![2; 8])
                .in_collection("totals".to_string()),
        ];
        let parts = plan_group_parts(&target, &entries, wal.max_payload()).expect("plan");
        let count = parts.count().expect("count");
        append_planned_parts(appender(&wal), &target, parts, |part| part == 1).expect("append");
        let records = groups(&wal);
        let expected = if count == 1 { 0 } else { 1 };
        assert_eq!(records.len(), expected);
        if let Some((_, record)) = records.first() {
            assert_eq!(record.group, WriteGroup::part_of(origin.as_u64(), 2, count));
        }
    }

    /// An opening origin always gets a part, which closes its group.
    #[test]
    fn an_opening_origin_with_no_rows_gets_a_closing_part() {
        let dir = tempfile::tempdir().expect("tempdir");
        let wal = open_wal(dir.path());
        let plan = PhysicalPlan::Document(DocumentOp::BulkUpdate {
            collection: QualifiedCollection::new(DatabaseId::DEFAULT, "docs"),
            filters: Vec::new(),
            updates: Vec::new(),
            returning: None,
            ollp_predicted_surrogates: None,
            ollp_predicted_edges: None,
            rls_filters: Vec::new(),
            rls_write_check: nodedb_types::RlsWriteCheck::NoPolicyApplies,
            resolved_sum_targets: Vec::new(),
            declared_primary_key: None,
        });
        let appended = append_group_origin(WalAppendRequest {
            wal: appender(&wal),
            event_source: crate::event::EventSource::User,
            tenant_id: TenantId::new(1),
            vshard_id: VShardId::new(0),
            database_id: DatabaseId::DEFAULT,
            plan: &plan,
            credentials: None,
            now_override: None,
        })
        .expect("origin");
        append_group_parts(appender(&wal), docs_target(appended.origin), &[]).expect("parts");

        let records = groups(&wal);
        assert_eq!(records.len(), 2);
        assert_eq!(records[0].0, appended.origin.lsn.as_u64());
        assert!(records[0].1.group.opens() && records[0].1.ops.is_empty());
        assert_eq!(
            records[1].1.group,
            WriteGroup::part_of(appended.origin.lsn.as_u64(), 1, 1)
        );
        assert!(records[1].1.ops.is_empty());
    }

    /// A point delete journals its forward record inside the opening record.
    #[test]
    fn a_point_delete_opens_its_group_with_its_forward_record() {
        let dir = tempfile::tempdir().expect("tempdir");
        let wal = open_wal(dir.path());
        let plan = PhysicalPlan::Document(DocumentOp::PointDelete {
            collection: QualifiedCollection::new(DatabaseId::DEFAULT, "docs"),
            document_id: "d1".into(),
            surrogate: Some(Surrogate::new(4)),
            pk_bytes: Vec::new(),
            returning: None,
            rls_filters: Vec::new(),
            rls_write_check: nodedb_types::RlsWriteCheck::NoPolicyApplies,
            resolved_sum_targets: Vec::new(),
        });
        let appended = append_group_origin(WalAppendRequest {
            wal: appender(&wal),
            event_source: crate::event::EventSource::User,
            tenant_id: TenantId::new(1),
            vshard_id: VShardId::new(0),
            database_id: DatabaseId::DEFAULT,
            plan: &plan,
            credentials: None,
            now_override: None,
        })
        .expect("origin");
        let records = groups(&wal);
        assert_eq!(records.len(), 1);
        assert_eq!(
            records[0].0,
            appended.origin.lsn.as_u64(),
            "the opening record is the group's origin"
        );
        assert!(appended.resolved_now_ms.is_none());
        let opening = &records[0].1;
        assert!(opening.group.opens());
        assert_eq!(opening.ops[0].record_type, RecordType::Delete as u32);
        let (_, document_id, _prov, surrogate) =
            zerompk::from_msgpack::<(String, String, Option<SyncProvenance>, u32)>(
                &opening.ops[0].payload,
            )
            .expect("decode delete");
        assert_eq!((document_id.as_str(), surrogate), ("d1", 4));
    }

    /// Entries of two vShards journal as two parts, each announcing both.
    #[test]
    fn entries_of_another_vshard_take_their_own_part() {
        let dir = tempfile::tempdir().expect("tempdir");
        let wal = open_wal(dir.path());
        let origin = put_forward(&wal);
        let target_collection = "totals";
        let entries = vec![
            WriteSetEntry::put(1, RowIdentity::from_user_key("a"), vec![1]),
            WriteSetEntry::put(2, RowIdentity::from_user_key("t"), vec![2])
                .in_collection(target_collection.to_string()),
        ];
        let expected =
            nodedb_types::CollectionKey::from_qualified_str(DatabaseId::DEFAULT, target_collection)
                .expect("key")
                .vshard();
        append_group_parts(appender(&wal), docs_target(forward(origin)), &entries).expect("append");
        wal.sync().expect("sync");
        let headers: Vec<(u32, WriteGroup)> = wal
            .replay()
            .expect("read")
            .into_iter()
            .filter(|r| {
                RecordType::from_raw(r.logical_record_type()) == Some(RecordType::WriteGroup)
            })
            .map(|r| {
                (
                    r.header.vshard_id,
                    WriteGroupRecord::from_bytes(&r.payload)
                        .expect("decode")
                        .group,
                )
            })
            .collect();
        let expected_parts = if expected == VShardId::new(0) { 1 } else { 2 };
        assert_eq!(headers.len(), expected_parts);
        assert!(
            headers
                .iter()
                .all(|(_, group)| group.parts as usize == expected_parts)
        );
        assert_eq!(headers.last().map(|(v, _)| *v), Some(expected.as_u32()));
    }

    /// A versioned image journals its version key, so replay installs the row
    /// at the key the live write used.
    #[test]
    fn a_versioned_image_journals_its_version_key() {
        let dir = tempfile::tempdir().expect("tempdir");
        let wal = open_wal(dir.path());
        let origin = put_forward(&wal);
        let entries = vec![
            WriteSetEntry::put(9, RowIdentity::from_user_key("r"), vec![1])
                .versioned(Some(crate::bridge::envelope::RowVersion::open(1_000))),
            WriteSetEntry::delete(10, RowIdentity::from_user_key("s"))
                .versioned(Some(crate::bridge::envelope::RowVersion::open(2_000))),
        ];
        append_group_parts(appender(&wal), docs_target(forward(origin)), &entries).expect("append");

        let parts = groups(&wal);
        type VersionedPut = (
            String,
            String,
            Vec<u8>,
            Option<SyncProvenance>,
            u32,
            i64,
            i64,
            i64,
        );
        let (_, _, _, _, surrogate, sys, valid_from, valid_until) =
            zerompk::from_msgpack::<VersionedPut>(&parts[0].1.ops[0].payload)
                .expect("decode versioned put");
        assert_eq!(
            (surrogate, sys, valid_from, valid_until),
            (9, 1_000, i64::MIN, i64::MAX)
        );
        let (_, _, _, surrogate, sys) =
            zerompk::from_msgpack::<(String, String, Option<SyncProvenance>, u32, i64)>(
                &parts[0].1.ops[1].payload,
            )
            .expect("decode versioned delete");
        assert_eq!((surrogate, sys), (10, 2_000));
    }

    /// A cancelled forward record is dropped from replay, and the part that
    /// replaces it is appended before the marker.
    #[test]
    fn a_cancelled_forward_record_is_dropped_after_its_part() {
        let dir = tempfile::tempdir().expect("tempdir");
        let wal = open_wal(dir.path());
        let origin = put_forward(&wal);
        let entries = vec![
            WriteSetEntry::put(9, RowIdentity::from_user_key("r"), vec![7])
                .versioned(Some(crate::bridge::envelope::RowVersion::open(5))),
            WriteSetEntry::cancel_forward(9, RowIdentity::from_user_key("r")),
        ];
        let last = append_group_parts(appender(&wal), docs_target(forward(origin)), &entries)
            .expect("append")
            .expect("the part and the marker are appended");
        assert!(last > origin);

        wal.sync().expect("sync wal");
        let replayed = wal.replay().expect("read wal");
        assert!(
            !replayed.iter().any(|r| r.header.lsn == origin.as_u64()),
            "the forward record is cancelled"
        );
        assert_eq!(groups(&wal).len(), 1, "the part replays in its place");
    }

    /// A write that reports only a cancel closes its group and cancels its
    /// origin.
    #[test]
    fn an_unapplied_write_closes_its_group_and_cancels_its_origin() {
        let dir = tempfile::tempdir().expect("tempdir");
        let wal = open_wal(dir.path());
        let origin = put_forward(&wal);
        append_group_parts(
            appender(&wal),
            docs_target(forward(origin)),
            &[WriteSetEntry::cancel_forward(
                9,
                RowIdentity::from_user_key("r"),
            )],
        )
        .expect("append");
        wal.sync().expect("sync wal");
        let replayed = wal.replay().expect("read wal");
        assert!(!replayed.iter().any(|r| r.header.lsn == origin.as_u64()));
        let records = groups(&wal);
        assert_eq!(records.len(), 1, "one closing part");
        assert!(records[0].1.ops.is_empty());
    }
}
