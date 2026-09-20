// SPDX-License-Identifier: BUSL-1.1

//! Decode the resolved apply pass of autocommit `MERGE` and `UPDATE ... FROM`
//! back into `PhysicalPlan::Document`.
//!
//! The write policy was decided on the proposing node over the resolved arms,
//! so every rebuilt plan stamps `decided_earlier_in_request()`.

use super::document::{ReturningFields, WireSumResolution, plan_targets};
use crate::bridge::envelope::PhysicalPlan;
use nodedb_physical::physical_plan::document::MergeClauseOp;
use nodedb_physical::physical_plan::{DocumentOp, UpdateValue};

/// The join the two writes share, as the record carries it.
pub(super) struct JoinFields<'a> {
    pub target_collection: &'a str,
    pub source_collection: &'a str,
    pub source_alias: &'a str,
    pub target_join_col: &'a str,
    pub source_join_col: &'a str,
    pub source_rows: &'a [(String, Vec<u8>)],
    pub resolved_sum_targets: &'a WireSumResolution<'a>,
    pub declared_primary_key: Option<String>,
}

/// The three MERGE-only inputs, as the record carries them.
pub(super) struct MergeFields<'a> {
    pub clauses: &'a [MergeClauseOp],
    pub resolved_inserts: &'a [(String, u32)],
    pub resolved_insert_identities: &'a [(String, u32)],
}

pub(super) fn merge_apply(
    join: JoinFields<'_>,
    merge: MergeFields<'_>,
    returning: ReturningFields<'_>,
) -> PhysicalPlan {
    PhysicalPlan::Document(DocumentOp::Merge {
        target_collection: nodedb_types::QualifiedCollection::from_stored(
            join.target_collection.to_owned(),
        ),
        source_collection: nodedb_types::QualifiedCollection::from_stored(
            join.source_collection.to_owned(),
        ),
        source_alias: join.source_alias.to_owned(),
        target_join_col: join.target_join_col.to_owned(),
        source_join_col: join.source_join_col.to_owned(),
        clauses: merge.clauses.to_vec(),
        returning: returning.returning,
        resolved_inserts: Some(merge.resolved_inserts.to_vec()),
        resolved_insert_identities: merge.resolved_insert_identities.to_vec(),
        source_rows: Some(join.source_rows.to_vec()),
        rls_filters: returning.rls_filters.to_vec(),
        rls_write_check: nodedb_types::RlsWriteCheck::decided_earlier_in_request(),
        resolved_sum_targets: plan_targets(join.resolved_sum_targets),
        declared_primary_key: join.declared_primary_key,
    })
}

pub(super) fn update_from_join_apply(
    join: JoinFields<'_>,
    updates: &[(String, UpdateValue)],
    target_filters: &[u8],
    returning: ReturningFields<'_>,
) -> PhysicalPlan {
    PhysicalPlan::Document(DocumentOp::UpdateFromJoin {
        target_collection: nodedb_types::QualifiedCollection::from_stored(
            join.target_collection.to_owned(),
        ),
        source_collection: nodedb_types::QualifiedCollection::from_stored(
            join.source_collection.to_owned(),
        ),
        source_alias: join.source_alias.to_owned(),
        target_join_col: join.target_join_col.to_owned(),
        source_join_col: join.source_join_col.to_owned(),
        updates: updates.to_vec(),
        target_filters: target_filters.to_vec(),
        returning: returning.returning,
        source_rows: Some(join.source_rows.to_vec()),
        rls_filters: returning.rls_filters.to_vec(),
        rls_write_check: nodedb_types::RlsWriteCheck::decided_earlier_in_request(),
        resolved_sum_targets: plan_targets(join.resolved_sum_targets),
        declared_primary_key: join.declared_primary_key,
    })
}

#[cfg(test)]
mod tests {
    use crate::bridge::envelope::PhysicalPlan;
    use crate::control::wal_replication::decode;
    use crate::types::{DatabaseId, TenantId, VShardId};
    use nodedb_physical::physical_plan::document::{
        MergeActionOp, MergeClauseKindOp, MergeClauseOp,
    };
    use nodedb_physical::physical_plan::{DocumentOp, ResolvedSumTarget, UpdateValue};
    use nodedb_types::{QualifiedCollection, RlsWriteCheck, Surrogate};

    /// Decide + encode + decode in one call.
    fn roundtrip(plan: &PhysicalPlan) -> PhysicalPlan {
        let write = crate::control::wal_replication::ReplicableWrite::decide_for_replication(plan)
            .expect("a decided write replicates");
        let entry = crate::control::wal_replication::encode::to_replicated_entry(
            TenantId::new(1),
            DatabaseId::DEFAULT,
            VShardId::new(0),
            &write,
        )
        .expect("encode must not error")
        .expect("the resolved shape replicates");
        let (_, _, decoded, _) = decode::from_replicated_entry(&entry.to_bytes(), None)
            .expect("from_replicated_entry error")
            .expect("from_replicated_entry returned None");
        decoded
    }

    fn sum_target() -> ResolvedSumTarget {
        ResolvedSumTarget::new("ledger", "acct-1", Surrogate::new(77))
    }

    #[test]
    fn resolved_merge_apply_roundtrips() {
        let plan = PhysicalPlan::Document(DocumentOp::Merge {
            target_collection: QualifiedCollection::new(DatabaseId::DEFAULT, "t"),
            source_collection: QualifiedCollection::new(DatabaseId::DEFAULT, "s"),
            source_alias: "s".into(),
            target_join_col: "id".into(),
            source_join_col: "id".into(),
            clauses: vec![MergeClauseOp {
                kind: MergeClauseKindOp::NotMatched,
                extra_predicate: Vec::new(),
                action: MergeActionOp::Insert {
                    columns: vec!["id".into(), "v".into()],
                    values: vec![UpdateValue::Literal(vec![0xa2, 0x6b, 0x32])],
                },
            }],
            returning: None,
            resolved_inserts: Some(vec![("k2".into(), 9001)]),
            resolved_insert_identities: vec![("k2".into(), 9001)],
            source_rows: Some(vec![(
                "k2".into(),
                vec![0x81, 0xa1, 0x76, 0xa3, 0x74, 0x77, 0x6f],
            )]),
            rls_filters: vec![1, 2],
            rls_write_check: RlsWriteCheck::decided_earlier_in_request(),
            resolved_sum_targets: vec![sum_target()],
            declared_primary_key: Some("id".into()),
        });
        let decoded = roundtrip(&plan);
        let PhysicalPlan::Document(DocumentOp::Merge {
            resolved_inserts,
            resolved_insert_identities,
            source_rows,
            rls_write_check,
            resolved_sum_targets,
            declared_primary_key,
            clauses,
            rls_filters,
            ..
        }) = decoded
        else {
            panic!("expected Document(Merge)");
        };
        assert_eq!(resolved_inserts, Some(vec![("k2".to_owned(), 9001)]));
        assert_eq!(resolved_insert_identities, vec![("k2".to_owned(), 9001)]);
        assert_eq!(source_rows.map(|r| r.len()), Some(1));
        assert_eq!(rls_write_check, RlsWriteCheck::decided_earlier_in_request());
        assert_eq!(resolved_sum_targets, vec![sum_target()]);
        assert_eq!(declared_primary_key.as_deref(), Some("id"));
        assert_eq!(clauses.len(), 1);
        assert_eq!(rls_filters, vec![1, 2]);
    }

    #[test]
    fn resolved_update_from_join_apply_roundtrips() {
        let plan = PhysicalPlan::Document(DocumentOp::UpdateFromJoin {
            target_collection: QualifiedCollection::new(DatabaseId::DEFAULT, "t"),
            source_collection: QualifiedCollection::new(DatabaseId::DEFAULT, "s"),
            source_alias: "s".into(),
            target_join_col: "id".into(),
            source_join_col: "id".into(),
            updates: vec![(
                "v".into(),
                UpdateValue::Literal(vec![0xa3, 0x6e, 0x65, 0x77]),
            )],
            target_filters: vec![7],
            returning: None,
            source_rows: Some(vec![("k1".into(), vec![0x80])]),
            rls_filters: Vec::new(),
            rls_write_check: RlsWriteCheck::decided_earlier_in_request(),
            resolved_sum_targets: vec![sum_target()],
            declared_primary_key: None,
        });
        let decoded = roundtrip(&plan);
        let PhysicalPlan::Document(DocumentOp::UpdateFromJoin {
            updates,
            target_filters,
            source_rows,
            rls_write_check,
            resolved_sum_targets,
            ..
        }) = decoded
        else {
            panic!("expected Document(UpdateFromJoin)");
        };
        assert_eq!(updates.len(), 1);
        assert_eq!(target_filters, vec![7]);
        assert_eq!(source_rows.map(|r| r.len()), Some(1));
        assert_eq!(rls_write_check, RlsWriteCheck::decided_earlier_in_request());
        assert_eq!(resolved_sum_targets, vec![sum_target()]);
    }
}
