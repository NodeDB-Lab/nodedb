// SPDX-License-Identifier: BUSL-1.1

//! Encode the resolved apply pass of the two join-driven document writes —
//! autocommit `MERGE` and `UPDATE ... FROM` — into `ReplicatedWrite`.
//!
//! Only the resolved shape replicates: source rows shipped, NOT-MATCHED
//! surrogates pre-assigned, sum targets resolved, write policy decided. The
//! unresolved shape is what every Control-Plane entry point intercepts and
//! never proposes.

use super::super::types::ReplicatedWrite;
use super::document::{WireReturning, wire_target_bindings, wire_targets};
use nodedb_physical::physical_plan::ResolvedSumTarget;
use nodedb_physical::physical_plan::document::MergeClauseOp;

/// The join the two writes share: target, source, alias and join columns.
pub(super) struct JoinFields<'a> {
    pub target_collection: &'a str,
    pub source_collection: &'a str,
    pub source_alias: &'a str,
    pub target_join_col: &'a str,
    pub source_join_col: &'a str,
    pub source_rows: &'a [(String, Vec<u8>)],
    pub resolved_sum_targets: &'a [ResolvedSumTarget],
    pub declared_primary_key: Option<&'a str>,
}

/// The three MERGE-only inputs.
pub(super) struct MergeFields<'a> {
    pub clauses: &'a [MergeClauseOp],
    pub resolved_inserts: &'a [(String, u32)],
    pub resolved_insert_identities: &'a [(String, u32)],
}

pub(super) fn merge_apply(
    join: JoinFields<'_>,
    merge: MergeFields<'_>,
    returning: WireReturning<'_>,
) -> ReplicatedWrite {
    ReplicatedWrite::MergeApply {
        target_collection: join.target_collection.to_owned(),
        source_collection: join.source_collection.to_owned(),
        source_alias: join.source_alias.to_owned(),
        target_join_col: join.target_join_col.to_owned(),
        source_join_col: join.source_join_col.to_owned(),
        clauses: merge.clauses.to_vec(),
        returning: returning.returning,
        resolved_inserts: merge.resolved_inserts.to_vec(),
        resolved_insert_identities: merge.resolved_insert_identities.to_vec(),
        source_rows: join.source_rows.to_vec(),
        rls_filters: returning.rls_filters.to_vec(),
        resolved_sum_targets: wire_targets(join.resolved_sum_targets),
        resolved_sum_target_bindings: wire_target_bindings(join.resolved_sum_targets),
        declared_primary_key: join.declared_primary_key.map(str::to_owned),
    }
}

pub(super) fn update_from_join_apply(
    join: JoinFields<'_>,
    updates: &[(String, nodedb_physical::physical_plan::UpdateValue)],
    target_filters: &[u8],
    returning: WireReturning<'_>,
) -> ReplicatedWrite {
    ReplicatedWrite::UpdateFromJoinApply {
        target_collection: join.target_collection.to_owned(),
        source_collection: join.source_collection.to_owned(),
        source_alias: join.source_alias.to_owned(),
        target_join_col: join.target_join_col.to_owned(),
        source_join_col: join.source_join_col.to_owned(),
        updates: updates.to_vec(),
        target_filters: target_filters.to_vec(),
        returning: returning.returning,
        source_rows: join.source_rows.to_vec(),
        rls_filters: returning.rls_filters.to_vec(),
        resolved_sum_targets: wire_targets(join.resolved_sum_targets),
        resolved_sum_target_bindings: wire_target_bindings(join.resolved_sum_targets),
        declared_primary_key: join.declared_primary_key.map(str::to_owned),
    }
}
