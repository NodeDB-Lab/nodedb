// SPDX-License-Identifier: BUSL-1.1

//! Which rows of an insert that skips a conflict read their stored row.
//!
//! An `INSERT .. ON CONFLICT DO NOTHING` lands a row only when no row is
//! stored. [`super::stored`] reads that row and the settlement credits the
//! balance only for a row that lands.
//!
//! Two kinds of row read nothing. A later write of one key in the same resolve
//! pass lands nothing: the first write of that key does, or a stored row skips
//! them all. A source whose targets are all co-resident folds on the source
//! core from the real outcome, so a read buys nothing.

use std::collections::HashSet;

use nodedb_physical::physical_plan::{DocumentOp, MaterializedSumBinding};
use nodedb_types::CollectionKey;

use super::stored::StoredRowScope;
use crate::query::sum_target_is_co_resident;
use crate::types::DatabaseId;

/// Whether `op` skips a row it finds stored.
pub(super) fn is_skip_insert(op: &DocumentOp) -> bool {
    matches!(
        op,
        DocumentOp::PointInsert {
            if_absent: true,
            ..
        } | DocumentOp::BatchInsert {
            if_absent: true,
            ..
        }
    )
}

/// The rows one resolve pass already settled for a skip insert.
#[derive(Default)]
pub(super) struct SkipInsertRows {
    seen: HashSet<(String, u32)>,
}

impl SkipInsertRows {
    /// The `scopes` of a skip insert that read a stored row.
    ///
    /// Each row keeps its first scope, in page order and across the ops of the
    /// pass, because the Data Plane lands the first write of a key and skips
    /// the rest. A row's identity is its surrogate. Every scope drops out when
    /// all targets are co-resident.
    pub(super) fn first_rows<'a>(
        &mut self,
        scopes: Vec<StoredRowScope<'a>>,
        bindings: &[MaterializedSumBinding],
        collection: &str,
        database_id: DatabaseId,
    ) -> crate::Result<Vec<StoredRowScope<'a>>> {
        let source = CollectionKey::from_qualified_str(database_id, collection)?;
        if bindings
            .iter()
            .all(|binding| sum_target_is_co_resident(source, &binding.target_collection))
        {
            return Ok(Vec::new());
        }
        Ok(scopes
            .into_iter()
            .filter(|scope| match scope.surrogate {
                Some(surrogate) => self
                    .seen
                    .insert((scope.collection.to_string(), surrogate.as_u32())),
                None => true,
            })
            .collect())
    }
}

#[cfg(test)]
mod tests {
    use nodedb_types::{QualifiedCollection, Surrogate};

    use super::super::stored::stored_row_scopes;
    use super::*;

    const DB: DatabaseId = DatabaseId::DEFAULT;

    /// A source and a target that do not share a vShard.
    fn cross_shard_pair() -> (String, String) {
        let source = "skip_entries".to_string();
        let home = CollectionKey::from_bare(DB, &source).vshard();
        let target = (0..2048)
            .map(|i| format!("skip_accounts_{i}"))
            .find(|candidate| CollectionKey::from_bare(DB, candidate).vshard() != home)
            .expect("a second vShard");
        (source, target)
    }

    fn binding(target: &str) -> MaterializedSumBinding {
        MaterializedSumBinding {
            target_collection: target.to_string(),
            target_column: "balance".to_string(),
            join_column: "account_id".to_string(),
            value_expr: nodedb_query::expr::SqlExpr::Column("amount".to_string()),
            declared_primary_key: None,
        }
    }

    /// A page of `surrogates`, one row each.
    fn page(collection: &str, surrogates: &[u32], if_absent: bool) -> DocumentOp {
        DocumentOp::BatchInsert {
            collection: QualifiedCollection::new(DB, collection),
            documents: surrogates
                .iter()
                .map(|s| (format!("e{s}"), Vec::new()))
                .collect(),
            surrogates: surrogates.iter().copied().map(Surrogate::new).collect(),
            if_absent,
            returning: None,
            rls_filters: Vec::new(),
            resolved_sum_targets: Vec::new(),
            deferred_sum_targets: Vec::new(),
        }
    }

    fn surrogates_of(scopes: &[StoredRowScope<'_>]) -> Vec<Option<Surrogate>> {
        scopes.iter().map(|scope| scope.surrogate).collect()
    }

    /// A page that repeats a key credits only the first row. A scope per row
    /// credits the repeat too.
    #[test]
    fn a_skip_page_reads_the_first_row_of_each_key() {
        let (source, target) = cross_shard_pair();
        let op = page(&source, &[1, 1, 2], true);
        let scopes = stored_row_scopes(&op, None);
        assert_eq!(scopes.len(), 3, "the page names one scope per row");
        let first = SkipInsertRows::default()
            .first_rows(scopes, &[binding(&target)], &source, DB)
            .expect("first rows");
        assert_eq!(
            surrogates_of(&first),
            vec![Some(Surrogate::new(1)), Some(Surrogate::new(2))]
        );
    }

    /// A key a later op of the pass repeats reads nothing, so the repeat
    /// credits nothing.
    #[test]
    fn a_later_op_of_one_pass_reads_no_repeated_key() {
        let (source, target) = cross_shard_pair();
        let bindings = [binding(&target)];
        let mut rows = SkipInsertRows::default();
        let first = page(&source, &[1], true);
        let second = page(&source, &[1, 2], true);
        let kept = rows
            .first_rows(stored_row_scopes(&first, None), &bindings, &source, DB)
            .expect("first op");
        assert_eq!(kept.len(), 1);
        let kept = rows
            .first_rows(stored_row_scopes(&second, None), &bindings, &source, DB)
            .expect("second op");
        assert_eq!(surrogates_of(&kept), vec![Some(Surrogate::new(2))]);
    }

    /// A co-resident source folds on its core from the real outcome, so a read
    /// costs a cut per statement for nothing.
    #[test]
    fn a_co_resident_source_reads_nothing() {
        let (source, _) = cross_shard_pair();
        let op = page(&source, &[1, 2], true);
        let kept = SkipInsertRows::default()
            .first_rows(
                stored_row_scopes(&op, None),
                &[binding(&source)],
                &source,
                DB,
            )
            .expect("first rows");
        assert!(kept.is_empty());
    }

    /// A plain insert fails on a stored key and needs no stored read.
    #[test]
    fn only_a_skip_insert_is_one() {
        assert!(is_skip_insert(&page("entries", &[1], true)));
        assert!(!is_skip_insert(&page("entries", &[1], false)));
    }
}
