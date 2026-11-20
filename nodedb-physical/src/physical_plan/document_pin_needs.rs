// SPDX-License-Identifier: Apache-2.0

//! The config pins one replicated document write requires.
//!
//! A document write derives its row bodies and judges its rules under the
//! config of each collection it writes. It folds its deltas into each sum
//! target under that target's config. Its proposer pins each of them, so
//! every replica derives and judges alike.
//!
//! A core refuses a pinned write that leaves one of them unpinned. The
//! proposer reads the replicated form of the same write, by the same rule.

use super::{DocumentOp, DocumentResolvedMutation, PinnedWrite, RedoSumTargets, ResolvedSumTarget};

/// The pins one document write requires.
#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct DocumentPinNeeds<'a> {
    /// Each written collection, database-qualified, with what the write
    /// makes there.
    pub writes: Vec<(PinnedWrite, &'a str)>,
    /// The sum targets the write folds into on its own core, one entry per
    /// source collection. A target an `ApplyBalanceDelta` entry moves is
    /// not listed.
    pub sum_targets: Vec<RedoSumTargets>,
}

impl DocumentPinNeeds<'_> {
    /// Whether the write requires no pin.
    pub fn is_empty(&self) -> bool {
        self.writes.is_empty() && self.sum_targets.is_empty()
    }
}

/// The pins `op` requires. A read, a config op, and a resolve wrapper
/// require none.
pub fn document_pin_needs(op: &DocumentOp) -> DocumentPinNeeds<'_> {
    let mut needs = DocumentPinNeeds::default();
    match op {
        DocumentOp::PointInsert {
            collection,
            resolved_sum_targets,
            deferred_sum_targets,
            ..
        }
        | DocumentOp::BatchInsert {
            collection,
            resolved_sum_targets,
            deferred_sum_targets,
            ..
        } => needs.rows(
            collection.as_str(),
            resolved_sum_targets,
            deferred_sum_targets,
        ),
        DocumentOp::PointPut {
            collection,
            resolved_sum_targets,
            ..
        }
        | DocumentOp::PointDelete {
            collection,
            resolved_sum_targets,
            ..
        }
        | DocumentOp::PointUpdate {
            collection,
            resolved_sum_targets,
            ..
        }
        | DocumentOp::Upsert {
            collection,
            resolved_sum_targets,
            ..
        }
        | DocumentOp::BatchUpsert {
            collection,
            resolved_sum_targets,
            ..
        }
        | DocumentOp::BulkUpdate {
            collection,
            resolved_sum_targets,
            ..
        }
        | DocumentOp::BulkDelete {
            collection,
            resolved_sum_targets,
            ..
        }
        | DocumentOp::Truncate {
            collection,
            resolved_sum_targets,
            ..
        } => needs.rows(collection.as_str(), resolved_sum_targets, &[]),
        DocumentOp::UpdateFromJoin {
            target_collection,
            resolved_sum_targets,
            ..
        }
        | DocumentOp::Merge {
            target_collection,
            resolved_sum_targets,
            ..
        } => needs.rows(target_collection.as_str(), resolved_sum_targets, &[]),
        // The op IS the fold into its target row.
        DocumentOp::ApplyBalanceDelta { collection, .. } => {
            needs
                .writes
                .push((PinnedWrite::SumTarget, collection.as_str()));
        }
        DocumentOp::ResolvedWrite { mutations, .. } => {
            for mutation in mutations {
                let (DocumentResolvedMutation::Put {
                    collection,
                    resolved_sum_targets,
                    ..
                }
                | DocumentResolvedMutation::Delete {
                    collection,
                    resolved_sum_targets,
                    ..
                }) = mutation;
                needs.rows(collection.as_str(), resolved_sum_targets, &[]);
            }
        }
        // Reads, config and index ops, and the resolve wrapper write no
        // row. `InsertSelect` never reaches a core as itself.
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
        | DocumentOp::ResolveWrite(_) => {}
    }
    needs
}

impl<'a> DocumentPinNeeds<'a> {
    /// Note document rows of `collection`, which fold into `resolved` less
    /// the targets `deferred` names.
    fn rows(&mut self, collection: &'a str, resolved: &[ResolvedSumTarget], deferred: &[String]) {
        if !self
            .writes
            .iter()
            .any(|(write, name)| *write == PinnedWrite::DocumentRows && *name == collection)
        {
            self.writes.push((PinnedWrite::DocumentRows, collection));
        }
        let folded: Vec<ResolvedSumTarget> = resolved
            .iter()
            .filter(|target| !deferred.contains(&target.target_collection))
            .cloned()
            .collect();
        if folded.is_empty() {
            return;
        }
        match self
            .sum_targets
            .iter_mut()
            .find(|targets| targets.collection == collection)
        {
            Some(targets) => {
                for target in folded {
                    if !targets.resolved.contains(&target) {
                        targets.resolved.push(target);
                    }
                }
            }
            None => self.sum_targets.push(RedoSumTargets {
                collection: collection.to_owned(),
                resolved: folded,
            }),
        }
    }
}

#[cfg(test)]
mod tests {
    use nodedb_types::{DatabaseId, QualifiedCollection, RlsWriteCheck, Surrogate};

    use super::*;

    fn qualified(name: &str) -> QualifiedCollection {
        QualifiedCollection::new(DatabaseId::DEFAULT, name)
    }

    fn insert(deferred: Vec<String>) -> DocumentOp {
        DocumentOp::PointInsert {
            collection: qualified("entries"),
            document_id: "e1".into(),
            value: vec![0x80],
            surrogate: Surrogate::new(1),
            if_absent: false,
            returning: None,
            rls_filters: Vec::new(),
            rls_write_check: RlsWriteCheck::NoPolicyApplies,
            resolved_sum_targets: vec![
                ResolvedSumTarget::new("accounts", "a1", Surrogate::new(7)),
                ResolvedSumTarget::new("ledgers", "l1", Surrogate::new(8)),
            ],
            deferred_sum_targets: deferred,
        }
    }

    #[test]
    fn an_insert_pins_its_rows_and_its_folded_targets() {
        let op = insert(Vec::new());
        let needs = document_pin_needs(&op);
        assert_eq!(needs.writes, vec![(PinnedWrite::DocumentRows, "entries")]);
        assert_eq!(needs.sum_targets.len(), 1);
        assert_eq!(needs.sum_targets[0].collection, "entries");
        assert_eq!(needs.sum_targets[0].resolved.len(), 2);
    }

    #[test]
    fn a_deferred_target_takes_no_pin_on_the_source() {
        let op = insert(vec!["ledgers".into()]);
        let needs = document_pin_needs(&op);
        let targets: Vec<&str> = needs.sum_targets[0]
            .resolved
            .iter()
            .map(|target| target.target_collection.as_str())
            .collect();
        assert_eq!(targets, vec!["accounts"]);
    }

    #[test]
    fn a_balance_delta_pins_its_target() {
        let op = DocumentOp::ApplyBalanceDelta {
            collection: qualified("accounts"),
            document_id: "00000007".into(),
            surrogate: Surrogate::new(7),
            column: "balance".into(),
            delta: "1".into(),
            join_column: "id".into(),
            join_value: "a1".into(),
            declared_primary_key: None,
        };
        let needs = document_pin_needs(&op);
        assert_eq!(needs.writes, vec![(PinnedWrite::SumTarget, "accounts")]);
        assert!(needs.sum_targets.is_empty());
    }

    #[test]
    fn a_resolved_write_pins_each_mutated_collection_once() {
        let put = |collection: &str, document_id: &str| DocumentResolvedMutation::Put {
            collection: qualified(collection),
            document_id: document_id.into(),
            surrogate: Surrogate::new(1),
            pk_bytes: document_id.as_bytes().to_vec(),
            value: vec![0x80],
            precondition: None,
            resolved_sum_targets: Vec::new(),
        };
        let op = DocumentOp::ResolvedWrite {
            mutations: vec![put("a", "1"), put("a", "2"), put("b", "3")],
            response_payload: Vec::new(),
            rls_write_check: RlsWriteCheck::NoPolicyApplies,
        };
        let needs = document_pin_needs(&op);
        assert_eq!(
            needs.writes,
            vec![
                (PinnedWrite::DocumentRows, "a"),
                (PinnedWrite::DocumentRows, "b")
            ]
        );
    }

    #[test]
    fn a_read_requires_no_pin() {
        let op = DocumentOp::PointGet {
            collection: qualified("entries"),
            document_id: "e1".into(),
            surrogate: None,
            pk_bytes: Vec::new(),
            rls_filters: Vec::new(),
            system_time: nodedb_types::SystemTimeScope::Current,
            valid_at_ms: None,
        };
        assert!(document_pin_needs(&op).is_empty());
    }
}
