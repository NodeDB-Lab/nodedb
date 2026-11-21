// SPDX-License-Identifier: BUSL-1.1

//! The config pins a replicated write requires, and the check that its entry
//! carries them.
//!
//! A document write derives its row bodies and judges its rules under the
//! config of each collection it writes. It folds into each sum target under
//! that target's config. The proposer pins each one. Every replica refuses an
//! entry that leaves one of them unpinned, before any dispatch.
//!
//! The rule here reads the replicated form of a write. A core reads the
//! decoded plan by `nodedb_physical::physical_plan::document_pin_needs`, and
//! the two name the same collections.

use std::sync::Arc;

use nodedb_physical::physical_plan::{RedoPins, RedoSumTargets, ResolvedSumTarget};
use nodedb_types::{QualifiedCollection, Surrogate};

use super::replicated_entry::ReplicatedEntry;
use super::replicated_write::ReplicatedWrite;
use super::wire_shapes::{DocumentResolvedMutationWire, ReplicatedSumTarget};
use crate::bridge::envelope::ErrorCode;
use crate::types::{DatabaseId, TenantId};

/// The names a replicated write's config pins derive from.
#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub(crate) struct WritePinNames {
    /// Each written collection whose write requires a pin, database-qualified.
    pub required: Vec<String>,
    /// The sum targets the write folds into on its own core, one entry per
    /// source collection.
    pub sum_targets: Vec<RedoSumTargets>,
}

impl WritePinNames {
    /// Whether the write requires no pin.
    pub(crate) fn is_empty(&self) -> bool {
        self.required.is_empty() && self.sum_targets.is_empty()
    }

    /// Note document rows of `collection`, which fold into `resolved` less
    /// the targets `deferred` names.
    fn rows(&mut self, collection: &str, resolved: &[ReplicatedSumTarget], deferred: &[String]) {
        self.require(collection);
        let folded: Vec<ResolvedSumTarget> = resolved
            .iter()
            .filter(|target| !deferred.contains(&target.target_collection))
            .map(|target| {
                ResolvedSumTarget::new(
                    target.target_collection.clone(),
                    target.join_value.clone(),
                    Surrogate::new(target.surrogate),
                )
            })
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

    /// Require a pin of `collection`.
    fn require(&mut self, collection: &str) {
        if !self.required.iter().any(|name| name == collection) {
            self.required.push(collection.to_owned());
        }
    }
}

impl ReplicatedWrite {
    /// The collections and sum targets the write's config pins cover. Empty
    /// for a write that derives no document row and folds into no target.
    ///
    /// A CRDT document write merges through Loro, and a KV write carries its
    /// declared columns, so neither requires a pin.
    pub(crate) fn pinned_names(&self) -> WritePinNames {
        let mut names = WritePinNames::default();
        match self {
            ReplicatedWrite::PointInsert {
                collection,
                resolved_sum_targets,
                deferred_sum_targets,
                ..
            }
            | ReplicatedWrite::DocBatchInsert {
                collection,
                resolved_sum_targets,
                deferred_sum_targets,
                ..
            } => names.rows(collection, resolved_sum_targets, deferred_sum_targets),
            ReplicatedWrite::PointPut {
                collection,
                resolved_sum_targets,
                ..
            }
            | ReplicatedWrite::PointDelete {
                collection,
                resolved_sum_targets,
                ..
            }
            | ReplicatedWrite::PointUpdate {
                collection,
                resolved_sum_targets,
                ..
            }
            | ReplicatedWrite::DocUpsert {
                collection,
                resolved_sum_targets,
                ..
            }
            | ReplicatedWrite::DocBatchUpsert {
                collection,
                resolved_sum_targets,
                ..
            }
            | ReplicatedWrite::BulkDml {
                collection,
                resolved_sum_targets,
                ..
            }
            | ReplicatedWrite::DocTruncate {
                collection,
                resolved_sum_targets,
                ..
            } => names.rows(collection, resolved_sum_targets, &[]),
            ReplicatedWrite::MergeApply {
                target_collection,
                resolved_sum_targets,
                ..
            }
            | ReplicatedWrite::UpdateFromJoinApply {
                target_collection,
                resolved_sum_targets,
                ..
            } => names.rows(target_collection, resolved_sum_targets, &[]),
            // The write IS the fold into its target row.
            ReplicatedWrite::ApplyBalanceDelta { collection, .. } => names.require(collection),
            ReplicatedWrite::DocumentResolvedWrite { mutations, .. } => {
                for mutation in mutations {
                    let (DocumentResolvedMutationWire::Put {
                        collection,
                        resolved_sum_targets,
                        ..
                    }
                    | DocumentResolvedMutationWire::Delete {
                        collection,
                        resolved_sum_targets,
                        ..
                    }) = mutation;
                    names.rows(collection, resolved_sum_targets, &[]);
                }
            }
            ReplicatedWrite::VectorInsert { .. }
            | ReplicatedWrite::VectorBatchInsert { .. }
            | ReplicatedWrite::VectorDelete { .. }
            | ReplicatedWrite::SetVectorParams { .. }
            | ReplicatedWrite::SparseInsert { .. }
            | ReplicatedWrite::SparseDelete { .. }
            | ReplicatedWrite::MultiVectorInsert { .. }
            | ReplicatedWrite::MultiVectorDelete { .. }
            | ReplicatedWrite::DeleteBySurrogate { .. }
            | ReplicatedWrite::DirectUpsert { .. }
            | ReplicatedWrite::CrdtApply { .. }
            | ReplicatedWrite::ColumnarIngest { .. }
            | ReplicatedWrite::TimeseriesIngest { .. }
            | ReplicatedWrite::FtsIndex { .. }
            | ReplicatedWrite::FtsDelete { .. }
            | ReplicatedWrite::SpatialInsert { .. }
            | ReplicatedWrite::SpatialDelete { .. }
            | ReplicatedWrite::EdgePut { .. }
            | ReplicatedWrite::EdgeDelete { .. }
            | ReplicatedWrite::SetNodeLabels { .. }
            | ReplicatedWrite::RemoveNodeLabels { .. }
            | ReplicatedWrite::EdgePutBatch { .. }
            | ReplicatedWrite::EdgeDeleteBatch { .. }
            | ReplicatedWrite::KvPut { .. }
            | ReplicatedWrite::KvDelete { .. }
            | ReplicatedWrite::KvInsert { .. }
            | ReplicatedWrite::KvInsertOnConflictUpdate { .. }
            | ReplicatedWrite::KvBatchPut { .. }
            | ReplicatedWrite::KvExpire { .. }
            | ReplicatedWrite::KvPersist { .. }
            | ReplicatedWrite::KvIncr { .. }
            | ReplicatedWrite::KvIncrFloat { .. }
            | ReplicatedWrite::KvCas { .. }
            | ReplicatedWrite::KvGetSet { .. }
            | ReplicatedWrite::KvRegisterSortedIndex { .. }
            | ReplicatedWrite::KvDropSortedIndex { .. }
            | ReplicatedWrite::KvRegisterIndex { .. }
            | ReplicatedWrite::KvDropIndex { .. }
            // Index entries derive from stored rows. The metadata floor
            // orders the step after the index's registration.
            | ReplicatedWrite::DocIndexBuild { .. }
            | ReplicatedWrite::DocIndexRetire { .. }
            | ReplicatedWrite::KvFieldSet { .. }
            | ReplicatedWrite::KvTransfer { .. }
            | ReplicatedWrite::KvTransferItem { .. }
            | ReplicatedWrite::ArrayOp { .. }
            | ReplicatedWrite::ArraySchema { .. }
            | ReplicatedWrite::ColumnarBulkDml { .. }
            | ReplicatedWrite::CrdtImportCollection { .. }
            | ReplicatedWrite::CrdtListInsert { .. }
            | ReplicatedWrite::CrdtListDelete { .. }
            | ReplicatedWrite::CrdtListMove { .. }
            | ReplicatedWrite::CrdtDocUpsert { .. }
            | ReplicatedWrite::CrdtDocDelete { .. }
            | ReplicatedWrite::CalvinReadResult { .. }
            | ReplicatedWrite::KvTruncate { .. }
            | ReplicatedWrite::ConstraintChange { .. }
            | ReplicatedWrite::ArrayCellPut { .. }
            | ReplicatedWrite::ArrayCellDelete { .. }
            | ReplicatedWrite::CrdtApplyFenced { .. }
            | ReplicatedWrite::CrdtApplyAuthenticated { .. }
            | ReplicatedWrite::DropVectorIndex { .. }
            | ReplicatedWrite::ColumnarBulkDmlResolved { .. }
            | ReplicatedWrite::ColumnarTruncate { .. }
            | ReplicatedWrite::TimeseriesTruncate { .. }
            | ReplicatedWrite::TimeseriesAggregateRefresh { .. }
            | ReplicatedWrite::KvResolvedWrite { .. }
            | ReplicatedWrite::VectorDirectDelete { .. }
            | ReplicatedWrite::VectorDirectTruncate { .. }
            | ReplicatedWrite::VectorDirectUpdate { .. }
            | ReplicatedWrite::VectorResolvedDirectWrite { .. }
            // A committed redo carries its own pins in its body.
            | ReplicatedWrite::TransactionRedo { .. }
            | ReplicatedWrite::CutBarrier { .. }
            | ReplicatedWrite::TopicPublish { .. }
            | ReplicatedWrite::SurrogateBind { .. }
            | ReplicatedWrite::RedoChunk { .. }
            | ReplicatedWrite::RedoAbandon { .. }
            | ReplicatedWrite::CalvinReadTimeout { .. }
            | ReplicatedWrite::MirrorSeal { .. }
            | ReplicatedWrite::TenantPurge { .. }
            | ReplicatedWrite::CollectionConvert { .. }
            | ReplicatedWrite::RetentionDrop { .. }
            | ReplicatedWrite::TimeseriesAggregateImage { .. }
            | ReplicatedWrite::CalvinFrozenReply { .. }
            | ReplicatedWrite::CalvinSequenceEffects { .. } => {}
        }
        names
    }
}

/// A replicated document write whose entry does not carry a pin it requires.
/// Every replica holds the same entry bytes, so every replica refuses it.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub(crate) enum UncoveredPins {
    #[error("the replicated document write carries no config pins")]
    Unpinned,
    #[error("the replicated document write carries no config pin of {collection}")]
    Missing { collection: String },
}

impl From<UncoveredPins> for crate::Error {
    fn from(error: UncoveredPins) -> Self {
        Self::DataPlane(ErrorCode::RejectedPrevalidation {
            reason: error.to_string(),
        })
    }
}

impl ReplicatedEntry {
    /// Take the config pins every replica applies the entry's write under.
    /// `None` for a write that requires no pin. `Err` when the write
    /// requires a pin the entry does not carry.
    pub(crate) fn take_applied_config_pins(
        &mut self,
    ) -> Result<Option<Arc<RedoPins>>, UncoveredPins> {
        let names = self.write.pinned_names();
        if names.is_empty() {
            return Ok(None);
        }
        let pins = std::mem::take(&mut self.config_pins);
        check_covered(
            &pins,
            DatabaseId::new(self.database_id),
            TenantId::new(self.tenant_id),
            &names,
        )?;
        Ok(Some(Arc::new(pins)))
    }
}

/// Refuse `pins` unless they are pinned and hold a pin of every name in
/// `names`. A sum target names its collection bare. Its pin keys it by its
/// qualified name.
fn check_covered(
    pins: &RedoPins,
    database_id: DatabaseId,
    tenant_id: TenantId,
    names: &WritePinNames,
) -> Result<(), UncoveredPins> {
    if !pins.pinned() {
        return Err(UncoveredPins::Unpinned);
    }
    let missing = |collection: &str| {
        pins.config_named(database_id, tenant_id, collection)
            .is_none()
    };
    if let Some(collection) = names.required.iter().find(|name| missing(name)) {
        return Err(UncoveredPins::Missing {
            collection: collection.clone(),
        });
    }
    for target in names
        .sum_targets
        .iter()
        .flat_map(|targets| &targets.resolved)
    {
        let qualified = QualifiedCollection::new(database_id, &target.target_collection);
        if missing(qualified.as_str()) {
            return Err(UncoveredPins::Missing {
                collection: qualified.as_str().to_owned(),
            });
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use nodedb_physical::physical_plan::{ConfigPin, ConfigPinKey, CrsPin};

    use super::*;

    fn sum_target(target: &str) -> ReplicatedSumTarget {
        ReplicatedSumTarget {
            target_collection: target.into(),
            join_value: "a1".into(),
            surrogate: 7,
        }
    }

    fn put(resolved_sum_targets: Vec<ReplicatedSumTarget>) -> ReplicatedWrite {
        ReplicatedWrite::PointPut {
            collection: "entries".into(),
            document_id: "e1".into(),
            value: vec![0x80],
            surrogate: 1,
            resolved_sum_targets,
            returning: None,
            rls_filters: Vec::new(),
            rls_write_check: nodedb_types::RlsWriteCheck::NoPolicyApplies,
        }
    }

    fn pin(collection: &str) -> Arc<ConfigPin> {
        Arc::new(ConfigPin {
            key: ConfigPinKey {
                database_id: DatabaseId::DEFAULT,
                tenant_id: TenantId::new(1),
                collection: collection.into(),
            },
            digest: 3,
            wire: vec![0x80].into(),
        })
    }

    /// The pins of `collections`, which the caller names in sorted order.
    fn pins(collections: &[&str]) -> RedoPins {
        RedoPins {
            configs: collections.iter().map(|name| pin(name)).collect(),
            crs: Some(Arc::new(CrsPin {
                digest: 4,
                bytes: vec![0x90].into(),
            })),
        }
    }

    fn entry(write: ReplicatedWrite, pins: RedoPins) -> ReplicatedEntry {
        let mut entry = ReplicatedEntry::new(1, 0, 0, write);
        entry.config_pins = pins;
        entry
    }

    #[test]
    fn a_put_names_its_collection_and_its_sum_targets() {
        let names = put(vec![sum_target("accounts")]).pinned_names();
        assert_eq!(names.required, vec!["entries".to_string()]);
        assert_eq!(names.sum_targets.len(), 1);
        assert_eq!(names.sum_targets[0].collection, "entries");
    }

    #[test]
    fn a_deferred_target_takes_no_pin_on_the_source() {
        let write = ReplicatedWrite::PointInsert {
            collection: "entries".into(),
            document_id: "e1".into(),
            value: vec![0x80],
            if_absent: false,
            surrogate: 1,
            resolved_sum_targets: vec![sum_target("accounts")],
            deferred_sum_targets: vec!["accounts".into()],
            returning: None,
            rls_filters: Vec::new(),
            rls_write_check: nodedb_types::RlsWriteCheck::NoPolicyApplies,
        };
        let names = write.pinned_names();
        assert_eq!(names.required, vec!["entries".to_string()]);
        assert!(names.sum_targets.is_empty());
    }

    #[test]
    fn a_balance_delta_names_its_target() {
        let write = ReplicatedWrite::ApplyBalanceDelta {
            collection: "accounts".into(),
            document_id: "00000007".into(),
            surrogate: 7,
            column: "balance".into(),
            delta: "1".into(),
            join_column: "id".into(),
            join_value: "a1".into(),
            declared_primary_key: None,
        };
        assert_eq!(write.pinned_names().required, vec!["accounts".to_string()]);
    }

    #[test]
    fn a_kv_or_crdt_write_requires_no_pin() {
        let crdt = ReplicatedWrite::CrdtDocUpsert {
            collection: "notes".into(),
            document_id: "n1".into(),
            surrogate: 1,
            fields: vec![0x80],
            partial: false,
            verb: nodedb_physical::physical_plan::CrdtWriteVerb::Insert,
            returning: None,
            rls_filters: Vec::new(),
        };
        assert!(crdt.pinned_names().is_empty());
        let kv = ReplicatedWrite::KvPersist {
            collection: "cache".into(),
            key: b"k".to_vec(),
        };
        assert!(kv.pinned_names().is_empty());
    }

    #[test]
    fn a_covered_entry_hands_its_pins_to_the_apply() {
        let mut covered = entry(
            put(vec![sum_target("accounts")]),
            pins(&["accounts", "entries"]),
        );
        let applied = covered
            .take_applied_config_pins()
            .expect("covered")
            .expect("pinned");
        assert_eq!(applied.configs.len(), 2);
    }

    #[test]
    fn an_entry_that_requires_no_pin_applies_with_none() {
        let write = ReplicatedWrite::KvPersist {
            collection: "cache".into(),
            key: b"k".to_vec(),
        };
        let mut plain = entry(write, RedoPins::default());
        assert_eq!(plain.take_applied_config_pins(), Ok(None));
    }

    #[test]
    fn an_entry_with_no_pins_is_refused() {
        let mut bare = entry(put(Vec::new()), RedoPins::default());
        assert_eq!(
            bare.take_applied_config_pins(),
            Err(UncoveredPins::Unpinned)
        );
    }

    #[test]
    fn an_entry_that_leaves_a_collection_unpinned_is_refused() {
        let mut uncovered = entry(put(Vec::new()), pins(&["other"]));
        assert_eq!(
            uncovered.take_applied_config_pins(),
            Err(UncoveredPins::Missing {
                collection: "entries".into()
            })
        );
    }

    #[test]
    fn an_entry_that_leaves_a_sum_target_unpinned_is_refused() {
        let mut uncovered = entry(put(vec![sum_target("accounts")]), pins(&["entries"]));
        assert_eq!(
            uncovered.take_applied_config_pins(),
            Err(UncoveredPins::Missing {
                collection: "accounts".into()
            })
        );
    }

    #[test]
    fn a_refusal_is_a_final_prevalidation_refusal() {
        let error = crate::Error::from(UncoveredPins::Unpinned);
        assert!(crate::control::server::dispatch_utils::error_is_final_refusal(&error));
    }

    /// The replicated form and the decoded plan name the same pins.
    #[test]
    fn the_decoded_plan_names_the_same_pins() {
        use nodedb_physical::physical_plan::{PhysicalPlan, PinnedWrite, document_pin_needs};

        let mut write_entry = entry(put(vec![sum_target("accounts")]), RedoPins::default());
        let names = write_entry.write.pinned_names();
        let bytes = write_entry.to_bytes();
        let (_, _, plan, _) = crate::control::wal_replication::decode_replicated_entry(&bytes)
            .expect("decode")
            .expect("an entry");
        let PhysicalPlan::Document(op) = &plan else {
            panic!("expected a document plan, got {plan:?}");
        };
        let needs = document_pin_needs(op);
        let required: Vec<String> = needs
            .writes
            .iter()
            .filter(|(write, _)| write.requires_pin())
            .map(|(_, name)| name.to_string())
            .collect();
        assert_eq!(required, names.required);
        assert!(
            needs
                .writes
                .iter()
                .all(|(write, _)| *write == PinnedWrite::DocumentRows)
        );
        let targets = |all: &[RedoSumTargets]| -> Vec<String> {
            all.iter()
                .flat_map(|targets| &targets.resolved)
                .map(|target| target.target_collection.clone())
                .collect()
        };
        assert_eq!(targets(&needs.sum_targets), targets(&names.sum_targets));
        assert!(write_entry.take_applied_config_pins().is_err());
    }
}
