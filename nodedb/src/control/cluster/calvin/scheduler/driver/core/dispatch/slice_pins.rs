// SPDX-License-Identifier: BUSL-1.1

//! The config pins one vShard's slice of a Calvin transaction carries.
//!
//! The submitter pins the whole transaction's configs on its class. A slice
//! keeps the pins of the collections its local plans write or fold into,
//! and the CRS set. A balance delta's target is among them: its resolve
//! writes the target row. The filter reads only the class and the slice's
//! plans, so every replica derives the same pins.

use std::collections::BTreeSet;
use std::sync::Arc;

use nodedb_cluster::calvin::types::TxClass;
use nodedb_physical::physical_plan::{
    PhysicalPlan, RedoPins, pin_required_collections, written_collections,
};
use nodedb_types::QualifiedCollection;

use crate::control::planner::calvin::submit::config_pins::decode_config_pins;
use crate::control::wal_replication::transaction_redo::sum_targets::redo_sum_targets;

/// The pins of the slice whose local plans are `plans`, from `tx_class`.
///
/// A slice with no local plan writes nothing, so it keeps only the CRS pin.
/// A write slice is malformed when its class carries no pins, or lacks a
/// pin the slice's install requires: every submit pins every one. `Err`
/// refuses it, and every replica refuses it alike.
pub(in crate::control::cluster::calvin::scheduler::driver) fn slice_pins(
    tx_class: &TxClass,
    plans: &[PhysicalPlan],
) -> crate::Result<Arc<RedoPins>> {
    let class_pins = decode_config_pins(tx_class)?;
    if plans.is_empty() {
        return Ok(Arc::new(RedoPins {
            configs: Vec::new(),
            crs: class_pins.crs,
        }));
    }
    if !class_pins.pinned() {
        return Err(crate::Error::Internal {
            detail: "a Calvin write slice belongs to a class that carries no config pins; \
                     every submit pins its class, so the slice is refused"
                .into(),
        });
    }
    // Every name the install's check requires a pin of: each write the core
    // judges by `document_pin_needs`, and each sum target the redo folds.
    let mut required: BTreeSet<String> = pin_required_collections(plans).into_iter().collect();
    // A binding names its target bare. A pin keys it by its qualified name.
    for target in redo_sum_targets(plans)
        .iter()
        .flat_map(|targets| &targets.resolved)
    {
        required.insert(
            QualifiedCollection::new(tx_class.database_id, &target.target_collection)
                .as_str()
                .to_owned(),
        );
    }
    // A required pin the class lacks is a submit error. Every replica
    // refuses the slice alike, before it stages or commits.
    if let Some(missing) = required.iter().find(|name| {
        class_pins
            .config_named(tx_class.database_id, tx_class.tenant_id, name)
            .is_none()
    }) {
        return Err(crate::Error::Internal {
            detail: format!(
                "a Calvin write slice writes {missing}, but its class carries no config pin \
                 of it; the slice is refused"
            ),
        });
    }
    // A written collection whose write requires no pin keeps its pin when
    // the class carries one, as the submit pinned it.
    let mut named = required;
    named.extend(written_collections(plans));
    let configs = class_pins
        .configs
        .into_iter()
        .filter(|pin| named.contains(&pin.key.collection))
        .collect();
    Ok(Arc::new(RedoPins {
        configs,
        crs: class_pins.crs,
    }))
}

#[cfg(test)]
mod tests {
    use nodedb_cluster::calvin::types::{EngineKeySet, ReadWriteSet, SortedVec, VersionedReadSet};
    use nodedb_physical::physical_plan::{DocumentOp, ResolvedSumTarget};
    use nodedb_types::{DatabaseId, Surrogate, TenantId};

    use super::*;
    use crate::control::cluster::calvin::scheduler::driver::core::test_support::pins_for;

    fn class_pinned(collections: &[&str]) -> TxClass {
        let mut class = TxClass::new_single_vshard(
            ReadWriteSet::new(vec![]),
            ReadWriteSet::new(vec![EngineKeySet::Document {
                collection: "orders".to_owned(),
                surrogates: SortedVec::new(vec![1]),
            }]),
            Vec::new(),
            TenantId::new(1),
            None,
            VersionedReadSet::default(),
        )
        .expect("valid class");
        let bytes = zerompk::to_msgpack_vec(&pins_for(collections)).expect("encode pins");
        class.set_config_pins(bytes.into());
        class
    }

    fn truncate(collection: &str, targets: Vec<ResolvedSumTarget>) -> PhysicalPlan {
        PhysicalPlan::Document(DocumentOp::Truncate {
            collection: QualifiedCollection::new(DatabaseId::DEFAULT, collection),
            restart_identity: false,
            resolved_sum_targets: targets,
            declared_primary_key: None,
        })
    }

    fn names(pins: &RedoPins) -> Vec<&str> {
        pins.configs
            .iter()
            .map(|pin| pin.key.collection.as_str())
            .collect()
    }

    /// A slice keeps the pins of what it writes and folds into, and the CRS
    /// pin. Another slice's collections stay out.
    #[test]
    fn a_slice_keeps_its_own_collections_and_the_crs_pin() {
        let class = class_pinned(&["accounts", "items", "orders"]);
        let target = ResolvedSumTarget::new("accounts", "a1", Surrogate::new(1));
        let pins = slice_pins(&class, &[truncate("orders", vec![target])]).expect("pins");
        assert_eq!(names(&pins), vec!["accounts", "orders"]);
        assert!(pins.pinned());
    }

    /// A balance delta's task folds into its target row on the target's
    /// vShard. The resolve writes that row, so the install requires its
    /// pin, though no local plan reports the target as its collection.
    #[test]
    fn a_balance_delta_slice_keeps_its_target_pin() {
        let delta = PhysicalPlan::Document(DocumentOp::ApplyBalanceDelta {
            collection: QualifiedCollection::new(DatabaseId::DEFAULT, "accounts"),
            document_id: "01".into(),
            surrogate: Surrogate::new(1),
            column: "balance".into(),
            delta: "5".into(),
            join_column: "id".into(),
            join_value: "a1".into(),
            declared_primary_key: None,
        });
        let class = class_pinned(&["accounts", "entries"]);
        let pins = slice_pins(&class, std::slice::from_ref(&delta)).expect("pins");
        assert_eq!(names(&pins), vec!["accounts"]);

        let missing = class_pinned(&["entries"]);
        assert!(
            slice_pins(&missing, &[delta]).is_err(),
            "a class that lacks the target's pin is refused before the stage"
        );
    }

    /// A slice with no local plan keeps the CRS pin alone.
    #[test]
    fn a_read_only_slice_keeps_only_the_crs_pin() {
        let class = class_pinned(&["orders"]);
        let pins = slice_pins(&class, &[]).expect("pins");
        assert!(pins.configs.is_empty());
        assert!(pins.pinned());
    }

    /// A write slice of an unpinned class is refused.
    #[test]
    fn a_write_slice_of_an_unpinned_class_is_refused() {
        let mut class = class_pinned(&["orders"]);
        class.set_config_pins(nodedb_types::WireBytes::default());
        assert!(slice_pins(&class, &[truncate("orders", Vec::new())]).is_err());
        let pins = slice_pins(&class, &[]).expect("a read-only slice stays unpinned");
        assert!(!pins.pinned());
    }
}
