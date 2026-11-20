// SPDX-License-Identifier: BUSL-1.1

//! The config pins a Calvin submit stamps on its class.
//!
//! The submitter pins the config of every collection the plans write or fold
//! into, and the CRS set, from its own catalog. Every participant's redo
//! carries its slice of them, so every replica judges the redo under the
//! same config content.

use nodedb_cluster::calvin::types::{IncarnationKind, TxClass};
use nodedb_physical::physical_plan::{
    PhysicalPlan, RedoPins, pin_required_collections, written_collections,
};

use crate::Error;
use crate::control::planner::calvin::incarnation_names::incarnation_names;
use crate::control::state::SharedState;
use crate::control::wal_replication::transaction_redo::pins::{PinScope, redo_pins};
use crate::control::wal_replication::transaction_redo::sum_targets::redo_sum_targets;

/// Stamp the config pins `plans` require on `tx_class`. `plans` are the
/// class's decoded plans.
///
/// A class that carries pins keeps them: a forwarded submit arrives pinned.
/// A split class was pinned by its coordinator before the split. A written
/// collection or sum target with no active committed catalog row refuses
/// the submit with `CollectionNotFound`.
pub(crate) fn stamp_config_pins(
    state: &SharedState,
    tx_class: &mut TxClass,
    plans: &[PhysicalPlan],
) -> crate::Result<()> {
    if !tx_class.config_pins.is_empty() || tx_class.is_multi_part() {
        return Ok(());
    }
    let sum_targets = redo_sum_targets(plans);
    let named = incarnation_names(plans, |plan| {
        written_collections(std::slice::from_ref(plan))
    });
    // An array has no document config, so it takes no pin.
    let pinned_names = named
        .iter()
        .filter(|(_, kind)| matches!(kind, IncarnationKind::Collection))
        .map(|(name, _)| name.as_str());
    let scope = PinScope {
        database_id: tx_class.database_id,
        tenant_id: tx_class.tenant_id,
    };
    let required = pin_required_collections(plans);
    let pins = redo_pins(state, scope, pinned_names, &required, &sum_targets)?;
    let bytes = zerompk::to_msgpack_vec(&pins).map_err(|e| Error::Serialization {
        format: "msgpack".into(),
        detail: format!("Calvin transaction config pins: encode: {e}"),
    })?;
    tx_class.set_config_pins(bytes.into());
    Ok(())
}

/// The config pins `tx_class` carries. An unpinned class decodes to empty
/// pins.
pub(crate) fn decode_config_pins(tx_class: &TxClass) -> crate::Result<RedoPins> {
    if tx_class.config_pins.is_empty() {
        return Ok(RedoPins::default());
    }
    zerompk::from_msgpack::<RedoPins>(&tx_class.config_pins).map_err(|e| Error::Serialization {
        format: "msgpack".into(),
        detail: format!("Calvin transaction config pins: decode: {e}"),
    })
}
