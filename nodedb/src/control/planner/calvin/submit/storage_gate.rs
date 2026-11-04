// SPDX-License-Identifier: BUSL-1.1

//! The storage-quota check of a cross-shard Calvin submit.
//!
//! A sequenced transaction applies through a Calvin redo entry, which the
//! leader proposal gate exempts. The submit is the one place the whole
//! transaction's plans are in hand.

use nodedb_cluster::calvin::types::TxClass;

use crate::control::server::shared::write_admission::{check_storage_quota, plan_adds_storage};
use crate::control::state::SharedState;
use crate::event::EventSource;

/// Refuse `tx_class` when it adds bytes to a database or tenant at its
/// storage limit. A class already split into parts passed this check at its
/// coordinator.
pub(super) fn admit_storage(state: &SharedState, tx_class: &TxClass) -> crate::Result<()> {
    if tx_class.is_multi_part() {
        return Ok(());
    }
    // Code `0` is the client default.
    let source = EventSource::from_wal_code(tx_class.event_source).unwrap_or(EventSource::User);
    check_storage_quota(
        state,
        tx_class.database_id,
        tx_class.tenant_id,
        source,
        || {
            let plans = nodedb_physical::physical_plan::wire::decode_batch(&tx_class.plans)
                .map_err(|e| crate::Error::Serialization {
                    format: "msgpack".into(),
                    detail: format!("calvin storage quota: plan decode: {e}"),
                })?;
            Ok(plans.iter().any(plan_adds_storage))
        },
    )
}
