// SPDX-License-Identifier: BUSL-1.1

//! The config content a committed redo's proposer pins for its validate.
//!
//! The pin set is one config pin per written collection and per
//! materialized-sum target the redo folds into, plus the CRS set pin. A
//! collection with no active catalog row has no config, so it has no pin.
//! The read set needs no pin: the validate reads no config of it.

use std::collections::BTreeSet;

use nodedb_physical::physical_plan::{ConfigPinKey, RedoPins, RedoSumTargets};
use nodedb_types::QualifiedCollection;

use crate::control::state::SharedState;
use crate::types::{DatabaseId, TenantId};

/// The pins of a redo that writes `written`, database-qualified names, and
/// folds into the targets `sum_targets` names.
pub fn redo_pins<'a>(
    state: &SharedState,
    database_id: DatabaseId,
    tenant_id: TenantId,
    written: impl IntoIterator<Item = &'a str>,
    sum_targets: &[RedoSumTargets],
) -> crate::Result<RedoPins> {
    let key = |collection: String| ConfigPinKey {
        database_id,
        tenant_id,
        collection,
    };
    let mut keys: BTreeSet<ConfigPinKey> = written
        .into_iter()
        .map(|name| key(name.to_owned()))
        .collect();
    // A binding names its target bare. The core keys the target's config
    // by its qualified name.
    keys.extend(
        sum_targets
            .iter()
            .flat_map(|targets| &targets.resolved)
            .map(|target| {
                key(
                    QualifiedCollection::new(database_id, &target.target_collection)
                        .as_str()
                        .to_owned(),
                )
            }),
    );
    let catalog = state.credentials.catalog();
    let mut configs = Vec::with_capacity(keys.len());
    for key in &keys {
        if let Some(pin) = state.config_pins.config_pin(catalog, key)? {
            configs.push(pin);
        }
    }
    Ok(RedoPins {
        configs,
        crs: Some(state.config_pins.crs_pin(catalog)?),
    })
}
