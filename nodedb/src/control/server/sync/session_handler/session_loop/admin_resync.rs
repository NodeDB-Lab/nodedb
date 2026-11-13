// SPDX-License-Identifier: BUSL-1.1

//! The full current set of one definition family, for an admin session whose
//! queue dropped a notice of it.
//!
//! A reconnect replays no definition notice: a client that missed one stays
//! diverged until the definition changes again. So a dropped notice marks
//! its family, and the session sends a collection resync of the family's
//! pseudo-collection with every definition of its tenant and database. The
//! client replaces its rows of the family with it, so a definition dropped
//! while the notice was lost leaves the client too.
//!
//! Each row is the notice's own encoding: the definition under the document
//! ID its notice carries.

use nodedb_types::sync::wire::{CollectionResyncMsg, SyncFrame, SyncMessageType};

use crate::control::state::SharedState;
use crate::event::crdt_sync::types::DefinitionFamily;
use crate::types::DatabaseId;

/// The rows of `family` in `(tenant_id, database_id)`: `(document_id,
/// definition)` pairs, the definition in the notice's encoding.
fn family_rows(
    shared: &SharedState,
    tenant_id: u64,
    database_id: DatabaseId,
    family: DefinitionFamily,
) -> crate::Result<Vec<(String, Vec<u8>)>> {
    let catalog = shared.credentials.catalog();
    let mut rows = Vec::new();
    match family {
        DefinitionFamily::AlertRules => {
            for def in catalog.load_alert_rules_in_database(database_id.as_u64())? {
                if def.tenant_id == tenant_id {
                    rows.push((def.name.clone(), encode(&def)?));
                }
            }
        }
        DefinitionFamily::RetentionPolicies => {
            for def in catalog.load_retention_policies_in_database(database_id.as_u64())? {
                if def.tenant_id == tenant_id {
                    rows.push((def.name.clone(), encode(&def)?));
                }
            }
        }
        DefinitionFamily::Schedules => {
            for def in catalog.load_schedules_in_database(database_id, tenant_id)? {
                let document_id = format!("{}:{}", database_id.as_u64(), def.name);
                rows.push((document_id, encode(&def)?));
            }
        }
    }
    Ok(rows)
}

fn encode<T: zerompk::ToMessagePack>(definition: &T) -> crate::Result<Vec<u8>> {
    zerompk::to_msgpack_vec(definition).map_err(|error| crate::Error::Serialization {
        format: "msgpack".into(),
        detail: format!("definition does not encode: {error}"),
    })
}

/// The collection resync frame of `family` in `(tenant_id, database_id)`.
pub(super) fn family_resync(
    shared: &SharedState,
    tenant_id: u64,
    database_id: DatabaseId,
    family: DefinitionFamily,
) -> crate::Result<SyncFrame> {
    let rows = family_rows(shared, tenant_id, database_id, family)?;
    let doc_count = rows.len();
    let data = crate::data::executor::response_codec::encode_raw_document_rows(&rows)?;
    let msg = CollectionResyncMsg {
        collection: family.collection().to_string(),
        shape_ids: Vec::new(),
        data,
        snapshot_lsn: 0,
        doc_count,
    };
    SyncFrame::try_encode(SyncMessageType::CollectionResync, &msg).ok_or_else(|| {
        crate::Error::Serialization {
            format: "msgpack".into(),
            detail: format!("the {} resync frame does not encode", family.collection()),
        }
    })
}
