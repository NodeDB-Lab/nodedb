// SPDX-License-Identifier: BUSL-1.1

//! Restored document rows, decoded and identified but not yet bound.
//!
//! Preparing a row reads its identity and the surrogate its storage key
//! carries, and binds nothing. The restore checks every carried surrogate
//! against its home before any bind, then [`document_units`] binds each row
//! and builds its unit.

use nodedb_types::{QualifiedCollection, RowIdentity, StorageKey};

use crate::control::state::SharedState;
use crate::control::surrogate::CarriedIdentity;
use crate::types::{DatabaseId, TenantId};
use crate::wal::{RedoRowChange, RedoRowKind};

use super::super::bind_conflicts::CarriedBind;
use super::sub_record::{VersionStamp, document_put, document_tombstone};
use super::units::{CollectionUnits, RowUnit};

/// One collection's restored rows in the destination database.
pub(in crate::control::backup::restore) struct PreparedRows {
    pub(super) database_id: DatabaseId,
    pub(super) tenant: TenantId,
    /// Bare catalog name: it keys the identity binds.
    pub(super) bare: String,
    /// The name the destination Data Plane stores the collection under.
    pub(super) stored: QualifiedCollection,
    pub(super) rows: Vec<PendingRow>,
}

/// One decoded row and the identity it binds.
pub(super) struct PendingRow {
    pub(super) key: StorageKey,
    pub(super) identity: RowIdentity,
    pub(super) body: PendingBody,
}

/// A row's MessagePack body, or every version of a bitemporal row with the
/// body of each live version.
pub(super) enum PendingBody {
    Current(Vec<u8>),
    Versions(Vec<(VersionStamp, Option<Vec<u8>>)>),
}

impl PreparedRows {
    /// Every surrogate the rows carry, with the key each one names.
    pub(in crate::control::backup::restore) fn carried(
        &self,
    ) -> impl Iterator<Item = CarriedBind> + '_ {
        self.rows.iter().map(|row| CarriedBind {
            collection: self.bare.clone(),
            pk: row.identity.as_str().as_bytes().to_vec(),
            surrogate: row.key.surrogate().as_u32(),
        })
    }
}

/// Bind each prepared row's identity on this node and build its unit. The
/// backup's surrogate wins unless this node already binds the identity: the
/// row then installs under that surrogate, over the row it names.
pub(super) fn document_units(
    state: &SharedState,
    prepared: Vec<PreparedRows>,
) -> crate::Result<Vec<CollectionUnits>> {
    let mut out = Vec::with_capacity(prepared.len());
    for collection in prepared {
        let key = nodedb_types::CollectionKey::from_bare(collection.database_id, &collection.bare);
        let mut units = Vec::with_capacity(collection.rows.len());
        // Every row of the collection binds in one catalog transaction.
        let carried_rows: Vec<(&[u8], nodedb_types::Surrogate)> = collection
            .rows
            .iter()
            .map(|row| (row.identity.as_str().as_bytes(), row.key.surrogate()))
            .collect();
        let bound = state
            .surrogate_assigner
            .bind_many(key, collection.tenant, &carried_rows)?;
        drop(carried_rows);
        if bound.len() != collection.rows.len() {
            return Err(crate::Error::Internal {
                detail: format!(
                    "restore bind of '{}' answered {} of {} rows",
                    collection.bare,
                    bound.len(),
                    collection.rows.len()
                ),
            });
        }
        for (row, surrogate) in collection.rows.into_iter().zip(bound) {
            let pk = row.identity.as_str();
            let carried = CarriedIdentity {
                collection: collection.bare.clone(),
                pk_bytes: pk.as_bytes().to_vec(),
                surrogate,
            };
            let raw = surrogate.as_u32();
            let stored = collection.stored.as_str();
            let (ops, ends_live) = match row.body {
                PendingBody::Current(value) => {
                    (vec![document_put(stored, pk, value, raw, None)?], true)
                }
                PendingBody::Versions(versions) => {
                    let ends_live = versions.last().is_some_and(|(_, body)| body.is_some());
                    let mut ops = Vec::with_capacity(versions.len());
                    for (stamp, body) in versions {
                        ops.push(match body {
                            Some(value) => document_put(stored, pk, value, raw, Some(stamp))?,
                            None => document_tombstone(stored, pk, raw, stamp.sys_from)?,
                        });
                    }
                    (ops, ends_live)
                }
            };
            // A restored row that ends live installs as a new row. One whose
            // last version is a tombstone ends absent and publishes nothing.
            let changes = if ends_live {
                vec![RedoRowChange {
                    collection: stored.to_owned(),
                    row: pk.to_owned(),
                    kind: RedoRowKind::Insert,
                }]
            } else {
                Vec::new()
            };
            units.push(RowUnit {
                ops,
                identities: vec![carried],
                changes,
                rows: vec![nodedb_physical::physical_plan::RestoredRow {
                    collection: stored.to_owned(),
                    document_id: pk.to_owned(),
                    surrogate: raw,
                }],
            });
        }
        out.push(CollectionUnits::rows(
            collection.database_id,
            collection.bare,
            units,
        ));
    }
    Ok(out)
}
