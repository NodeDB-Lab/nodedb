// SPDX-License-Identifier: BUSL-1.1

//! The vShards this node hosts, and the ones it left whose data it still
//! owes a purge, backing `_system.vshard_residency`.
//!
//! A row is written when a vShard starts being hosted here and changes when
//! it leaves, so a departure that happens while this node is down is found
//! at the next boot. A departed row records the WAL barrier its purge wrote
//! and whether the purge finished. It goes once the WAL no longer holds a
//! record below the barrier: no replay can bring a purged row back then.
//!
//! Table: `vshard_id` -> MessagePack [`VShardResidency`].

use redb::{ReadableDatabase, ReadableTable, TableDefinition};

use super::types::{SystemCatalog, catalog_err};

pub(super) const VSHARD_RESIDENCY: TableDefinition<u32, &[u8]> =
    TableDefinition::new("_system.vshard_residency");

/// Where one vShard stands on this node.
#[derive(Debug, Clone, Copy, PartialEq, Eq, zerompk::ToMessagePack, zerompk::FromMessagePack)]
#[msgpack(map)]
pub struct VShardResidency {
    /// Whether the vShard left this node.
    pub departed: bool,
    /// The WAL position the purge's barrier names: replay skips every record
    /// of a purged collection below it. `0` until the barrier is written.
    pub barrier_lsn: u64,
    /// Whether every core purged the vShard's data.
    pub purged: bool,
}

impl VShardResidency {
    /// A vShard this node hosts.
    pub const HOSTED: Self = Self {
        departed: false,
        barrier_lsn: 0,
        purged: false,
    };

    /// A vShard that left, with its purge not yet run.
    pub const DEPARTED: Self = Self {
        departed: true,
        barrier_lsn: 0,
        purged: false,
    };
}

/// One change of a row: `Some` writes it, `None` removes it.
pub type ResidencyChange = (u32, Option<VShardResidency>);

impl SystemCatalog {
    /// Every residency row, in vShard order.
    pub fn load_vshard_residency(&self) -> crate::Result<Vec<(u32, VShardResidency)>> {
        let txn = self
            .db
            .begin_read()
            .map_err(|e| catalog_err("vshard_residency read txn", e))?;
        let table = txn
            .open_table(VSHARD_RESIDENCY)
            .map_err(|e| catalog_err("open vshard_residency", e))?;
        let mut out = Vec::new();
        for row in table
            .iter()
            .map_err(|e| catalog_err("iterate vshard_residency", e))?
        {
            let (vshard_id, value) = row.map_err(|e| catalog_err("read vshard_residency", e))?;
            let residency: VShardResidency =
                zerompk::from_msgpack(value.value()).map_err(|e| crate::Error::Serialization {
                    format: "msgpack".into(),
                    detail: format!("vshard_residency row of vShard {}: {e}", vshard_id.value()),
                })?;
            out.push((vshard_id.value(), residency));
        }
        Ok(out)
    }

    /// Apply every change of `changes` in one transaction.
    pub fn apply_vshard_residency(&self, changes: &[ResidencyChange]) -> crate::Result<()> {
        if changes.is_empty() {
            return Ok(());
        }
        let txn = self
            .db
            .begin_write()
            .map_err(|e| catalog_err("vshard_residency write txn", e))?;
        {
            let mut table = txn
                .open_table(VSHARD_RESIDENCY)
                .map_err(|e| catalog_err("open vshard_residency", e))?;
            for (vshard_id, change) in changes {
                match change {
                    Some(residency) => {
                        let bytes = zerompk::to_msgpack_vec(residency).map_err(|e| {
                            crate::Error::Serialization {
                                format: "msgpack".into(),
                                detail: format!("vshard_residency row of vShard {vshard_id}: {e}"),
                            }
                        })?;
                        table
                            .insert(*vshard_id, bytes.as_slice())
                            .map_err(|e| catalog_err("insert vshard_residency", e))?;
                    }
                    None => {
                        table
                            .remove(*vshard_id)
                            .map_err(|e| catalog_err("remove vshard_residency", e))?;
                    }
                }
            }
        }
        txn.commit()
            .map_err(|e| catalog_err("vshard_residency commit", e))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rows_change_and_survive_a_reopen() {
        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().join("system.redb");
        {
            let catalog = SystemCatalog::open(&path).expect("open");
            catalog
                .apply_vshard_residency(&[
                    (3, Some(VShardResidency::HOSTED)),
                    (4, Some(VShardResidency::HOSTED)),
                    (5, Some(VShardResidency::DEPARTED)),
                ])
                .expect("write");
            catalog
                .apply_vshard_residency(&[
                    (4, None),
                    (
                        5,
                        Some(VShardResidency {
                            departed: true,
                            barrier_lsn: 9,
                            purged: true,
                        }),
                    ),
                ])
                .expect("change");
        }
        let catalog = SystemCatalog::open(&path).expect("reopen");
        assert_eq!(
            catalog.load_vshard_residency().expect("load"),
            vec![
                (3, VShardResidency::HOSTED),
                (
                    5,
                    VShardResidency {
                        departed: true,
                        barrier_lsn: 9,
                        purged: true,
                    }
                ),
            ]
        );
    }
}
