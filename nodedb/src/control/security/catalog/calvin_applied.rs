// SPDX-License-Identifier: BUSL-1.1

//! Persistent Calvin applied state backing `_system.calvin_applied`.
//!
//! A Calvin scheduler learns at boot which `(epoch, position)` it already
//! applied from the applied markers in the WAL. A checkpoint deletes the WAL
//! segments that hold them, while the sequencer log still holds the entries
//! and delivers them again after a restart. So before each truncation the
//! checkpoint saves every scheduler's applied state here, and boot recovery
//! reads it together with the markers the WAL still holds.

use std::collections::BTreeSet;

use redb::{ReadableDatabase, ReadableTable, TableDefinition};

use super::types::{SystemCatalog, catalog_err};

/// Table: `vshard_id` -> `(fully_applied_epoch, msgpack of the applied tail)`.
pub(super) const CALVIN_APPLIED: TableDefinition<u32, (u64, &[u8])> =
    TableDefinition::new("_system.calvin_applied");

/// Sentinel `fully_applied_epoch`: no epoch is fully applied.
const NONE_FULLY_APPLIED: u64 = u64::MAX;

/// One vShard's applied state.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StoredCalvinApplied {
    pub vshard_id: u32,
    /// Every position of every epoch at or below this is applied. `u64::MAX`
    /// means none is.
    pub fully_applied_epoch: u64,
    /// Applied `(epoch, position)` pairs above the watermark.
    pub tail: BTreeSet<(u64, u32)>,
}

impl StoredCalvinApplied {
    /// The union of two states of one vShard: the higher watermark, and the
    /// tail entries of both above it.
    fn merge(self, other: StoredCalvinApplied) -> StoredCalvinApplied {
        let fully_applied_epoch = match (self.fully_applied_epoch, other.fully_applied_epoch) {
            (NONE_FULLY_APPLIED, w) | (w, NONE_FULLY_APPLIED) => w,
            (a, b) => a.max(b),
        };
        let tail = self
            .tail
            .into_iter()
            .chain(other.tail)
            .filter(|(epoch, _)| {
                fully_applied_epoch == NONE_FULLY_APPLIED || *epoch > fully_applied_epoch
            })
            .collect();
        StoredCalvinApplied {
            vshard_id: self.vshard_id,
            fully_applied_epoch,
            tail,
        }
    }
}

fn encode_tail(tail: &BTreeSet<(u64, u32)>) -> crate::Result<Vec<u8>> {
    let pairs: Vec<(u64, u32)> = tail.iter().copied().collect();
    zerompk::to_msgpack_vec(&pairs).map_err(|e| crate::Error::Serialization {
        format: "msgpack".into(),
        detail: format!("encode calvin applied tail: {e}"),
    })
}

fn decode_tail(bytes: &[u8]) -> crate::Result<BTreeSet<(u64, u32)>> {
    let pairs: Vec<(u64, u32)> =
        zerompk::from_msgpack(bytes).map_err(|e| crate::Error::Serialization {
            format: "msgpack".into(),
            detail: format!("decode calvin applied tail: {e}"),
        })?;
    Ok(pairs.into_iter().collect())
}

impl SystemCatalog {
    /// The saved applied state of `vshard_id`, if any.
    pub fn load_calvin_applied(
        &self,
        vshard_id: u32,
    ) -> crate::Result<Option<StoredCalvinApplied>> {
        let read_txn = self
            .db
            .begin_read()
            .map_err(|e| catalog_err("load_calvin_applied read txn", e))?;
        let table = read_txn
            .open_table(CALVIN_APPLIED)
            .map_err(|e| catalog_err("open calvin_applied", e))?;
        let Some(row) = table
            .get(vshard_id)
            .map_err(|e| catalog_err("get calvin_applied", e))?
        else {
            return Ok(None);
        };
        let (fully_applied_epoch, tail) = row.value();
        Ok(Some(StoredCalvinApplied {
            vshard_id,
            fully_applied_epoch,
            tail: decode_tail(tail)?,
        }))
    }

    /// Save `states` in one transaction. Each is merged with the state
    /// already saved for its vShard, so the saved state only grows.
    pub fn save_calvin_applied(&self, states: Vec<StoredCalvinApplied>) -> crate::Result<()> {
        if states.is_empty() {
            return Ok(());
        }
        let write_txn = self
            .db
            .begin_write()
            .map_err(|e| catalog_err("save_calvin_applied txn", e))?;
        {
            let mut table = write_txn
                .open_table(CALVIN_APPLIED)
                .map_err(|e| catalog_err("open calvin_applied", e))?;
            for state in states {
                let saved = match table
                    .get(state.vshard_id)
                    .map_err(|e| catalog_err("get calvin_applied", e))?
                {
                    Some(row) => {
                        let (fully_applied_epoch, tail) = row.value();
                        Some(StoredCalvinApplied {
                            vshard_id: state.vshard_id,
                            fully_applied_epoch,
                            tail: decode_tail(tail)?,
                        })
                    }
                    None => None,
                };
                let merged = match saved {
                    Some(saved) => saved.merge(state),
                    None => state,
                };
                let tail = encode_tail(&merged.tail)?;
                table
                    .insert(
                        merged.vshard_id,
                        (merged.fully_applied_epoch, tail.as_slice()),
                    )
                    .map_err(|e| catalog_err("insert calvin_applied", e))?;
            }
        }
        write_txn
            .commit()
            .map_err(|e| catalog_err("commit calvin_applied", e))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn state(vshard_id: u32, w: u64, tail: &[(u64, u32)]) -> StoredCalvinApplied {
        StoredCalvinApplied {
            vshard_id,
            fully_applied_epoch: w,
            tail: tail.iter().copied().collect(),
        }
    }

    #[test]
    fn saved_state_reloads_and_only_grows() {
        let dir = tempfile::tempdir().expect("tempdir");
        let catalog = SystemCatalog::open(&dir.path().join("system.redb")).expect("catalog");
        assert_eq!(catalog.load_calvin_applied(3).expect("load"), None);

        catalog
            .save_calvin_applied(vec![state(3, NONE_FULLY_APPLIED, &[(0, 0), (2, 1)])])
            .expect("save");
        catalog
            .save_calvin_applied(vec![state(3, 1, &[(4, 0)])])
            .expect("save");
        assert_eq!(
            catalog.load_calvin_applied(3).expect("load"),
            Some(state(3, 1, &[(2, 1), (4, 0)]))
        );
    }
}
