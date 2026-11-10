// SPDX-License-Identifier: BUSL-1.1

//! Persistent frozen `RETURNING` replies backing
//! `_system.calvin_frozen_replies`.
//!
//! The data-group apply loop stores each `CalvinFrozenReply` entry in the
//! row of its `(vShard, txn)` before the entry counts as applied. Boot
//! resumes Raft delivery above the durable applied floor, so an entry at or
//! below it is never delivered again. Its row is what the slice's install
//! answers after a restart. Each store commits durably, so no floor covers
//! an entry whose row a crash loses.
//!
//! The first stored copy of a txn's reply stays: a later copy of the entry,
//! from a re-proposal or a new leader, writes nothing. Every replica applies
//! the log in one order, so every replica keeps the same copy.
//!
//! Each row holds the encoded reply. This module stores the bytes and never
//! decodes them.

use std::collections::BTreeSet;

use redb::{Durability, ReadableDatabase, ReadableTable, TableDefinition};

use super::types::{SystemCatalog, catalog_err};

/// Table: `(vshard_id, epoch, position)` -> encoded frozen reply.
pub(super) const CALVIN_FROZEN_REPLIES: TableDefinition<(u32, u64, u32), &[u8]> =
    TableDefinition::new("_system.calvin_frozen_replies");

/// One stored frozen reply.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StoredFrozenReply {
    pub vshard_id: u32,
    pub epoch: u64,
    pub position: u32,
    /// The encoded reply.
    pub reply: Vec<u8>,
}

/// The key range of every row of `vshard_id`.
fn vshard_range(vshard_id: u32) -> std::ops::RangeInclusive<(u32, u64, u32)> {
    (vshard_id, 0, 0)..=(vshard_id, u64::MAX, u32::MAX)
}

impl SystemCatalog {
    /// Store `reply` as the row of `(vshard_id, epoch, position)` unless a
    /// row is stored there. Returns whether this call stored it. The commit
    /// is durable when this returns.
    pub fn store_calvin_frozen_reply(
        &self,
        vshard_id: u32,
        epoch: u64,
        position: u32,
        reply: &[u8],
    ) -> crate::Result<bool> {
        let mut write_txn = self
            .db
            .begin_write()
            .map_err(|e| catalog_err("store_calvin_frozen_reply txn", e))?;
        write_txn
            .set_durability(Durability::Immediate)
            .map_err(|e| catalog_err("store_calvin_frozen_reply durability", e))?;
        {
            let mut table = write_txn
                .open_table(CALVIN_FROZEN_REPLIES)
                .map_err(|e| catalog_err("open calvin_frozen_replies", e))?;
            let key = (vshard_id, epoch, position);
            let held = table
                .get(key)
                .map_err(|e| catalog_err("get calvin_frozen_replies", e))?
                .is_some();
            if held {
                return Ok(false);
            }
            table
                .insert(key, reply)
                .map_err(|e| catalog_err("insert calvin_frozen_replies", e))?;
        }
        write_txn
            .commit()
            .map_err(|e| catalog_err("commit calvin_frozen_replies", e))?;
        Ok(true)
    }

    /// The row of `(vshard_id, epoch, position)`, when one is stored.
    pub fn load_calvin_frozen_reply(
        &self,
        vshard_id: u32,
        epoch: u64,
        position: u32,
    ) -> crate::Result<Option<Vec<u8>>> {
        let read_txn = self
            .db
            .begin_read()
            .map_err(|e| catalog_err("load_calvin_frozen_reply read txn", e))?;
        let table = read_txn
            .open_table(CALVIN_FROZEN_REPLIES)
            .map_err(|e| catalog_err("open calvin_frozen_replies", e))?;
        let row = table
            .get((vshard_id, epoch, position))
            .map_err(|e| catalog_err("get calvin_frozen_replies", e))?;
        Ok(row.map(|row| row.value().to_vec()))
    }

    /// Whether a row is stored at `(vshard_id, epoch, position)`. Reads no
    /// reply bytes.
    pub fn has_calvin_frozen_reply(
        &self,
        vshard_id: u32,
        epoch: u64,
        position: u32,
    ) -> crate::Result<bool> {
        let read_txn = self
            .db
            .begin_read()
            .map_err(|e| catalog_err("has_calvin_frozen_reply read txn", e))?;
        let table = read_txn
            .open_table(CALVIN_FROZEN_REPLIES)
            .map_err(|e| catalog_err("open calvin_frozen_replies", e))?;
        let row = table
            .get((vshard_id, epoch, position))
            .map_err(|e| catalog_err("get calvin_frozen_replies", e))?;
        Ok(row.is_some())
    }

    /// The `(vshard_id, epoch, position)` key of every stored row of
    /// `vshard_id`, in key order. Reads no reply bytes.
    pub fn calvin_frozen_reply_keys(&self, vshard_id: u32) -> crate::Result<Vec<(u32, u64, u32)>> {
        let read_txn = self
            .db
            .begin_read()
            .map_err(|e| catalog_err("calvin_frozen_reply_keys read txn", e))?;
        let table = read_txn
            .open_table(CALVIN_FROZEN_REPLIES)
            .map_err(|e| catalog_err("open calvin_frozen_replies", e))?;
        let range = table
            .range(vshard_range(vshard_id))
            .map_err(|e| catalog_err("range calvin_frozen_replies", e))?;
        let mut keys = Vec::new();
        for row in range {
            let (key, _) = row.map_err(|e| catalog_err("read calvin_frozen_replies", e))?;
            keys.push(key.value());
        }
        Ok(keys)
    }

    /// The `(vshard_id, epoch, position)` key of every stored row, in key
    /// order. Reads no reply bytes.
    pub fn all_calvin_frozen_reply_keys(&self) -> crate::Result<Vec<(u32, u64, u32)>> {
        let read_txn = self
            .db
            .begin_read()
            .map_err(|e| catalog_err("all_calvin_frozen_reply_keys read txn", e))?;
        let table = read_txn
            .open_table(CALVIN_FROZEN_REPLIES)
            .map_err(|e| catalog_err("open calvin_frozen_replies", e))?;
        let range = table
            .iter()
            .map_err(|e| catalog_err("iter calvin_frozen_replies", e))?;
        let mut keys = Vec::new();
        for row in range {
            let (key, _) = row.map_err(|e| catalog_err("read calvin_frozen_replies", e))?;
            keys.push(key.value());
        }
        Ok(keys)
    }

    /// Every stored row of the vShards in `vshards`, in key order.
    pub fn load_calvin_frozen_replies(
        &self,
        vshards: &BTreeSet<u32>,
    ) -> crate::Result<Vec<StoredFrozenReply>> {
        let read_txn = self
            .db
            .begin_read()
            .map_err(|e| catalog_err("load_calvin_frozen_replies read txn", e))?;
        let table = read_txn
            .open_table(CALVIN_FROZEN_REPLIES)
            .map_err(|e| catalog_err("open calvin_frozen_replies", e))?;
        let mut out = Vec::new();
        for vshard_id in vshards {
            let range = table
                .range(vshard_range(*vshard_id))
                .map_err(|e| catalog_err("range calvin_frozen_replies", e))?;
            for row in range {
                let (key, value) = row.map_err(|e| catalog_err("read calvin_frozen_replies", e))?;
                let (vshard_id, epoch, position) = key.value();
                out.push(StoredFrozenReply {
                    vshard_id,
                    epoch,
                    position,
                    reply: value.value().to_vec(),
                });
            }
        }
        Ok(out)
    }

    /// Remove the rows of `keys`, each `(vshard_id, epoch, position)`, in
    /// one transaction. A key with no row changes nothing.
    pub fn remove_calvin_frozen_replies(&self, keys: &[(u32, u64, u32)]) -> crate::Result<()> {
        if keys.is_empty() {
            return Ok(());
        }
        let write_txn = self
            .db
            .begin_write()
            .map_err(|e| catalog_err("remove_calvin_frozen_replies txn", e))?;
        {
            let mut table = write_txn
                .open_table(CALVIN_FROZEN_REPLIES)
                .map_err(|e| catalog_err("open calvin_frozen_replies", e))?;
            for key in keys {
                table
                    .remove(*key)
                    .map_err(|e| catalog_err("remove calvin_frozen_replies", e))?;
            }
        }
        write_txn
            .commit()
            .map_err(|e| catalog_err("commit calvin_frozen_replies", e))
    }

    /// Replace every row of the vShards in `vshards` with `rows`, in one
    /// durable transaction. A data-group snapshot install brings the rows
    /// the builder held for the group's vShards: they replace this node's.
    pub fn replace_calvin_frozen_replies(
        &self,
        vshards: &BTreeSet<u32>,
        rows: &[StoredFrozenReply],
    ) -> crate::Result<()> {
        let mut write_txn = self
            .db
            .begin_write()
            .map_err(|e| catalog_err("replace_calvin_frozen_replies txn", e))?;
        write_txn
            .set_durability(Durability::Immediate)
            .map_err(|e| catalog_err("replace_calvin_frozen_replies durability", e))?;
        {
            let mut table = write_txn
                .open_table(CALVIN_FROZEN_REPLIES)
                .map_err(|e| catalog_err("open calvin_frozen_replies", e))?;
            for vshard_id in vshards {
                table
                    .retain_in(vshard_range(*vshard_id), |_, _| false)
                    .map_err(|e| catalog_err("clear calvin_frozen_replies", e))?;
            }
            for row in rows {
                table
                    .insert(
                        (row.vshard_id, row.epoch, row.position),
                        row.reply.as_slice(),
                    )
                    .map_err(|e| catalog_err("insert calvin_frozen_replies", e))?;
            }
        }
        write_txn
            .commit()
            .map_err(|e| catalog_err("commit calvin_frozen_replies", e))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_first_stored_copy_stays() {
        let catalog = SystemCatalog::open_in_memory().expect("catalog");
        assert!(
            catalog
                .store_calvin_frozen_reply(3, 7, 1, b"first")
                .expect("store")
        );
        assert!(
            !catalog
                .store_calvin_frozen_reply(3, 7, 1, b"second")
                .expect("store")
        );
        assert_eq!(
            catalog.load_calvin_frozen_reply(3, 7, 1).expect("load"),
            Some(b"first".to_vec())
        );
        assert_eq!(
            catalog.load_calvin_frozen_reply(3, 7, 2).expect("load"),
            None
        );
    }

    #[test]
    fn rows_load_by_vshard_and_go_on_remove_and_replace() {
        let catalog = SystemCatalog::open_in_memory().expect("catalog");
        for (vshard, epoch) in [(3, 1), (3, 2), (4, 1)] {
            catalog
                .store_calvin_frozen_reply(vshard, epoch, 0, b"r")
                .expect("store");
        }
        let three = catalog
            .load_calvin_frozen_replies(&BTreeSet::from([3]))
            .expect("load");
        assert_eq!(
            three.iter().map(|row| row.epoch).collect::<Vec<_>>(),
            vec![1, 2]
        );
        catalog
            .remove_calvin_frozen_replies(&[(3, 1, 0)])
            .expect("remove");
        assert_eq!(
            catalog.load_calvin_frozen_reply(3, 1, 0).expect("load"),
            None
        );
        catalog
            .replace_calvin_frozen_replies(
                &BTreeSet::from([3]),
                &[StoredFrozenReply {
                    vshard_id: 3,
                    epoch: 9,
                    position: 0,
                    reply: b"x".to_vec(),
                }],
            )
            .expect("replace");
        let three = catalog
            .load_calvin_frozen_replies(&BTreeSet::from([3]))
            .expect("load");
        assert_eq!(
            three.iter().map(|row| row.epoch).collect::<Vec<_>>(),
            vec![9]
        );
        assert!(
            catalog
                .load_calvin_frozen_reply(4, 1, 0)
                .expect("load")
                .is_some(),
            "another vShard's rows stay"
        );
    }

    #[test]
    fn every_key_lists_in_key_order() {
        let catalog = SystemCatalog::open_in_memory().expect("catalog");
        for (vshard, epoch) in [(4, 1), (3, 2), (3, 1)] {
            catalog
                .store_calvin_frozen_reply(vshard, epoch, 0, b"r")
                .expect("store");
        }
        assert_eq!(
            catalog.all_calvin_frozen_reply_keys().expect("keys"),
            vec![(3, 1, 0), (3, 2, 0), (4, 1, 0)]
        );
    }
}
