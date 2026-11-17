// SPDX-License-Identifier: BUSL-1.1

//! UNIQUE claims from the final rows actually installed by one transaction.

use super::unique::PostImage;
use crate::data::executor::core_loop::CoreLoop;
use crate::data::executor::enforcement::materialized_sum::apply::TargetWrite;
use nodedb_types::StorageKey;
use redb::WriteTransaction;
use std::collections::{BTreeMap, HashMap};

impl CoreLoop {
    /// Judge one source mutation and every target it provisionally updates.
    pub(in crate::data::executor) fn check_source_and_targets_unique(
        &self,
        txn: &WriteTransaction,
        database_id: u64,
        tid: u64,
        source: (&str, StorageKey),
        targets: &[TargetWrite],
    ) -> crate::Result<()> {
        let mut writes = Vec::with_capacity(targets.len() + 1);
        writes.push((source.0.to_owned(), source.1));
        writes.extend(targets.iter().map(|target| {
            (
                target.collection.clone(),
                StorageKey::for_surrogate(target.surrogate),
            )
        }));
        self.check_written_unit_unique(txn, database_id, tid, &writes)
    }

    /// Judge actual final rows without rerunning defaults or generated expressions.
    pub(in crate::data::executor) fn check_written_unit_unique(
        &self,
        txn: &WriteTransaction,
        database_id: u64,
        tid: u64,
        writes: &[(String, StorageKey)],
    ) -> crate::Result<()> {
        let mut collections: BTreeMap<&str, Vec<StorageKey>> = BTreeMap::new();
        for (collection, key) in writes {
            collections.entry(collection).or_default().push(*key);
        }
        for (collection, keys) in collections {
            let Some(config) = self.unique_config(database_id, tid, collection) else {
                continue;
            };
            let last: HashMap<u32, usize> = keys
                .iter()
                .enumerate()
                .map(|(index, key)| (key.surrogate().as_u32(), index))
                .collect();
            let keys: Vec<_> = keys
                .into_iter()
                .enumerate()
                .filter_map(|(index, key)| {
                    (last.get(&key.surrogate().as_u32()) == Some(&index)).then_some(key)
                })
                .collect();
            let documents = keys
                .iter()
                .map(|key| {
                    self.current_row_body_in_txn(txn, database_id, tid, collection, key)?
                        .map(|body| self.decode_stored_document(config, &body))
                        .transpose()
                })
                .collect::<crate::Result<Vec<_>>>()?;
            let rows: Vec<_> = keys
                .iter()
                .zip(&documents)
                .map(|(key, doc)| PostImage {
                    surrogate: key.surrogate().as_u32(),
                    doc: doc.as_ref(),
                    judged: true,
                })
                .collect();
            self.check_unit_unique(database_id, tid, collection, &rows)?;
        }
        Ok(())
    }
}
