// SPDX-License-Identifier: BUSL-1.1

//! The stored-bytes change one write transaction makes, summed per
//! `(database_id, tenant_id, collection)` and applied once before commit.

use redb::{ReadableTable, WriteTransaction};

use super::table::{STORAGE_BYTES, counter_err, row_bytes};

/// Signed stored-bytes changes of one write transaction, one entry per
/// `(database_id, tenant_id, collection)`.
///
/// A writer records each row it inserts, overwrites or removes, then calls
/// [`Self::apply`] inside the same transaction before it commits.
#[derive(Debug, Default)]
pub struct StorageDelta {
    entries: Vec<(u64, u64, String, i64)>,
}

impl StorageDelta {
    /// Add `bytes` (negative to subtract) to the change of one collection.
    pub fn add(&mut self, database_id: u64, tenant_id: u64, collection: &str, bytes: i64) {
        if bytes == 0 {
            return;
        }
        let found = self
            .entries
            .iter_mut()
            .find(|(d, t, c, _)| *d == database_id && *t == tenant_id && c == collection);
        match found {
            Some((_, _, _, sum)) => *sum = sum.saturating_add(bytes),
            None => self
                .entries
                .push((database_id, tenant_id, collection.to_owned(), bytes)),
        }
    }

    /// Record one stored row: `key_len` key bytes and `value_len` value bytes.
    pub fn stored(
        &mut self,
        database_id: u64,
        tenant_id: u64,
        collection: &str,
        key_len: usize,
        value_len: usize,
    ) {
        self.add(
            database_id,
            tenant_id,
            collection,
            row_bytes(key_len, value_len),
        );
    }

    /// Record one removed row: `key_len` key bytes and `value_len` value bytes.
    pub fn removed(
        &mut self,
        database_id: u64,
        tenant_id: u64,
        collection: &str,
        key_len: usize,
        value_len: usize,
    ) {
        self.add(
            database_id,
            tenant_id,
            collection,
            row_bytes(key_len, value_len).saturating_neg(),
        );
    }

    /// Record one insert under a key: `new_len` value bytes stored, and the
    /// `old_len` value bytes the key held before, when it held a row.
    pub fn replaced(
        &mut self,
        database_id: u64,
        tenant_id: u64,
        collection: &str,
        key_len: usize,
        old_len: Option<usize>,
        new_len: usize,
    ) {
        match old_len {
            Some(old) => {
                let change = row_bytes(0, new_len).saturating_sub(row_bytes(0, old));
                self.add(database_id, tenant_id, collection, change);
            }
            None => self.stored(database_id, tenant_id, collection, key_len, new_len),
        }
    }

    /// Whether the transaction changed no stored bytes.
    pub fn is_empty(&self) -> bool {
        self.entries.iter().all(|(_, _, _, sum)| *sum == 0)
    }

    /// The change summed per `(database_id, tenant_id)`.
    pub fn tenant_sums(&self) -> Vec<(u64, u64, i64)> {
        let mut sums: Vec<(u64, u64, i64)> = Vec::new();
        for (d, t, _, sum) in &self.entries {
            match sums.iter_mut().find(|(sd, st, _)| sd == d && st == t) {
                Some((_, _, total)) => *total = total.saturating_add(*sum),
                None => sums.push((*d, *t, *sum)),
            }
        }
        sums
    }

    /// Move every collection's counter by its change inside `txn`. `engine`
    /// names the redb file in errors and reports.
    ///
    /// A change that takes a counter below zero is a broken invariant: the
    /// counter missed a stored row. It is reported, and the counter clamps
    /// at zero. A counter that reaches zero loses its row.
    pub fn apply(self, txn: &WriteTransaction, engine: &'static str) -> crate::Result<()> {
        if self.is_empty() {
            return Ok(());
        }
        let mut table = txn
            .open_table(STORAGE_BYTES)
            .map_err(|e| counter_err(engine, "open", e))?;
        for (database_id, tenant_id, collection, change) in &self.entries {
            if *change == 0 {
                continue;
            }
            let key = (*database_id, *tenant_id, collection.as_str());
            let held = table
                .get(key)
                .map_err(|e| counter_err(engine, "read", e))?
                .map(|guard| guard.value())
                .unwrap_or(0);
            let next = match u64::try_from(*change) {
                Ok(added) => held.saturating_add(added),
                Err(_) => {
                    let removed = change.unsigned_abs();
                    if removed > held {
                        crate::diag::storage_bytes_underflow(
                            engine,
                            *database_id,
                            *tenant_id,
                            Some(collection.as_str()),
                            held,
                            removed,
                        );
                    }
                    held.saturating_sub(removed)
                }
            };
            if next == 0 {
                table
                    .remove(key)
                    .map_err(|e| counter_err(engine, "clear", e))?;
            } else {
                table
                    .insert(key, next)
                    .map_err(|e| counter_err(engine, "write", e))?;
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn changes_sum_per_collection_and_per_tenant() {
        let mut delta = StorageDelta::default();
        delta.stored(1, 2, "a", 10, 5);
        delta.stored(1, 2, "a", 1, 1);
        delta.removed(1, 2, "b", 3, 0);
        delta.stored(1, 3, "a", 4, 0);
        assert_eq!(delta.entries.len(), 3);
        assert_eq!(delta.entries[0].3, 17);
        let mut sums = delta.tenant_sums();
        sums.sort_unstable();
        assert_eq!(sums, vec![(1, 2, 14), (1, 3, 4)]);
    }

    #[test]
    fn an_overwrite_counts_the_value_difference() {
        let mut delta = StorageDelta::default();
        delta.replaced(0, 0, "c", 8, Some(10), 4);
        assert_eq!(delta.entries[0].3, -6);
        let mut fresh = StorageDelta::default();
        fresh.replaced(0, 0, "c", 8, None, 4);
        assert_eq!(fresh.entries[0].3, 12);
    }

    #[test]
    fn a_cancelled_change_is_empty() {
        let mut delta = StorageDelta::default();
        delta.stored(0, 0, "c", 2, 2);
        delta.removed(0, 0, "c", 2, 2);
        assert!(delta.is_empty());
    }
}
