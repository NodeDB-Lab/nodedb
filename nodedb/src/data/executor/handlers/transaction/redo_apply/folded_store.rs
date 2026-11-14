// SPDX-License-Identifier: BUSL-1.1

//! The target rows a record's materialized-sum folds left, as the fold
//! check reads and writes them (`sum_fold_check`).
//!
//! A fold reads the row an earlier fold of the record left, else the stored
//! row. A folded row's UNIQUE values meet the values of every other row the
//! record's folds left. [`FoldedStore`] holds those rows. An in-memory map
//! holds them for a record the check judges whole.

use std::borrow::Cow;
use std::collections::{BTreeMap, BTreeSet};

use crate::data::executor::enforcement::unique::claimed_values;
use crate::engine::document::store::IndexPath;

/// One target row a fold of the record wrote: its document and the bytes
/// the install stores for it.
#[derive(Clone)]
pub(super) struct FoldedRow {
    pub(super) doc: serde_json::Value,
    pub(super) stored: Vec<u8>,
}

/// The target rows the folds of one record left so far.
pub(super) trait FoldedStore {
    /// The row the earlier folds left at `surrogate` of `collection`.
    /// `None` when no fold wrote it.
    fn folded(&self, collection: &str, surrogate: u32)
    -> crate::Result<Option<Cow<'_, FoldedRow>>>;

    /// Record `row` as the row the folds left at `surrogate` of
    /// `collection`.
    fn put(&mut self, collection: &str, surrogate: u32, row: FoldedRow) -> crate::Result<()>;

    /// Whether a fold wrote the row at `surrogate` of `collection`.
    fn holds(&self, collection: &str, surrogate: u32) -> crate::Result<bool>;

    /// The first of `needles`, in set order, that a folded row of
    /// `collection` other than `surrogate` claims under `path`.
    fn first_claimed_by_other(
        &self,
        collection: &str,
        surrogate: u32,
        path: &IndexPath,
        needles: &BTreeSet<String>,
        crs: &nodedb_crs::CrsContext<'_>,
    ) -> crate::Result<Option<String>>;
}

/// The folded rows of a record the check judges whole, keyed by
/// `(collection, surrogate)`.
pub(super) type FoldedTargets = BTreeMap<(String, u32), FoldedRow>;

impl FoldedStore for FoldedTargets {
    fn folded(
        &self,
        collection: &str,
        surrogate: u32,
    ) -> crate::Result<Option<Cow<'_, FoldedRow>>> {
        Ok(self
            .get(&(collection.to_string(), surrogate))
            .map(Cow::Borrowed))
    }

    fn put(&mut self, collection: &str, surrogate: u32, row: FoldedRow) -> crate::Result<()> {
        self.insert((collection.to_string(), surrogate), row);
        Ok(())
    }

    fn holds(&self, collection: &str, surrogate: u32) -> crate::Result<bool> {
        Ok(self.contains_key(&(collection.to_string(), surrogate)))
    }

    fn first_claimed_by_other(
        &self,
        collection: &str,
        surrogate: u32,
        path: &IndexPath,
        needles: &BTreeSet<String>,
        crs: &nodedb_crs::CrsContext<'_>,
    ) -> crate::Result<Option<String>> {
        let others: BTreeSet<String> = self
            .iter()
            .filter(|((owner, held), _)| owner == collection && *held != surrogate)
            .flat_map(|(_, row)| claimed_values(&row.doc, path, crs))
            .collect();
        Ok(needles
            .iter()
            .find(|needle| others.contains(*needle))
            .cloned())
    }
}
