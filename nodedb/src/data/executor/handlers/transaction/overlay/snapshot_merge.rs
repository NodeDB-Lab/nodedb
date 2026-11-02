// SPDX-License-Identifier: BUSL-1.1

//! Fold a transaction's staging overlay into a document scan read at the
//! transaction's snapshot.
//!
//! A snapshot scan reads the version of each row visible at the snapshot
//! ordinal, and hands its bodies downstream normalized to MessagePack. The
//! staged bodies keep the stored encoding: a Binary Tuple for a strict
//! collection. This merge normalizes each staged body the same way before it
//! re-checks the scan predicate, so a staged row and a snapshot row reach
//! the downstream stages in one shape.

use nodedb_types::StorageKey;
use nodedb_types::columnar::StrictSchema;

use crate::data::executor::core_loop::CoreLoop;
use crate::data::executor::scan_normalize::sparse_body_to_msgpack;
use crate::data::executor::sparse_body_format::SparseBodyFormatRef;
use crate::types::{DatabaseId, TenantId, TxnId};

use super::merge::merge_staged_rows;

impl CoreLoop {
    /// Merge the overlay for `txn_id` into `rows`, the MessagePack-normalized
    /// rows of a scan at the transaction's snapshot. `strict_schema` is the
    /// collection's schema, `None` for a schemaless collection. `matches` is
    /// the predicate the snapshot scan applied, evaluated on a normalized
    /// body. No-op when the transaction has no overlay.
    pub(in crate::data::executor) fn merge_overlay_into_snapshot_scan(
        &self,
        txn_id: TxnId,
        coll_key: &(DatabaseId, TenantId, String),
        strict_schema: Option<&StrictSchema>,
        rows: &mut Vec<(StorageKey, Vec<u8>)>,
        matches: &dyn Fn(&StorageKey, &[u8]) -> bool,
    ) {
        // Read-your-own-writes refreshes the lease (see the reaper).
        self.touch_overlay(txn_id);
        let Some(overlay) = self.txn_overlays.get(&txn_id) else {
            return;
        };
        merge_snapshot_rows(overlay, coll_key, strict_schema, rows, matches);
    }
}

/// Fold `overlay`'s staged rows for `coll_key` into `rows`, normalizing each
/// staged body to MessagePack under `strict_schema` first.
fn merge_snapshot_rows(
    overlay: &super::TxnOverlay,
    coll_key: &(DatabaseId, TenantId, String),
    strict_schema: Option<&StrictSchema>,
    rows: &mut Vec<(StorageKey, Vec<u8>)>,
    matches: &dyn Fn(&StorageKey, &[u8]) -> bool,
) {
    let format = SparseBodyFormatRef::from_schema(strict_schema);
    let Ok(()) = merge_staged_rows::<std::convert::Infallible>(
        overlay,
        coll_key,
        rows,
        matches,
        &|_, body| Ok(sparse_body_to_msgpack(body, format).into_owned()),
    );
}

#[cfg(test)]
mod tests {
    use super::*;
    use nodedb_types::{RowIdentity, Surrogate};

    fn key() -> (DatabaseId, TenantId, String) {
        (DatabaseId::DEFAULT, TenantId::new(1), "docs".to_string())
    }

    /// The MessagePack map `{"v": v}`.
    fn map(v: u8) -> Vec<u8> {
        vec![0x81, 0xa1, b'v', v]
    }

    fn row(surrogate: u32, body: &[u8]) -> (StorageKey, Vec<u8>) {
        (
            StorageKey::for_surrogate(Surrogate::new(surrogate)),
            body.to_vec(),
        )
    }

    /// A snapshot scan's rows take the transaction's staged writes: a staged
    /// put replaces its snapshot row, a staged delete hides its row, and a
    /// staged insert joins the rows.
    #[test]
    fn staged_writes_merge_over_the_snapshot_rows() {
        let mut overlay = super::super::TxnOverlay::new();
        overlay.insert_put(key(), 1, &RowIdentity::from_user_key("a"), map(1));
        overlay.insert_tombstone(key(), 2, &RowIdentity::from_user_key("b"));
        overlay.insert_put(key(), 3, &RowIdentity::from_user_key("c"), map(3));
        let mut rows = vec![row(1, &map(0)), row(2, &map(0))];
        merge_snapshot_rows(&overlay, &key(), None, &mut rows, &|_, _| true);
        rows.sort_by_key(|(key, _)| key.surrogate().as_u32());
        assert_eq!(rows, vec![row(1, &map(1)), row(3, &map(3))]);
    }

    /// A staged row the scan predicate rejects never joins the rows.
    #[test]
    fn a_staged_row_meets_the_scan_predicate() {
        let mut overlay = super::super::TxnOverlay::new();
        overlay.insert_put(key(), 5, &RowIdentity::from_user_key("e"), map(5));
        let mut rows = Vec::new();
        merge_snapshot_rows(&overlay, &key(), None, &mut rows, &|_, _| false);
        assert!(rows.is_empty());
    }
}
