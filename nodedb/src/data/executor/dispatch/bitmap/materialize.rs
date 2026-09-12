// SPDX-License-Identifier: BUSL-1.1

//! Sub-plan execution → `SurrogateBitmap` materialization.
//!
//! The executor calls `collect_surrogates` after running a bitmap-producer
//! sub-plan. The sub-plan returns `(doc_id, bytes)` pairs whose `doc_id`
//! strings are 8-char lowercase-hex-encoded surrogate u32 values (the
//! document engine's canonical row-key encoding). This function parses those
//! ids into `Surrogate` values and accumulates them into a `SurrogateBitmap`.
//!
//! Non-hex doc_ids (e.g. from KV or columnar scans) are silently skipped —
//! the bitmap will be partial, and the executor will fall back to a full scan
//! for the remaining probe rows.

use nodedb_types::{Surrogate, SurrogateBitmap};

/// Parse a sequence of `(doc_id, _bytes)` pairs into a `SurrogateBitmap`.
///
/// Accepts 8-char lowercase-hex doc_ids produced by `StorageKey`'s `Display`
/// impl. Non-conforming ids are skipped without error.
pub(crate) fn collect_surrogates(docs: &[(String, Vec<u8>)]) -> SurrogateBitmap {
    let mut bm = SurrogateBitmap::new();
    for (doc_id, _) in docs {
        if let Ok(n) = u32::from_str_radix(doc_id, 16) {
            bm.insert(Surrogate::new(n));
        }
    }
    bm
}
