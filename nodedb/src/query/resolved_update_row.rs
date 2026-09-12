// SPDX-License-Identifier: BUSL-1.1

//! The wire shape of one resolved update arm. A MERGE or `UPDATE ... FROM`
//! runs in two passes — Data Plane resolves, Control Plane applies — and
//! this tuple travels between three encode/decode sites. Naming it once
//! keeps them from drifting into subtly different tuples.

/// `(document_id, surrogate, pre_image, post_image)` for one matched row.
/// Both images travel — a materialized sum folds a delta from the pair, and
/// a rewritten join key moves value between two targets, neither derivable
/// from the post-image alone. Bodies are schemaless wire form, never a
/// stored Binary Tuple, or the write path would double-encode them. The
/// surrogate is never absent: every matched row is a storage-keyed row.
pub type ResolvedUpdateRowWire = (String, u32, Vec<u8>, Vec<u8>);
