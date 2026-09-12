// SPDX-License-Identifier: BUSL-1.1

//! Versioned document storage — bitemporal key layout backed by redb.
//!
//! Key: `"{tenant}:{coll}:{doc_id}\x00{system_from_ms:020}"`, where `doc_id`
//! is a [`nodedb_types::StorageKey`]'s 8-hex-character rendering — a hex
//! render cannot contain the `\x00` version separator.
//! Value: `[tag:u8][valid_from_ms:i64 LE][valid_until_ms:i64 LE][body...]`
//! where `tag = 0x00` (live), `0xFF` (tombstone), `0xFE` (GDPR erased).

pub mod doc;
pub mod index;
pub mod key;
pub mod purge;
pub mod scan;
pub mod value;

pub use doc::VersionedRow;
pub use key::{
    coll_prefix, coll_prefix_end, doc_prefix, doc_prefix_end, format_sys_from, parse_sys_from,
    tenant_prefix, tenant_prefix_end, versioned_doc_key,
};
pub use value::{
    DecodedValue, TAG_GDPR_ERASED, TAG_LIVE, TAG_TOMBSTONE, VersionedIndexEntry, VersionedPut,
    VersionedScanParams, decode_value, encode_value,
};
