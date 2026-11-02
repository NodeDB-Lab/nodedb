// SPDX-License-Identifier: BUSL-1.1

//! Versioned document storage backed by redb: the only document store of
//! every document collection, bitemporal or not.
//!
//! Key: `"{tenant}:{coll}:{doc_id}\x00{system_from:020}"`, where `doc_id`
//! is a [`nodedb_types::StorageKey`]'s 8-hex-character rendering — a hex
//! render cannot contain the `\x00` version separator. `system_from` is the
//! version's system-time ordinal, from the clock graph edges stamp from.
//! Value: `[tag:u8][valid_from_ms:i64 LE][valid_until_ms:i64 LE][body...]`
//! where `tag = 0x00` (live), `0xFF` (tombstone), `0xFE` (GDPR erased).

pub mod doc;
pub mod history_bytes;
pub mod index;
pub mod index_visible;
pub mod key;
pub mod neighbors;
pub mod newest;
pub mod purge;
pub mod scan;
pub mod snapshot;
pub mod stamped;
pub mod value;
pub mod visible;

pub use doc::VersionedRow;
pub use index::VersionedIndexRange;
pub use key::{
    coll_prefix, coll_prefix_end, doc_prefix, doc_prefix_end, format_sys_from, parse_sys_from,
    tenant_prefix, tenant_prefix_end, versioned_doc_key,
};
pub use neighbors::{AdjacentVersion, VersionNeighbors};
pub use newest::{CollectionNewest, IndexNewestRange};
pub use stamped::{StampedScan, StampedVersion};
pub use value::{
    DecodedValue, TAG_GDPR_ERASED, TAG_LIVE, TAG_TOMBSTONE, VersionedIndexEntry, VersionedPut,
    VersionedScanParams, decode_value, encode_value,
};
pub use visible::VisibleVersion;
