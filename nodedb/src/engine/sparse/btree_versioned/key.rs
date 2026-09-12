// SPDX-License-Identifier: BUSL-1.1

//! Key layout for the versioned document and index tables.

use nodedb_types::StorageKey;

/// 20-digit zero-pad for i64 lexicographic ordering under reverse-scan.
pub fn format_sys_from(sys_from_ms: i64) -> String {
    format!("{sys_from_ms:020}")
}

/// Build a versioned document key.
pub fn versioned_doc_key(
    database_id: u64,
    tenant: u64,
    coll: &str,
    doc_id: &StorageKey,
    sys_from_ms: i64,
) -> String {
    format!(
        "{database_id}:{tenant}:{coll}:{doc_id}\x00{}",
        format_sys_from(sys_from_ms)
    )
}

/// Prefix matching every version of a single doc_id — used by reverse-scan.
pub fn doc_prefix(database_id: u64, tenant: u64, coll: &str, doc_id: &StorageKey) -> String {
    format!("{database_id}:{tenant}:{coll}:{doc_id}\x00")
}

/// Upper-bound exclusive companion of [`doc_prefix`]: because `\x00` is
/// the minimum byte, `\x01` is the next-greater separator and bounds all
/// suffixes for this doc_id cleanly.
pub fn doc_prefix_end(database_id: u64, tenant: u64, coll: &str, doc_id: &StorageKey) -> String {
    format!("{database_id}:{tenant}:{coll}:{doc_id}\x01")
}

/// Prefix matching every version of every doc_id in a collection.
pub fn coll_prefix(database_id: u64, tenant: u64, coll: &str) -> String {
    format!("{database_id}:{tenant}:{coll}:")
}

/// Upper-bound exclusive companion of [`coll_prefix`]. `:` = 0x3A; `;` =
/// 0x3B is the next byte, giving a clean exclusive upper bound that still
/// holds after the leading `{database_id}:` is prepended.
pub fn coll_prefix_end(database_id: u64, tenant: u64, coll: &str) -> String {
    format!("{database_id}:{tenant}:{coll};")
}

/// Prefix matching every version of every doc_id in every collection of a
/// single `(database_id, tenant)` — used by the whole-tenant hard drop.
pub fn tenant_prefix(database_id: u64, tenant: u64) -> String {
    format!("{database_id}:{tenant}:")
}

/// Upper-bound exclusive companion of [`tenant_prefix`]. The tenant field is
/// terminated by `:` (0x3A); `;` (0x3B) is the next byte, giving a clean
/// exclusive upper bound over every collection under this tenant.
pub fn tenant_prefix_end(database_id: u64, tenant: u64) -> String {
    format!("{database_id}:{tenant};")
}

/// Extract `sys_from_ms` from a versioned key. Returns `None` if the key
/// has no NUL separator (defensive — should not happen for keys produced
/// by [`versioned_doc_key`]).
pub fn parse_sys_from(key: &str) -> Option<i64> {
    let (_, suffix) = key.rsplit_once('\x00')?;
    suffix.parse().ok()
}
