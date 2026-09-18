// SPDX-License-Identifier: BUSL-1.1

//! Parameter structs shared by the KV CRUD handlers.

use nodedb_types::Surrogate;

/// Parameters for `INSERT ... ON CONFLICT (key) DO UPDATE SET ...` on KV.
pub(in crate::data::executor) struct KvInsertOnConflictUpdateParams<'a> {
    pub did: u64,
    pub tid: u64,
    pub collection: &'a str,
    pub key: &'a [u8],
    pub value: &'a [u8],
    pub ttl_ms: u64,
    pub updates: &'a [(String, nodedb_physical::physical_plan::UpdateValue)],
    pub surrogate: Surrogate,
    /// Compiled row-level-security WRITE predicate. Whichever body this op
    /// ends up persisting — the incoming row or its merge with the stored one
    /// — is decided against it.
    pub rls_write_check: &'a nodedb_types::RlsWriteCheck,
    /// When `Some`, project the STORED post-image per spec instead of
    /// reporting a bare status/count.
    pub returning: Option<&'a nodedb_physical::physical_plan::ReturningSpec>,
    /// Compiled read policy bounding which of those rows may be shown back.
    pub rls_filters: &'a [u8],
}

/// Parameters for a KV `DELETE` by primary key(s).
pub(in crate::data::executor) struct KvDeleteParams<'a> {
    pub did: u64,
    pub tid: u64,
    pub collection: &'a str,
    pub keys: &'a [Vec<u8>],
    /// Compiled row-level-security WRITE predicate. The row a delete removes
    /// is the image the policy decides.
    pub rls_write_check: &'a nodedb_types::RlsWriteCheck,
    /// When `Some`, project the STORED pre-image of every removed row per
    /// spec instead of reporting a bare count.
    pub returning: Option<&'a nodedb_physical::physical_plan::ReturningSpec>,
    /// Compiled read policy bounding which of those rows may be shown back.
    pub rls_filters: &'a [u8],
}

/// Parameters for a KV point `GET`.
pub(in crate::data::executor) struct KvGetParams<'a> {
    pub did: u64,
    pub tid: u64,
    pub collection: &'a str,
    pub key: &'a [u8],
    pub rls_filters: &'a [u8],
    pub surrogate_ceiling: Option<u32>,
}

/// Parameters for a KV point write (`PUT` / `INSERT` / `INSERT ... IF ABSENT`).
pub(in crate::data::executor) struct KvWriteParams<'a> {
    pub did: u64,
    pub tid: u64,
    pub collection: &'a str,
    pub key: &'a [u8],
    pub value: &'a [u8],
    pub ttl_ms: u64,
    pub surrogate: Surrogate,
    /// When `Some`, project the STORED post-image per spec instead of
    /// reporting a bare status/count.
    pub returning: Option<&'a nodedb_physical::physical_plan::ReturningSpec>,
    /// Compiled read policy bounding which of those rows may be shown back.
    pub rls_filters: &'a [u8],
}
