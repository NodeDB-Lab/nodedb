//! `ArrayCatalogEntry` — the persistent schema digest for one array.
//!
//! The schema is carried as an opaque msgpack blob so
//! `nodedb-array`'s typed `ArraySchema` (and its transitive types) don't
//! leak into Control-Plane catalog code. The Data Plane decodes the
//! blob on open.

use nodedb_array::types::ArrayId;
use serde::{Deserialize, Serialize};

fn default_prefix_bits() -> u8 {
    8
}

/// One catalog row for a registered array.
#[derive(
    Debug,
    Clone,
    PartialEq,
    Serialize,
    Deserialize,
    zerompk::ToMessagePack,
    zerompk::FromMessagePack,
)]
pub struct ArrayCatalogEntry {
    pub array_id: ArrayId,
    pub name: String,
    /// zerompk-encoded `ArraySchema` (decoded on Data-Plane open).
    pub schema_msgpack: Vec<u8>,
    /// Content-addressed hash of the schema, used to reject open-with-
    /// mismatched-schema attempts without re-decoding the blob.
    pub schema_hash: u64,
    /// Wall-clock creation time, epoch-millis.
    pub created_at_ms: i64,
    /// Number of high-order Hilbert-prefix bits used to assign tiles to
    /// vShards. Immutable after CREATE ARRAY. Default 8 → 256 buckets.
    /// Existing rows without this field deserialize to 8.
    #[serde(default = "default_prefix_bits")]
    pub prefix_bits: u8,
    /// Bitemporal audit retention window in milliseconds. Compaction may
    /// discard superseded tile versions older than `now - audit_retain_ms`.
    /// `None` means retain all versions forever (default).
    #[serde(default)]
    pub audit_retain_ms: Option<i64>,
}
