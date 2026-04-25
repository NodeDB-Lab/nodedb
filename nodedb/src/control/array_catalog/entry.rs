//! `ArrayCatalogEntry` — the persistent schema digest for one array.
//!
//! The schema is carried as an opaque msgpack blob so
//! `nodedb-array`'s typed `ArraySchema` (and its transitive types) don't
//! leak into Control-Plane catalog code. Sub-pass 1c decodes the blob
//! only inside the Data Plane on open.

use nodedb_array::types::ArrayId;
use serde::{Deserialize, Serialize};

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
}
