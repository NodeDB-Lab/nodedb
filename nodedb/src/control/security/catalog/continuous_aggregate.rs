// SPDX-License-Identifier: BUSL-1.1

//! Continuous-aggregate metadata persisted in the system catalog.
//!
//! The Data Plane's `continuous_agg_mgr` holds the runtime state
//! (per-bucket accumulators, watermark cursor); this record is the
//! durable definition every node replays at startup so the manager
//! can rebuild its registration without re-issuing the DDL.

use nodedb_types::Hlc;

/// Catalog-side projection of `ContinuousAggregateDef`.
///
/// `def_bytes` is the MessagePack-encoded
/// `engine::timeseries::continuous_agg::ContinuousAggregateDef` —
/// stored opaquely so the on-disk format does not depend on the
/// Data Plane's struct layout (the runtime type carries
/// SIMD/quantization tuning that has no place in the catalog).
#[derive(zerompk::ToMessagePack, zerompk::FromMessagePack, Debug, Clone)]
#[msgpack(map)]
pub struct StoredContinuousAggregate {
    /// Owning database. Scopes the catalog key so an aggregate named
    /// identically in two databases never collides on disk.
    pub database_id: u64,
    pub tenant_id: u64,
    pub name: String,
    pub source: String,
    /// MessagePack-encoded `ContinuousAggregateDef`. Decoded into the
    /// runtime type on Data Plane register dispatch.
    pub def_bytes: Vec<u8>,
    pub owner: String,
    pub created_at: u64,
    /// Monotonic descriptor version. See `StoredCollection::descriptor_version`.
    pub descriptor_version: u64,
    /// HLC assigned by the metadata applier.
    pub modification_hlc: Hlc,
}
