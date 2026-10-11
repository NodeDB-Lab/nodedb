// SPDX-License-Identifier: Apache-2.0

//! Types carried by `MetaOp::RestoreTenantSnapshot` and
//! `MetaOp::CreateTenantSnapshot`.

/// One collection a Raft snapshot install clears on a core before it
/// installs that core's share of the snapshot.
#[derive(
    Debug,
    Clone,
    PartialEq,
    Eq,
    serde::Serialize,
    serde::Deserialize,
    zerompk::ToMessagePack,
    zerompk::FromMessagePack,
)]
pub struct SnapshotClearTarget {
    pub database_id: u64,
    pub tenant_id: u64,
    /// The name the Data Plane stores the collection under:
    /// database-qualified outside the default database.
    pub collection: String,
    /// Whether this core also removes the collection's shared on-disk L1
    /// files. Those paths are keyed by `(database, tenant, collection)`, not
    /// by core, so exactly one core per collection (its home core) sets it.
    pub reclaim_l1_files: bool,
}

/// A database backup's request to capture its tenants at its cut barrier.
///
/// The barrier each data group's leader places for the backup carries it. The
/// group's leader, when it applies the first barrier of `request_id`,
/// snapshots every tenant of `tenants` in `database_id` before it applies the
/// next entry, and parks the capture for the backup to collect.
#[derive(
    Debug,
    Clone,
    PartialEq,
    Eq,
    serde::Serialize,
    serde::Deserialize,
    zerompk::ToMessagePack,
    zerompk::FromMessagePack,
)]
pub struct CutCaptureRequest {
    /// Unique per backup. It keys every parked capture.
    pub request_id: u64,
    pub database_id: u64,
    pub tenants: Vec<u64>,
}
