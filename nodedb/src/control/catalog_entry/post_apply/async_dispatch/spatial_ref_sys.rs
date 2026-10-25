// SPDX-License-Identifier: BUSL-1.1

//! Async post-apply for user-defined CRS catalog entries.
//!
//! `PutSpatialRefSys` and `DropSpatialRefSys` change the catalog's
//! user-defined CRS set. Each Data Plane core resolves SRIDs against its own
//! copy, so this sends the whole set to every core on this node. Both run on
//! every node, so every core in the cluster resolves a user-defined SRID the
//! way the statement's node does.
//!
//! The metadata applier awaits the install before the entry counts as
//! applied. A core that does not apply the set fails the install: the
//! applier stops the batch at the entry, and the re-delivered entry sends
//! the set again. So no later metadata entry, and no replicated write held
//! behind this entry's metadata index, reaches a core that lacks the set.

use nodedb_physical::physical_plan::MetaOp;

use crate::bridge::envelope::PhysicalPlan;
use crate::control::state::SharedState;

use super::core_fanout::{CoreFanout, dispatch_to_every_core};

/// Name the fan-out reports a CRS set dispatch under. The set belongs to no
/// collection, and the fan-out addresses each core directly.
const SPATIAL_REF_SYS_SENTINEL_COLLECTION: &str = "_spatial_ref_sys";

/// Send the catalog's current user-defined CRS set to every core on this
/// node. `cause` names the change for the report: `define`, `drop` or
/// `image`. `Err` when a core did not apply the set.
pub async fn install_on_every_core(shared: &SharedState, cause: &'static str) -> crate::Result<()> {
    let set = shared.credentials.catalog().custom_crs_set();
    let entries = set.len();
    let plan = PhysicalPlan::Meta(MetaOp::SetCustomCrsSet {
        set: nodedb_crs::CustomCrsSet::clone(&set),
    });
    let fanout = CoreFanout {
        database_id: crate::types::DatabaseId::DEFAULT.as_u64(),
        tenant_id: 0,
        collection: SPATIAL_REF_SYS_SENTINEL_COLLECTION,
        what: "user-defined CRS set",
        detail: cause,
    };
    dispatch_to_every_core(shared, &fanout, &plan)
        .await
        .inspect_err(|error| crate::diag::custom_crs_set_not_applied(error, cause, entries))
}
