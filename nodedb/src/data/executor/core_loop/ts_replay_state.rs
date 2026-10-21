// SPDX-License-Identifier: BUSL-1.1

//! The timeseries replay stamps a core holds, and the cursor of the restart
//! replay's timeseries pass.

use std::collections::HashMap;

use nodedb_types::DatabaseId;

use crate::data::executor::timeseries_checkpoint::stamp::TsReplayStamp;
use crate::types::TenantId;

/// Replay stamps of every timeseries collection on this core.
#[derive(Default)]
pub(in crate::data::executor) struct TsReplayState {
    /// Per-collection replay stamps: the records whose rows a partition
    /// holds or a truncate removed, and the truncates that took effect. See
    /// `timeseries_checkpoint::stamp`. Key: (DatabaseId, TenantId, collection).
    pub(in crate::data::executor) stamps: HashMap<(DatabaseId, TenantId, String), TsReplayStamp>,
    /// While restart replay runs the timeseries pass: the LSN through which
    /// it has passed every record. A flush or truncate then stamps through it
    /// instead of the core stamp. `None` outside that pass.
    pub(in crate::data::executor) cursor: Option<u64>,
}
