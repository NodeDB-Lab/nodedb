// SPDX-License-Identifier: BUSL-1.1

//! The header record of framed redo content: the fields of a redo that are
//! not row-sized lists.

use nodedb_physical::physical_plan::RedoSumTargets;

use crate::wal::{CalvinStamp, CrossShardAppliedKey};

/// The fields of a redo that are not row-sized lists. The first record of
/// framed content holds it.
#[derive(Debug, Clone, PartialEq, zerompk::ToMessagePack, zerompk::FromMessagePack)]
pub struct RedoContentHeader {
    pub version: u16,
    pub calvin_stamp: Option<CalvinStamp>,
    pub cross_shard_applied: Option<CrossShardAppliedKey>,
    pub user_roles: Vec<String>,
    pub sum_targets: Vec<RedoSumTargets>,
}
