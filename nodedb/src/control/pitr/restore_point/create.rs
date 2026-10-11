// SPDX-License-Identifier: BUSL-1.1

//! Create and list cluster restore points.

use crate::control::metadata_proposer::propose_restore_point;
use crate::control::security::catalog::restore_points::StoredRestorePoint;
use crate::control::state::SharedState;

#[derive(Debug, thiserror::Error)]
pub enum RestorePointError {
    #[error(transparent)]
    Node(#[from] crate::Error),
}

impl From<RestorePointError> for crate::Error {
    fn from(e: RestorePointError) -> Self {
        match e {
            RestorePointError::Node(inner) => inner,
        }
    }
}

/// Create a cluster restore point at this node's HLC now, and wait until this
/// node applied it. Every node then cuts each group it hosts at the point.
pub async fn create_restore_point(
    state: &SharedState,
) -> Result<StoredRestorePoint, RestorePointError> {
    let hlc = state.hlc_clock.now().wall_ns;
    // Parks the proposal after the watermark is taken until the test releases
    // it: a test orders a metadata entry stamped at or above the watermark
    // before the point.
    #[cfg(feature = "failpoints")]
    crate::control::fail_gate::wait(
        crate::fail_point::FailScope::Node(state.node_id),
        "restore_point::after_watermark",
    )
    .await;
    let created_at_ms = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| u64::try_from(d.as_millis()).unwrap_or(u64::MAX))
        .unwrap_or(0);
    let id = propose_restore_point(state, hlc, created_at_ms).await?;
    Ok(StoredRestorePoint {
        id,
        hlc,
        created_at_ms,
    })
}

/// Every restore point this node applied, oldest first.
pub fn list_restore_points(
    state: &SharedState,
) -> Result<Vec<StoredRestorePoint>, RestorePointError> {
    Ok(state.credentials.catalog().list_restore_points()?)
}
