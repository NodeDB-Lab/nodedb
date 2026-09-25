// SPDX-License-Identifier: BUSL-1.1

//! Load permission-tree state before the gateway serves.
//!
//! The permission cache is in-memory. After a restart it holds the tree
//! definitions from the catalog, but none of the hierarchy edges or grants
//! stored in the source collections. This step reads them, once every data
//! group finished its replay, so the first statement plans against the
//! grants that were in force before the restart.

use std::sync::Arc;

use crate::control::security::permission_tree::reload;
use crate::control::state::SharedState;

/// Apply the tree-definition changes the metadata replay committed, then
/// load every tree's edges and grants from this node's cores.
pub async fn load_permission_trees(shared: &Arc<SharedState>) -> crate::Result<()> {
    {
        let mut cache = shared.permission_cache.write().await;
        shared.authorization_fence.tree_defs().apply_to(&mut cache);
    }
    reload::reload_all(shared, None).await
}
