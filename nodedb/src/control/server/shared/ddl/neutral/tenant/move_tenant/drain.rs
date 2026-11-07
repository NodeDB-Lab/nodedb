// SPDX-License-Identifier: BUSL-1.1

//! Drain phase for `MOVE TENANT`.
//!
//! Every live collection and every array of the source database goes through
//! the replicated descriptor-lease drain. While it is active, every node's planner refuses a
//! new plan on a drained collection as a retryable schema change. The phase
//! returns once no node holds a lease on any of them, so no write to a moving
//! collection is in flight when the capture runs, and none starts after.
//!
//! The drain is owned by this move ([`move_tenant_drain_owner`]) and covers
//! every descriptor version, so a DDL on a moving collection neither ends it
//! nor escapes it by bumping the version. It stays active until the cutover's
//! catalog entry applies, which ends it on every node, or until its holds drop
//! or [`release`] ends it. A dropped statement strands no drain.

use std::time::{Duration, Instant};

use crate::control::lease::{
    DrainHolds, drain_for_owner_async, end_drain_async, move_source_descriptor,
    move_tenant_drain_owner,
};
use crate::control::state::SharedState;
use crate::types::{DatabaseId, TenantId};
use nodedb_cluster::DescriptorId;
use nodedb_types::NodeDbError;

/// Run the drain phase.
///
/// A drain that does not finish within `timeout`, or a catalog read or
/// propose error, ends every drain this phase started and fails with the
/// retryable drain-timeout error. The source is left untouched.
///
/// The returned [`DrainHolds`] own the drains from before each start. Dropping
/// them or this future, as a killed connection does, ends the drains in the
/// background. The caller calls [`DrainHolds::keep`] once the cutover commits.
pub(crate) async fn run<'a>(
    state: &'a SharedState,
    tenant_id: TenantId,
    source_db_id: DatabaseId,
    timeout: Duration,
) -> Result<DrainHolds<'a>, NodeDbError> {
    let timed_out = || {
        NodeDbError::move_tenant_drain_timeout(
            tenant_id.as_u64().to_string(),
            source_db_id.as_u64().to_string(),
        )
    };

    let descriptors = source_descriptors(state, source_db_id)
        .map_err(|e| timed_out().with_cause(crate::error_classify::classify(&e)))?;
    let owner = move_tenant_drain_owner(tenant_id.as_u64(), source_db_id.as_u64());
    let deadline = Instant::now() + timeout;
    let mut holds = DrainHolds::new(state);
    for id in descriptors {
        holds.hold(id.clone(), owner.clone());
        let remaining = deadline.saturating_duration_since(Instant::now());
        if let Err(e) = drain_for_owner_async(state, id, owner.clone(), remaining).await {
            holds.end().await;
            return Err(timed_out().with_cause(crate::error_classify::classify(&e)));
        }
    }
    Ok(holds)
}

/// End the drain on every live collection of the source database.
///
/// Called on every path that fails before the cutover's catalog entry
/// applies, so the source collections are still in the catalog. An error
/// leaves that drain active, and plans on its collection keep failing as
/// retryable until a later `release` or a restart.
pub async fn release(state: &SharedState, tenant_id: TenantId, source_db_id: DatabaseId) {
    let descriptors = match source_descriptors(state, source_db_id) {
        Ok(descriptors) => descriptors,
        Err(error) => {
            tracing::warn!(
                tenant = tenant_id.as_u64(),
                source_db = source_db_id.as_u64(),
                %error,
                "MOVE TENANT drain release: cannot read the source collections"
            );
            return;
        }
    };
    let owner = move_tenant_drain_owner(tenant_id.as_u64(), source_db_id.as_u64());
    for id in descriptors {
        if let Err(error) = end_drain_async(state, id.clone(), owner.clone()).await {
            tracing::warn!(
                tenant = tenant_id.as_u64(),
                descriptor = ?id,
                %error,
                "MOVE TENANT drain release: the drain end did not apply"
            );
        }
    }
}

/// Each live source collection's descriptor, then each source array's.
fn source_descriptors(
    state: &SharedState,
    source_db_id: DatabaseId,
) -> crate::Result<Vec<DescriptorId>> {
    let catalog = state.credentials.catalog();
    let mut descriptors: Vec<DescriptorId> = catalog
        .load_all_collections(source_db_id)?
        .into_iter()
        .filter(|c| c.is_active)
        .map(|c| move_source_descriptor(source_db_id.as_u64(), c.tenant_id, &c.name))
        .collect();
    descriptors.extend(super::arrays::source_descriptors(catalog, source_db_id)?);
    Ok(descriptors)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::control::security::catalog::StoredCollection;

    /// A killed connection drops the statement future, and the holds with it.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn dropping_the_holds_ends_the_move_drain() {
        let cluster = crate::control::cluster::test_one_node::boot().await;
        let state = &cluster.state;
        let source = DatabaseId::new(70);
        let mut orders = StoredCollection::stamped_for_test(5, "orders", "mover");
        orders.database_id = source;
        state
            .credentials
            .catalog()
            .put_collection(source, &orders)
            .expect("seed the source collection");
        let id = move_source_descriptor(source.as_u64(), 5, "orders");

        let holds = run(state, TenantId::new(5), source, Duration::from_secs(5))
            .await
            .expect("an unleased collection drains at once");
        assert!(state.lease_drain.is_draining(&id, 1));

        drop(holds);

        let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
        while state.lease_drain.is_draining(&id, 1) {
            assert!(
                tokio::time::Instant::now() < deadline,
                "a dropped statement left its drain in force"
            );
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        cluster.shutdown().await;
    }
}
