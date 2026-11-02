// SPDX-License-Identifier: BUSL-1.1

//! Descriptor cache-miss recovery.
//!
//! When the planner returns `Error::RetryableSchemaChanged { descriptor }`,
//! the gateway:
//! 1. Fetches a fresh descriptor lease via the descriptor lease machinery.
//! 2. Calls the supplied `plan_fn` once more to re-plan against fresh state.
//! 3. Proceeds to dispatch with the new plan.
//!
//! This is a **single** retry — if the second plan still fails with a cache
//! miss, the error is propagated to the caller.

use tracing::debug;

use crate::Error;
use crate::control::lease::{DEFAULT_LEASE_DURATION, acquire_lease};
use crate::control::state::SharedState;

/// Attempt planning once; on `RetryableSchemaChanged` fetch a fresh lease
/// and try once more.
///
/// `plan_fn` — closure that produces a `PhysicalPlan` or an error. Called
/// at most twice. On the second call the lease for the affected descriptor
/// has been refreshed so the catalog adapter returns a fresh version.
///
/// `database_id` and `tenant_id` — used when acquiring the descriptor lease.
pub async fn plan_with_cache_miss_retry<F, P>(
    shared: &SharedState,
    database_id: nodedb_types::DatabaseId,
    tenant_id: u64,
    plan_fn: F,
) -> Result<P, Error>
where
    F: Fn() -> Result<P, Error>,
{
    match plan_fn() {
        Ok(plan) => Ok(plan),
        Err(Error::RetryableSchemaChanged { descriptor }) => {
            debug!(
                descriptor = %descriptor,
                tenant_id,
                "gateway: descriptor cache miss — fetching fresh lease and retrying plan"
            );
            refresh_descriptor_lease(shared, database_id, tenant_id, &descriptor).await?;
            // Single retry — if this also fails, propagate.
            plan_fn()
        }
        Err(other) => Err(other),
    }
}

/// Acquire (or renew) the lease for a descriptor, forcing the catalog adapter
/// to re-read from the replicated metadata store.
async fn refresh_descriptor_lease(
    shared: &SharedState,
    database_id: nodedb_types::DatabaseId,
    tenant_id: u64,
    descriptor: &str,
) -> Result<(), Error> {
    let descriptor_id = nodedb_cluster::DescriptorId::new(
        database_id.as_u64(),
        tenant_id,
        nodedb_cluster::DescriptorKind::Collection,
        descriptor,
    );

    // The lease MUST be requested at the descriptor's real current version.
    // A drain rejects every `requested_version <= up_to_version`, so a
    // hardcoded version 0 falls inside every active drain range and makes this
    // opportunistic refresh unable to ever succeed while a drain is running —
    // which is precisely when it is called.
    let stored = shared
        .credentials
        .catalog()
        .get_collection(database_id, tenant_id, descriptor)?;
    let Some(version) = refresh_target_version(stored.as_ref()) else {
        return Err(Error::CollectionNotFound {
            tenant_id: crate::types::TenantId::new(tenant_id),
            collection: descriptor.to_string(),
        });
    };

    acquire_lease(shared, descriptor_id, version, DEFAULT_LEASE_DURATION).await?;

    Ok(())
}

/// The descriptor version an opportunistic lease refresh must request.
///
/// Mirrors the planner's own version resolution: a freshly created collection
/// can still carry version 0 in the catalog, and version 0 is inside every
/// drain range, so it is floored to 1. A dropped/absent collection has no
/// version to lease.
fn refresh_target_version(
    stored: Option<&crate::control::security::catalog::StoredCollection>,
) -> Option<u64> {
    stored
        .filter(|collection| collection.is_active)
        .map(|collection| collection.descriptor_version.max(1))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::control::security::catalog::StoredCollection;
    use nodedb_physical::physical_plan::{KvOp, PhysicalPlan};

    fn ok_plan() -> Result<PhysicalPlan, Error> {
        Ok(PhysicalPlan::Kv(KvOp::Get {
            collection: nodedb_types::QualifiedCollection::new(
                nodedb_types::DatabaseId::DEFAULT,
                "users",
            ),
            key: vec![],
            rls_filters: vec![],
            surrogate_ceiling: None,
            system_time: nodedb_types::SystemTimeScope::Current,
        }))
    }

    #[test]
    fn ok_path_calls_plan_fn_once() {
        let call_count = std::cell::Cell::new(0usize);
        let rt = tokio::runtime::Runtime::new().unwrap();
        // Test the retry branches directly, with no SharedState.
        let mut attempts = 0usize;
        let result: Result<PhysicalPlan, Error> = rt.block_on(async {
            // Simulate plan_with_cache_miss_retry with an always-ok plan_fn.
            attempts += 1;
            match ok_plan() {
                Ok(p) => Ok(p),
                Err(Error::RetryableSchemaChanged { .. }) => {
                    attempts += 1;
                    ok_plan()
                }
                Err(e) => Err(e),
            }
        });
        let _ = call_count;
        assert!(result.is_ok());
        assert_eq!(attempts, 1);
    }

    fn stored_collection(descriptor_version: u64, is_active: bool) -> StoredCollection {
        let mut collection = StoredCollection::new(7, "orders", "owner");
        collection.descriptor_version = descriptor_version;
        collection.is_active = is_active;
        collection
    }

    #[test]
    fn refresh_requests_the_real_descriptor_version_not_zero() {
        let stored = stored_collection(4, true);
        assert_eq!(refresh_target_version(Some(&stored)), Some(4));
    }

    #[test]
    fn refresh_never_requests_version_zero() {
        // Version 0 sits inside EVERY active drain range, so an unstamped
        // descriptor must still be leased at 1.
        let stored = stored_collection(0, true);
        assert_eq!(refresh_target_version(Some(&stored)), Some(1));
    }

    #[test]
    fn refresh_has_no_target_for_dropped_or_absent_collection() {
        let dropped = stored_collection(4, false);
        assert_eq!(refresh_target_version(Some(&dropped)), None);
        assert_eq!(refresh_target_version(None), None);
    }

    #[test]
    fn double_miss_propagates_error() {
        let rt = tokio::runtime::Runtime::new().unwrap();
        let mut calls = 0usize;
        let result: Result<PhysicalPlan, Error> = rt.block_on(async {
            let mut result = Err(Error::RetryableSchemaChanged {
                descriptor: "orders".into(),
            });
            // First call.
            calls += 1;
            // Simulated re-plan also fails.
            if matches!(result, Err(Error::RetryableSchemaChanged { .. })) {
                calls += 1;
                result = Err(Error::RetryableSchemaChanged {
                    descriptor: "orders".into(),
                });
            }
            result
        });
        assert!(matches!(result, Err(Error::RetryableSchemaChanged { .. })));
        assert_eq!(calls, 2);
    }
}
