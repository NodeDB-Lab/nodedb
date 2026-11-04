// SPDX-License-Identifier: BUSL-1.1

//! The one storage-quota decision every write gate calls.
//!
//! A write that can add bytes is refused once the cluster total of its
//! database or tenant reaches the matching `max_storage_bytes`. A write that
//! frees space or adds none always passes, so an over-limit database can
//! shrink.
//!
//! Gates: the write funnel for a write with no replicated form, the leader
//! proposal gate for every replicated entry, and the Calvin submit for a
//! cross-shard transaction.
//!
//! Enforcement is soft. Before the first complete total the gate admits,
//! because a total missing a peer's report understates usage. After that, the
//! total lags the stored bytes by one sample period plus the peer timeout.
//! A burst can overshoot the limit by the writes of that window.

use crate::control::metrics::ClusterStorageTotals;
use crate::control::state::SharedState;
use crate::event::EventSource;
use crate::types::{DatabaseId, TenantId};

/// Refuse a write of `source` that adds bytes to a database or tenant at its
/// storage limit. `adds_storage` runs only once a limit is reached, so a
/// write under its limits never classifies its plan.
pub(crate) fn check_storage_quota(
    state: &SharedState,
    database_id: DatabaseId,
    tenant_id: TenantId,
    source: EventSource,
    adds_storage: impl FnOnce() -> crate::Result<bool>,
) -> crate::Result<()> {
    let totals = &state.cluster_storage_totals;
    if !totals.has_total() || !source_is_gated(source) {
        return Ok(());
    }
    let database_limit = state.database_metrics.storage_limit(database_id);
    let tenant_limit = state
        .tenants
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
        .quota(tenant_id)
        .max_storage_bytes;
    decide(
        totals,
        &Limits {
            database_id,
            database_limit,
            tenant_id,
            tenant_limit,
        },
        source,
        adds_storage,
    )
}

/// The database and tenant a write counts against, with their limits.
/// A limit of zero is unlimited.
struct Limits {
    database_id: DatabaseId,
    database_limit: u64,
    tenant_id: TenantId,
    tenant_limit: u64,
}

/// Whether writes of `source` pass the storage gate.
///
/// A follower apply replays an entry its leader already admitted. A restore
/// reinstates a backup the operator chose, so the limit does not refuse it.
fn source_is_gated(source: EventSource) -> bool {
    match source {
        EventSource::User
        | EventSource::Trigger
        | EventSource::CrdtSync
        | EventSource::Deferred
        | EventSource::ImplicitClient => true,
        EventSource::RaftFollower | EventSource::Restore => false,
    }
}

/// Admit or refuse one write against the cluster totals. Before the first
/// complete total every write passes.
fn decide(
    totals: &ClusterStorageTotals,
    limits: &Limits,
    source: EventSource,
    adds_storage: impl FnOnce() -> crate::Result<bool>,
) -> crate::Result<()> {
    if !totals.has_total() || !source_is_gated(source) {
        return Ok(());
    }
    let database_bytes = totals.database_bytes(limits.database_id);
    let tenant_bytes = totals.tenant_bytes(limits.tenant_id);
    let refusal = if limits.database_limit > 0 && database_bytes >= limits.database_limit {
        crate::Error::StorageQuotaExceeded {
            scope: format!("database {}", limits.database_id.as_u64()),
            used: database_bytes,
            limit: limits.database_limit,
        }
    } else if limits.tenant_limit > 0 && tenant_bytes >= limits.tenant_limit {
        crate::Error::StorageQuotaExceeded {
            scope: format!("tenant {}", limits.tenant_id.as_u64()),
            used: tenant_bytes,
            limit: limits.tenant_limit,
        }
    } else {
        return Ok(());
    };
    if adds_storage()? {
        Err(refusal)
    } else {
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::bridge::envelope::PhysicalPlan;
    use crate::control::server::shared::write_admission::plan_adds_storage;
    use nodedb_physical::physical_plan::{KvOp, KvResolvedMutation, MetaOp};
    use nodedb_types::{QualifiedCollection, Surrogate};

    fn db() -> DatabaseId {
        DatabaseId::new(1)
    }

    fn tenant() -> TenantId {
        TenantId::new(7)
    }

    fn totals_with(bytes: u64, complete: bool) -> ClusterStorageTotals {
        let totals = ClusterStorageTotals::default();
        totals.publish([((db(), tenant()), bytes)].into_iter().collect(), complete);
        totals
    }

    fn limits(database_limit: u64, tenant_limit: u64) -> Limits {
        Limits {
            database_id: db(),
            database_limit,
            tenant_id: tenant(),
            tenant_limit,
        }
    }

    fn collection() -> QualifiedCollection {
        QualifiedCollection::new(DatabaseId::DEFAULT, "c")
    }

    fn insert() -> PhysicalPlan {
        PhysicalPlan::Kv(KvOp::Put {
            collection: collection(),
            key: b"k".to_vec(),
            value: b"v".to_vec(),
            ttl_ms: 0,
            surrogate: Surrogate::new(1),
            returning: None,
            rls_filters: Vec::new(),
            provenance: None,
        })
    }

    fn delete() -> PhysicalPlan {
        PhysicalPlan::Kv(KvOp::Delete {
            collection: collection(),
            keys: vec![b"k".to_vec()],
            rls_write_check: nodedb_types::RlsWriteCheck::NoPolicyApplies,
            returning: None,
            rls_filters: Vec::new(),
            provenance: None,
        })
    }

    fn truncate() -> PhysicalPlan {
        PhysicalPlan::Kv(KvOp::Truncate {
            collection: collection(),
            restart_identity: false,
        })
    }

    fn update() -> PhysicalPlan {
        PhysicalPlan::Kv(KvOp::ResolvedWrite {
            mutations: vec![KvResolvedMutation::Rewrite {
                collection: collection(),
                key: b"k".to_vec(),
                value: b"v2".to_vec(),
                ttl_ms: 0,
                expire_at_ms: 0,
                precondition: b"v".to_vec(),
            }],
            response_payload: Vec::new(),
            rls_write_check: nodedb_types::RlsWriteCheck::NoPolicyApplies,
        })
    }

    fn admit(bytes: u64, limits: &Limits, plan: &PhysicalPlan) -> crate::Result<()> {
        decide(&totals_with(bytes, true), limits, EventSource::User, || {
            Ok(plan_adds_storage(plan))
        })
    }

    fn refusal(result: crate::Result<()>) -> (String, u64, u64) {
        match result {
            Err(crate::Error::StorageQuotaExceeded { scope, used, limit }) => (scope, used, limit),
            other => panic!("expected StorageQuotaExceeded, got {other:?}"),
        }
    }

    #[test]
    fn under_the_limit_admits() {
        assert!(admit(99, &limits(100, 0), &insert()).is_ok());
    }

    #[test]
    fn at_the_database_limit_refuses_naming_the_database() {
        let (scope, used, limit) = refusal(admit(100, &limits(100, 0), &insert()));
        assert_eq!(scope, "database 1");
        assert_eq!((used, limit), (100, 100));
    }

    #[test]
    fn over_the_limit_refuses_with_disk_full_and_a_next_action() {
        let err = admit(250, &limits(100, 0), &insert()).unwrap_err();
        let message = err.to_string();
        assert!(message.contains("250"), "{message}");
        assert!(message.contains("100"), "{message}");
        assert!(message.contains("raise max_storage_bytes"), "{message}");
        assert_eq!(
            crate::control::server::pgwire::types::error_to_sqlstate(&err).1,
            nodedb_types::error::sqlstate::DISK_FULL
        );
    }

    #[test]
    fn no_complete_total_admits_even_over_the_limit() {
        let totals = totals_with(1_000, false);
        let result = decide(&totals, &limits(100, 100), EventSource::User, || {
            Ok(plan_adds_storage(&insert()))
        });
        assert!(result.is_ok());
    }

    #[test]
    fn a_write_that_frees_space_admits_over_the_limit() {
        let limits = limits(100, 100);
        assert!(admit(500, &limits, &delete()).is_ok());
        assert!(admit(500, &limits, &truncate()).is_ok());
        assert!(
            admit(
                500,
                &limits,
                &PhysicalPlan::Meta(MetaOp::PurgeTenant { tenant_id: 7 })
            )
            .is_ok()
        );
    }

    #[test]
    fn a_non_write_plan_admits_over_the_limit() {
        let compact = PhysicalPlan::Meta(MetaOp::Compact);
        assert!(admit(500, &limits(100, 100), &compact).is_ok());
    }

    #[test]
    fn an_update_refuses_over_the_limit() {
        assert!(admit(500, &limits(100, 0), &update()).is_err());
    }

    #[test]
    fn the_tenant_limit_is_independent_of_the_database_limit() {
        let (scope, used, limit) = refusal(admit(500, &limits(0, 400), &insert()));
        assert_eq!(scope, "tenant 7");
        assert_eq!((used, limit), (500, 400));
        assert!(admit(500, &limits(1_000, 600), &insert()).is_ok());
    }

    #[test]
    fn a_zero_limit_is_unlimited() {
        assert!(admit(u64::MAX, &limits(0, 0), &insert()).is_ok());
    }

    #[test]
    fn another_tenants_bytes_do_not_count_against_this_tenant() {
        let other = Limits {
            tenant_id: TenantId::new(8),
            ..limits(0, 400)
        };
        assert!(admit(500, &other, &insert()).is_ok());
    }

    #[test]
    fn a_restore_is_not_gated() {
        let totals = totals_with(500, true);
        let result = decide(&totals, &limits(100, 0), EventSource::Restore, || {
            Ok(plan_adds_storage(&insert()))
        });
        assert!(result.is_ok());
    }

    #[test]
    fn a_write_under_its_limits_never_classifies() {
        let totals = totals_with(10, true);
        let result = decide(&totals, &limits(100, 100), EventSource::User, || {
            panic!("classified a write under its limits")
        });
        assert!(result.is_ok());
    }

    #[test]
    fn a_classification_error_surfaces_over_the_limit() {
        let totals = totals_with(500, true);
        let result = decide(&totals, &limits(100, 0), EventSource::User, || {
            Err(crate::Error::Internal {
                detail: "decode".into(),
            })
        });
        assert!(matches!(result, Err(crate::Error::Internal { .. })));
    }

    #[test]
    fn a_follower_apply_is_not_gated() {
        let totals = totals_with(500, true);
        let result = decide(&totals, &limits(100, 0), EventSource::RaftFollower, || {
            Ok(plan_adds_storage(&insert()))
        });
        assert!(result.is_ok());
    }
}
