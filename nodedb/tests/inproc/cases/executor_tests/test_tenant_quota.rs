// SPDX-License-Identifier: BUSL-1.1

//! Tenant quota enforcement tests.
//!
//! Verifies that TenantIsolation correctly enforces quotas and that
//! request_start/request_end lifecycle tracking works.

use nodedb::control::security::tenant::{QuotaCheck, TenantIsolation, TenantQuota};
use nodedb::types::TenantId;

fn t(id: u64) -> TenantId {
    TenantId::new(id)
}

#[test]
fn qps_enforcement() {
    let mut iso = TenantIsolation::new(TenantQuota {
        max_qps: 5,
        ..Default::default()
    });

    // Fire 5 requests — all should be allowed.
    for _ in 0..5 {
        assert!(iso.check(t(1)).is_allowed());
        iso.request_start(t(1));
        iso.request_end(t(1));
    }

    // 6th request in the same second window — rate limited.
    assert_eq!(
        iso.check(t(1)),
        QuotaCheck::RateLimited { qps: 5, limit: 5 }
    );

    // After rate counter reset, requests are allowed again.
    iso.reset_rate_counters();
    assert!(iso.check(t(1)).is_allowed());
}

#[test]
fn concurrency_enforcement() {
    let mut iso = TenantIsolation::new(TenantQuota {
        max_concurrent_requests: 3,
        ..Default::default()
    });

    // Open 3 concurrent requests — all allowed.
    for _ in 0..3 {
        assert!(iso.check(t(1)).is_allowed());
        iso.request_start(t(1));
    }

    // 4th concurrent request — blocked.
    assert_eq!(
        iso.check(t(1)),
        QuotaCheck::ConcurrencyExceeded {
            active: 3,
            limit: 3,
        }
    );

    // Complete one request — now it's allowed again.
    iso.request_end(t(1));
    assert!(iso.check(t(1)).is_allowed());
}

#[test]
fn memory_enforcement() {
    let mut iso = TenantIsolation::new(TenantQuota {
        max_memory_bytes: 1024,
        ..Default::default()
    });

    // Under limit — allowed.
    iso.update_memory(t(1), 512);
    assert!(iso.check(t(1)).is_allowed());

    // Over limit — blocked.
    iso.update_memory(t(1), 2048);
    assert!(matches!(
        iso.check(t(1)),
        QuotaCheck::MemoryExceeded {
            used: 2048,
            limit: 1024
        }
    ));

    // Back under — allowed.
    iso.update_memory(t(1), 500);
    assert!(iso.check(t(1)).is_allowed());
}

/// Storage is enforced at write admission, against the cluster total. The
/// tenant check stays open, so reads and deletes reach an over-limit tenant.
#[test]
fn storage_over_limit_leaves_the_tenant_check_open() {
    let mut iso = TenantIsolation::new(TenantQuota {
        max_storage_bytes: 5000,
        ..Default::default()
    });

    iso.update_storage(t(1), 6000);
    assert!(iso.check(t(1)).is_allowed());
}

#[test]
fn connection_enforcement() {
    let mut iso = TenantIsolation::new(TenantQuota {
        max_connections: 2,
        ..Default::default()
    });

    iso.connection_start(t(1));
    assert!(iso.check_connection(t(1)).is_allowed());

    iso.connection_start(t(1));
    assert_eq!(
        iso.check_connection(t(1)),
        QuotaCheck::ConcurrencyExceeded {
            active: 2,
            limit: 2,
        }
    );

    iso.connection_end(t(1));
    assert!(iso.check_connection(t(1)).is_allowed());
}

#[test]
fn unlimited_connections_allowed() {
    let iso = TenantIsolation::new(TenantQuota {
        max_connections: 0, // Unlimited.
        ..Default::default()
    });
    assert!(iso.check_connection(t(1)).is_allowed());
}

#[test]
fn request_lifecycle_tracking() {
    let mut iso = TenantIsolation::new(TenantQuota::default());

    iso.request_start(t(1));
    iso.request_start(t(1));
    let usage = iso.usage(t(1)).unwrap();
    assert_eq!(usage.active_requests, 2);
    assert_eq!(usage.total_requests, 2);

    iso.request_end(t(1));
    let usage = iso.usage(t(1)).unwrap();
    assert_eq!(usage.active_requests, 1);
    assert_eq!(usage.total_requests, 2); // Total doesn't decrease.

    iso.request_rejected(t(1));
    let usage = iso.usage(t(1)).unwrap();
    assert_eq!(usage.rejected_requests, 1);
}

#[test]
fn multi_tenant_quota_isolation() {
    let mut iso = TenantIsolation::new(TenantQuota {
        max_concurrent_requests: 1,
        ..Default::default()
    });

    // Tenant 1 at concurrency limit.
    iso.request_start(t(1));
    assert!(!iso.check(t(1)).is_allowed());

    // Tenant 2 is completely unaffected.
    assert!(iso.check(t(2)).is_allowed());
    iso.request_start(t(2));
    assert!(!iso.check(t(2)).is_allowed());

    // Tenants are independent.
    iso.request_end(t(1));
    assert!(iso.check(t(1)).is_allowed());
    assert!(!iso.check(t(2)).is_allowed()); // Tenant 2 still at limit.
}

#[test]
fn snapshot_metrics_correctness() {
    let mut iso = TenantIsolation::new(TenantQuota {
        max_memory_bytes: 1024,
        max_storage_bytes: 2048,
        max_qps: 100,
        max_connections: 10,
        ..Default::default()
    });

    iso.update_memory(t(1), 512);
    iso.update_storage(t(1), 1000);
    iso.request_start(t(1));
    iso.connection_start(t(1));

    let metrics = iso.snapshot_metrics(t(1));
    assert_eq!(metrics.tenant_id, 1);
    assert_eq!(metrics.memory_bytes_used, 512);
    assert_eq!(metrics.memory_bytes_limit, 1024);
    assert_eq!(metrics.storage_bytes_used, 1000);
    assert_eq!(metrics.storage_bytes_limit, 2048);
    assert_eq!(metrics.qps_limit, 100);
    assert_eq!(metrics.connections_active, 1);
    assert_eq!(metrics.connections_limit, 10);
}
