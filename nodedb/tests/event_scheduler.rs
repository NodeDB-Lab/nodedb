//! Integration tests for Event Plane cron scheduler.
//!
//! Tests: cron expression matching, per-collection affinity detection,
//! missed execution policy, overlap enforcement, job history.

use nodedb::event::scheduler::cron::CronExpr;
use nodedb::event::scheduler::history::JobHistoryStore;
use nodedb::event::scheduler::types::{JobRun, MissedPolicy, ScheduleDef, ScheduleScope};

#[test]
fn cron_every_minute() {
    let expr = CronExpr::parse("* * * * *").unwrap();
    // Every minute boundary should match.
    assert!(expr.matches_epoch(0)); // 1970-01-01 00:00
    assert!(expr.matches_epoch(60)); // 1970-01-01 00:01
    assert!(expr.matches_epoch(3600)); // 1970-01-01 01:00
}

#[test]
fn cron_specific_minute() {
    let expr = CronExpr::parse("30 * * * *").unwrap();
    // 00:30 UTC → 30*60 = 1800s from epoch.
    assert!(expr.matches_epoch(1800));
    // 00:00 should NOT match.
    assert!(!expr.matches_epoch(0));
}

#[test]
fn cron_specific_hour_and_minute() {
    let expr = CronExpr::parse("0 12 * * *").unwrap();
    // 12:00 UTC → 12*3600 = 43200s from epoch.
    assert!(expr.matches_epoch(43200));
    // 13:00 should NOT match.
    assert!(!expr.matches_epoch(46800));
}

#[test]
fn cron_range() {
    let expr = CronExpr::parse("0 9-17 * * *").unwrap();
    // 09:00 through 17:00 should match.
    assert!(expr.matches_epoch(9 * 3600));
    assert!(expr.matches_epoch(12 * 3600));
    assert!(expr.matches_epoch(17 * 3600));
    // 08:00 should NOT match.
    assert!(!expr.matches_epoch(8 * 3600));
}

#[test]
fn cron_step() {
    let expr = CronExpr::parse("*/15 * * * *").unwrap();
    // Minutes 0, 15, 30, 45.
    assert!(expr.matches_epoch(0));
    assert!(expr.matches_epoch(15 * 60));
    assert!(expr.matches_epoch(30 * 60));
    assert!(!expr.matches_epoch(10 * 60));
}

#[test]
fn cron_invalid_rejected() {
    assert!(CronExpr::parse("").is_err());
    assert!(CronExpr::parse("* * *").is_err()); // Only 3 fields.
    assert!(CronExpr::parse("60 * * * *").is_err()); // Minute > 59.
}

#[test]
fn schedule_def_target_collection() {
    let def = ScheduleDef {
        tenant_id: 1,
        name: "cleanup".into(),
        cron_expr: "0 0 * * *".into(),
        body_sql: "DELETE FROM metrics WHERE ts < now() - INTERVAL '90 days'".into(),
        scope: ScheduleScope::Normal,
        missed_policy: MissedPolicy::Skip,
        allow_overlap: true,
        enabled: true,
        target_collection: Some("metrics".into()),
        owner: "admin".into(),
        created_at: 0,
    };
    assert_eq!(def.target_collection.as_deref(), Some("metrics"));
    assert_eq!(def.scope, ScheduleScope::Normal);
}

#[test]
fn schedule_local_scope() {
    let def = ScheduleDef {
        tenant_id: 1,
        name: "local_job".into(),
        cron_expr: "* * * * *".into(),
        body_sql: "SELECT 1".into(),
        scope: ScheduleScope::Local,
        missed_policy: MissedPolicy::Skip,
        allow_overlap: false,
        enabled: true,
        target_collection: None,
        owner: "admin".into(),
        created_at: 0,
    };
    assert_eq!(def.scope, ScheduleScope::Local);
    assert!(!def.allow_overlap);
}

#[test]
fn missed_policy_variants() {
    assert_eq!(MissedPolicy::Skip.as_str(), "SKIP");
    assert_eq!(MissedPolicy::CatchUp.as_str(), "CATCH_UP");
    assert_eq!(MissedPolicy::Queue.as_str(), "QUEUE");
    assert_eq!(MissedPolicy::from_str_opt("SKIP"), Some(MissedPolicy::Skip));
    assert_eq!(
        MissedPolicy::from_str_opt("CATCH_UP"),
        Some(MissedPolicy::CatchUp)
    );
    assert!(MissedPolicy::from_str_opt("INVALID").is_none());
}

#[test]
fn job_history_record_and_query() {
    let dir = tempfile::tempdir().unwrap();
    let store = JobHistoryStore::open(dir.path()).unwrap();

    store
        .record(JobRun {
            schedule_name: "cleanup".into(),
            tenant_id: 1,
            started_at: 1000,
            duration_ms: 50,
            success: true,
            error: None,
        })
        .unwrap();

    store
        .record(JobRun {
            schedule_name: "cleanup".into(),
            tenant_id: 1,
            started_at: 2000,
            duration_ms: 30,
            success: false,
            error: Some("timeout".into()),
        })
        .unwrap();

    let runs = store.last_runs(1, "cleanup", 10);
    assert_eq!(runs.len(), 2);
    // last_runs returns most recent first.
    assert!(!runs[0].success); // Most recent = the failing run.
    assert!(runs[1].success); // Older = the successful run.
}

#[test]
fn job_history_persists_across_reopen() {
    let dir = tempfile::tempdir().unwrap();
    {
        let store = JobHistoryStore::open(dir.path()).unwrap();
        store
            .record(JobRun {
                schedule_name: "s1".into(),
                tenant_id: 1,
                started_at: 1000,
                duration_ms: 10,
                success: true,
                error: None,
            })
            .unwrap();
    }
    let store = JobHistoryStore::open(dir.path()).unwrap();
    let runs = store.last_runs(1, "s1", 10);
    assert_eq!(runs.len(), 1);
}
