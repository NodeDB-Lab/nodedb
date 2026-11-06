// SPDX-License-Identifier: BUSL-1.1

//! Scheduler executor: Tokio task that evaluates cron expressions every second.
//!
//! For each due schedule, dispatches the SQL body through the Control Plane
//! query path using a system identity (SECURITY DEFINER).
//!
//! **Leader-aware (cluster mode):** Before firing, the scheduler checks
//! if this node is the Raft leader for the schedule's target vShard.
//! Only the leader fires — follower nodes skip. When a vShard migrates
//! or leadership changes, the new leader automatically picks up the schedule.
//!
//! **Lease-aware:** If this node is leader but the Raft group is lagging
//! (commit_index > last_applied), the scheduler skips firing to prevent
//! stale execution during a network partition.

use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use tokio::sync::watch;
use tracing::{debug, info, trace, warn};

use crate::control::state::SharedState;

use super::backup_job::BackupJobs;
use super::coordinator::is_system_coordinator;
use super::cron::CronExpr;
use super::dispatcher::{DispatchOutcome, JobDispatcher, JobDispatcherConfig};
use super::history::JobHistoryStore;
use super::job_run::execute_job;
use super::registry::ScheduleRegistry;
use super::types::{JobRun, ScheduleDef, ScheduleScope};

/// Compute the set of cron-matching minute numbers that still need to
/// fire for one schedule given its last-fired minute and the current
/// observation.
///
/// The caller is the per-second tick loop. The loop cannot assume its
/// observed `now_secs` lands on a minute boundary — Tokio scheduling
/// latency, GC pauses, and leader handoffs can all push the observation
/// a second or more past `xx:00`. Gating on `now_secs % 60 == 0` drops
/// the entire minute on any such jitter; in the worst case a daily
/// schedule misses its single matching minute and doesn't fire for 24
/// hours.
///
/// Contract:
/// - `last_fired_minute = None` (never fired, or loop restart): consider
///   only the current minute. This bounds catch-up — a freshly started
///   node does not replay ticks from epoch.
/// - `last_fired_minute = Some(L)`: return every minute in
///   `(L ..= now_secs / 60]` that the cron expression matches.
/// - Within-minute re-observations (e.g. second 61 when minute 1 has
///   already fired) return empty.
pub fn pending_minute_ticks(
    last_fired_minute: Option<u64>,
    now_secs: u64,
    cron: &CronExpr,
    utc_offset_seconds: i32,
) -> Vec<u64> {
    let now_min = now_secs / 60;
    let start = match last_fired_minute {
        Some(last) => last.saturating_add(1),
        None => now_min,
    };
    (start..=now_min)
        .filter(|m| cron.matches_epoch_with_offset(m.saturating_mul(60), utc_offset_seconds))
        .collect()
}

/// Spawn the scheduler loop as a background Tokio task.
pub fn spawn_scheduler(
    state: Arc<SharedState>,
    registry: Arc<ScheduleRegistry>,
    history: Arc<JobHistoryStore>,
    shutdown: watch::Receiver<bool>,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        scheduler_loop(state, registry, history, shutdown).await;
    })
}

/// The main scheduler loop. Runs every second.
async fn scheduler_loop(
    state: Arc<SharedState>,
    registry: Arc<ScheduleRegistry>,
    history: Arc<JobHistoryStore>,
    mut shutdown: watch::Receiver<bool>,
) {
    info!("scheduler started");

    // Track currently running jobs (for ALLOW_OVERLAP = false enforcement).
    // Shared with spawned job tasks so they remove themselves on completion.
    let running: Arc<std::sync::Mutex<HashSet<(u64, u64, String)>>> =
        Arc::new(std::sync::Mutex::new(HashSet::new()));

    // Bounded job dispatcher — caps concurrency (from SchedulerTuning)
    // and owns the shutdown bus every job observes. The result-byte
    // ceiling is advisory for callers that have a planner estimate;
    // the scheduler enforces memory by routing each job through the
    // statement executor's `ExecutionBudget` wall-clock timeout below.
    let sched_tuning = &state.tuning.scheduler;
    let dispatcher = Arc::new(JobDispatcher::new(JobDispatcherConfig {
        max_concurrent_jobs: sched_tuning.max_concurrent_jobs,
        max_result_bytes: u64::MAX,
    }));
    let job_timeout_secs = sched_tuning.job_timeout_secs;

    // Per-schedule last-fired minute. Tracked in memory only: on loop
    // restart every schedule's last_fired is None, and
    // `pending_minute_ticks` returns at most the current minute. That
    // bounded catch-up is what prevents a cold start from replaying
    // ticks from epoch.
    let mut last_fired_minute: HashMap<(u64, u64, String), u64> = HashMap::new();

    // `[[backup.schedule]]` jobs share the dispatcher and the history.
    let backup_jobs = BackupJobs::new(&state.backup_schedules);

    loop {
        tokio::select! {
            _ = tokio::time::sleep(Duration::from_secs(1)) => {}
            _ = shutdown.changed() => {
                if *shutdown.borrow() {
                    debug!("scheduler shutting down");
                    dispatcher.shutdown_and_drain().await;
                    return;
                }
            }
        }

        if *shutdown.borrow() {
            dispatcher.shutdown_and_drain().await;
            return;
        }

        let now_secs = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();

        backup_jobs.fire(&state, &dispatcher, &history, now_secs);

        // Get all enabled schedules.
        let schedules = registry.list_all_enabled();
        if schedules.is_empty() {
            continue;
        }

        for sched in &schedules {
            // A job's writes on a mirror come from the source's run of it. The
            // schedule starts firing at PROMOTE, from that minute on.
            if state
                .mirror
                .fence
                .is_read_only(nodedb_types::id::DatabaseId::new(sched.database_id))
            {
                continue;
            }
            // Parse cron expression.
            let cron = match CronExpr::parse(&sched.cron_expr) {
                Ok(c) => c,
                Err(e) => {
                    warn!(
                        schedule = %sched.name,
                        error = %e,
                        "invalid cron expression, skipping"
                    );
                    continue;
                }
            };

            // Per-schedule minute tracking: fire every matching minute
            // between the last-fired and the current observation. This
            // tolerates tick jitter past a minute boundary; the old code
            // gated on `now_secs % 60 == 0` and silently dropped any
            // minute whose tick landed at second != 0.
            let sched_key = (sched.database_id, sched.tenant_id, sched.name.clone());
            let last = last_fired_minute.get(&sched_key).copied();
            let tz_offset = state.scheduler_config.cron_timezone.offset_seconds();
            let pending = pending_minute_ticks(last, now_secs, &cron, tz_offset);
            if pending.is_empty() {
                // Still advance the marker on first observation so that
                // a schedule whose cron never matches the current minute
                // doesn't fall into unbounded catch-up on the next tick.
                if last.is_none() {
                    last_fired_minute.insert(sched_key.clone(), now_secs / 60);
                }
                continue;
            }
            // Record the highest minute we're about to fire so subsequent
            // ticks within the same minute don't refire it.
            if let Some(&max_min) = pending.iter().max() {
                last_fired_minute.insert(sched_key.clone(), max_min);
            }

            // Leader-aware: skip if this node is not the right one for this schedule.
            if !should_fire_on_this_node(sched, &state) {
                trace!(
                    schedule = %sched.name,
                    "skipping: this node is not the leader for target vShard"
                );
                continue;
            }

            // Lease-aware: skip if the Raft group is lagging (stale leader).
            if !is_raft_group_healthy(sched, &state) {
                debug!(
                    schedule = %sched.name,
                    "skipping: Raft group lagging (lease-aware suspension)"
                );
                continue;
            }

            // Check overlap policy.
            let key = (sched.database_id, sched.tenant_id, sched.name.clone());
            if !sched.allow_overlap {
                let guard = running.lock().unwrap_or_else(|p| p.into_inner());
                if guard.contains(&key) {
                    trace!(
                        schedule = %sched.name,
                        "skipping: previous run still active (ALLOW_OVERLAP = false)"
                    );
                    continue;
                }
            }

            // Mark as running before spawning (prevents race with next scheduler tick).
            {
                let mut guard = running.lock().unwrap_or_else(|p| p.into_inner());
                guard.insert(key.clone());
            }

            debug!(schedule = %sched.name, "firing scheduled job");

            let state_clone = Arc::clone(&state);
            let history_clone = Arc::clone(&history);
            let running_clone = Arc::clone(&running);
            let sched_clone = sched.clone();

            // Dispatch through the bounded dispatcher. Rejection at the
            // concurrency cap surfaces as a recorded failure — the job
            // simply didn't run this tick, rather than starving the
            // Tokio runtime with unbounded parallel SQL.
            let outcome = dispatcher.try_spawn(move |mut shutdown_rx| async move {
                let result = tokio::select! {
                    r = execute_job(&state_clone, &sched_clone, job_timeout_secs) => r,
                    _ = shutdown_rx.changed() => {
                        return Err(crate::Error::Dispatch { detail: "scheduler shutdown".to_string() });
                    }
                };
                let now_ms = SystemTime::now()
                    .duration_since(UNIX_EPOCH)
                    .unwrap_or_default()
                    .as_millis() as u64;

                let run = match result {
                    Ok(duration_ms) => JobRun {
                        database_id: sched_clone.database_id,
                        schedule_name: sched_clone.name.clone(),
                        tenant_id: sched_clone.tenant_id,
                        started_at: now_ms.saturating_sub(duration_ms),
                        duration_ms,
                        success: true,
                        error: None,
                    },
                    Err(e) => {
                        warn!(
                            schedule = %sched_clone.name,
                            error = %e,
                            "scheduled job failed"
                        );
                        JobRun {
                            database_id: sched_clone.database_id,
                            schedule_name: sched_clone.name.clone(),
                            tenant_id: sched_clone.tenant_id,
                            started_at: now_ms,
                            duration_ms: 0,
                            success: false,
                            error: Some(e.to_string()),
                        }
                    }
                };

                if let Err(e) = history_clone.record(run) {
                    warn!(error = %e, "failed to record job history");
                }

                let key = (sched_clone.database_id, sched_clone.tenant_id, sched_clone.name.clone());
                let mut guard = running_clone.lock().unwrap_or_else(|p| p.into_inner());
                guard.remove(&key);
                Ok(())
            });

            if outcome == DispatchOutcome::OverBudget {
                // Release the running-set slot we reserved — the job
                // didn't actually start.
                let mut guard = running.lock().unwrap_or_else(|p| p.into_inner());
                guard.remove(&key);
                warn!(
                    schedule = %sched.name,
                    max_concurrent = sched_tuning.max_concurrent_jobs,
                    "scheduled job rejected: concurrency cap reached"
                );
            }
        }
    }
}

/// Check whether this node should fire the given schedule.
///
/// - `ScheduleScope::Local` → always fire (local node only).
/// - No `cluster_routing` → the cluster is not wired → never fire.
/// - `target_collection` is Some → resolve vShard → check leader.
/// - `target_collection` is None → cross-collection job → only the
///   `_system` coordinator fires: the vShard 0 leader under a valid lease.
fn should_fire_on_this_node(sched: &ScheduleDef, state: &SharedState) -> bool {
    // LOCAL scope: always fire on this node.
    if sched.scope == ScheduleScope::Local {
        return true;
    }

    // A node whose cluster is not wired knows no leader, so it fires nothing.
    let Some(ref routing_lock) = state.cluster_routing else {
        return false;
    };

    let node_id = state.node_id;

    if let Some(ref collection) = sched.target_collection {
        // Collection-targeted schedule: fire only on the shard leader in
        // the database that owns the schedule definition.
        let vshard_id =
            nodedb_cluster::routing::vshard_for_collection(nodedb_types::CollectionKey::from_bare(
                nodedb_types::id::DatabaseId::new(sched.database_id),
                collection,
            ));
        let routing = routing_lock.read().unwrap_or_else(|p| p.into_inner());
        match routing.leader_for_vshard(vshard_id) {
            Ok(leader) => leader == node_id,
            Err(_) => {
                // Can't determine leader — skip to be safe.
                false
            }
        }
    } else {
        is_system_coordinator(state)
    }
}

/// Check if the Raft group for this schedule's target vShard is healthy.
///
/// A group is healthy when `commit_index == last_applied` (fully caught up).
/// If the group is lagging, this node may be a stale leader during a partition.
/// Skipping prevents dual execution when the new leader hasn't taken over yet.
///
/// Returns `false` before `start_raft` installed the status function or when
/// the cluster is not wired.
fn is_raft_group_healthy(sched: &ScheduleDef, state: &SharedState) -> bool {
    // LOCAL scope: no Raft group to check.
    if sched.scope == ScheduleScope::Local {
        return true;
    }

    let Some(status_fn) = state.raft_status_fn.get() else {
        return false;
    };
    let Some(ref routing_lock) = state.cluster_routing else {
        return false;
    };

    // Determine the target vShard's Raft group.
    let vshard_id = sched
        .target_collection
        .as_ref()
        .map(|c| {
            nodedb_cluster::routing::vshard_for_collection(nodedb_types::CollectionKey::from_bare(
                nodedb_types::id::DatabaseId::new(sched.database_id),
                c,
            ))
        })
        .unwrap_or(0); // Cross-collection → coordinator vShard 0.

    let group_id = {
        let routing = routing_lock.read().unwrap_or_else(|p| p.into_inner());
        match routing.group_for_vshard(vshard_id) {
            Ok(gid) => gid,
            Err(_) => return true, // Can't determine group — fire anyway.
        }
    };

    // Check the group's status.
    let statuses = status_fn();
    let Some(status) = statuses.iter().find(|s| s.group_id == group_id) else {
        return true; // Group not found locally — might be on another node, fire anyway.
    };

    // Stale leader check: if commit_index is ahead of last_applied,
    // this node has uncommitted entries — it may be partitioned.
    if status.commit_index > status.last_applied {
        let lag = status.commit_index - status.last_applied;
        debug!(
            group_id,
            commit_index = status.commit_index,
            last_applied = status.last_applied,
            lag,
            "Raft group lagging — suspending schedule fire"
        );
        return false;
    }

    // Also check that we're actually the leader for this group.
    if status.leader_id != state.node_id {
        return false;
    }

    true
}

#[cfg(test)]
mod tests {
    use super::super::types::MissedPolicy;
    use super::*;

    fn make_schedule(name: &str, target: Option<&str>, scope: ScheduleScope) -> ScheduleDef {
        ScheduleDef {
            database_id: 0,
            tenant_id: 1,
            name: name.into(),
            cron_expr: "* * * * *".into(),
            body_sql: "SELECT 1".into(),
            scope,
            missed_policy: MissedPolicy::Skip,
            allow_overlap: true,
            enabled: true,
            target_collection: target.map(|s| s.to_string()),
            owner: "admin".into(),
            created_at: 0,
        }
    }

    #[tokio::test]
    async fn local_scope_always_fires() {
        let sched = make_schedule("local_job", Some("orders"), ScheduleScope::Local);
        // Even if we had cluster routing, LOCAL always fires.
        let dir = tempfile::tempdir().unwrap();
        let (_, _, state, _, _) = crate::event::test_utils::event_test_deps(&dir);
        assert!(should_fire_on_this_node(&sched, &state));
    }

    #[tokio::test]
    async fn an_unwired_node_fires_nothing() {
        let sched = make_schedule("normal_job", Some("orders"), ScheduleScope::Normal);
        let dir = tempfile::tempdir().unwrap();
        let (_, _, state, _, _) = crate::event::test_utils::event_test_deps(&dir);
        assert!(state.cluster_routing.is_none());
        assert!(!should_fire_on_this_node(&sched, &state));
        assert!(!is_raft_group_healthy(&sched, &state));
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_one_node_cluster_fires_its_collection_schedules() {
        let sched = make_schedule("normal_job", Some("orders"), ScheduleScope::Normal);
        let cluster = crate::control::cluster::test_one_node::boot().await;
        assert!(should_fire_on_this_node(&sched, &cluster.state));
        cluster.shutdown().await;
    }

    #[tokio::test]
    async fn local_scope_healthy_without_raft() {
        let sched = make_schedule("local_job", None, ScheduleScope::Local);
        let dir = tempfile::tempdir().unwrap();
        let (_, _, state, _, _) = crate::event::test_utils::event_test_deps(&dir);
        assert!(is_raft_group_healthy(&sched, &state));
    }
}
