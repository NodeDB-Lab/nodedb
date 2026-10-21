// SPDX-License-Identifier: Apache-2.0

//! `CascadeDefer` FK lifecycle tests: retry-success and TTL-expiration paths.

use loro::LoroValue;
use nodedb_crdt::policy::{PolicyResolution, ResolvedAction};
use nodedb_crdt::state::CrdtState;
use nodedb_crdt::validator::{IncomingDelta, Validator};

use crate::common::{AuthCtx, cascade_defer_policy, post_change, schema, wall_now_ms};

/// FK violation under `CascadeDefer`: the parent row is missing at
/// admission time, so the delta is deferred. Later, a separate agent's
/// delta merges the parent into the leader state. The deferred delta,
/// re-validated by the caller after the queue says it's ready, now
/// accepts.
#[test]
fn cascade_defer_succeeds_when_parent_arrives_before_ttl() {
    let leader = CrdtState::new(0).unwrap();
    let mut validator =
        Validator::new_with_policies(schema(), 100, cascade_defer_policy(60, 3), 100);

    let post = post_change("post-1", "user-a");
    let started_ms = wall_now_ms();

    // Initial submission: FK fails, delta defers.
    let r = validator
        .validate_with_policy(
            &leader,
            &post,
            IncomingDelta {
                peer_id: 42,
                auth: AuthCtx::default(),
                delta_bytes: b"post-delta".to_vec(),
                hlc_timestamp: started_ms,
            },
            &nodedb_crs::CrsContext::builtin_only(),
        )
        .unwrap();
    match r {
        PolicyResolution::Deferred { attempt, .. } => assert_eq!(attempt, 0),
        other => panic!("expected Deferred, got {other:?}"),
    }
    assert_eq!(validator.deferred().len(), 1);

    // Some time later, agent B's delta lands and creates the user.
    leader
        .upsert(
            "users",
            "user-a",
            &[
                ("name", LoroValue::String("Alice".into())),
                ("email", LoroValue::String("alice@example.com".into())),
            ],
        )
        .unwrap();

    // Force the queue to mark the entry ready by polling far in the
    // future — the policy_dispatch dequeues using real wall-clock, so
    // our `hlc_timestamp` arg is not the queue's reference frame.
    let retry_now = wall_now_ms() + 60_000;
    let ready = validator.deferred_mut().poll_ready(retry_now);
    assert_eq!(ready.len(), 1, "exactly one entry must be ready for retry");
    assert_eq!(ready[0].constraint_name, "posts_author_fk");

    // Caller re-validates the original ProposedChange. Parent now
    // exists, so this accepts.
    let r2 = validator
        .validate_with_policy(
            &leader,
            &post,
            IncomingDelta {
                peer_id: 42,
                auth: AuthCtx::default(),
                delta_bytes: b"post-delta".to_vec(),
                hlc_timestamp: retry_now,
            },
            &nodedb_crs::CrsContext::builtin_only(),
        )
        .unwrap();
    assert!(
        matches!(
            r2,
            PolicyResolution::AutoResolved(ResolvedAction::OverwriteExisting)
        ),
        "retry after parent arrival must accept, got {r2:?}"
    );

    // Lifecycle complete: no deferred backlog, no DLQ entries.
    assert!(validator.deferred().is_empty());
    assert!(validator.dlq().is_empty());
}

/// FK violation under `CascadeDefer` where the parent NEVER arrives.
/// After the TTL window passes, the queue's `expire(now)` returns the
/// stale entry; the caller routes it to the DLQ as unrecoverable.
/// This is the negative-side lifecycle: `Deferred → Expired → DLQ`.
#[test]
fn cascade_defer_expires_to_dlq_when_parent_never_arrives() {
    let leader = CrdtState::new(0).unwrap();
    let ttl_secs = 10u64;
    let mut validator =
        Validator::new_with_policies(schema(), 100, cascade_defer_policy(ttl_secs, 3), 100);

    let started_ms = wall_now_ms();

    let r = validator
        .validate_with_policy(
            &leader,
            &post_change("post-orphan", "ghost-user"),
            IncomingDelta {
                peer_id: 42,
                auth: AuthCtx::default(),
                delta_bytes: b"orphan-delta".to_vec(),
                hlc_timestamp: started_ms,
            },
            &nodedb_crs::CrsContext::builtin_only(),
        )
        .unwrap();
    assert!(matches!(r, PolicyResolution::Deferred { .. }));
    assert_eq!(validator.deferred().len(), 1);

    // Before TTL: nothing expires. Use a value just after enqueue —
    // the entry's ttl_deadline_ms = enqueue_wall_clock + ttl_secs*1000,
    // which is well in the future.
    assert!(validator.deferred_mut().expire(started_ms).is_empty());
    assert_eq!(validator.deferred().len(), 1);

    // Far past TTL: entry expires. Caller is responsible for routing
    // expired entries to the DLQ — the queue itself does not auto-DLQ.
    let post_ttl = wall_now_ms() + (ttl_secs * 1000) + 60_000;
    let expired = validator.deferred_mut().expire(post_ttl);
    assert_eq!(expired.len(), 1);
    let entry = &expired[0];
    assert_eq!(entry.constraint_name, "posts_author_fk");
    assert_eq!(entry.peer_id, 42);
    assert_eq!(entry.collection, "posts");
    // The deferred queue has drained the entry.
    assert!(validator.deferred().is_empty());
}
