// SPDX-License-Identifier: Apache-2.0

//! FK chain across agents: child arrives before parent, defer-and-retry resolves.

use loro::LoroValue;
use nodedb_crdt::policy::{
    CollectionPolicy, ConflictPolicy, PolicyRegistry, PolicyResolution, ResolvedAction,
};
use nodedb_crdt::state::CrdtState;
use nodedb_crdt::validator::{IncomingDelta, Validator};

use crate::common::{AuthCtx, post_change, schema, user_change, wall_now_ms};

/// Two offline agents working on linked tables: agent A creates a
/// post referencing user `user-a`; agent B (separately offline)
/// creates the user. Their deltas arrive at the leader in the
/// "wrong" order — child first, parent second. The defer-and-retry
/// machinery must paper over the temporal inversion.
#[test]
fn fk_chain_child_arrives_before_parent_then_resolves() {
    let leader = CrdtState::new(0).unwrap();
    let mut policies = PolicyRegistry::new();
    let mut posts_policy = CollectionPolicy::ephemeral();
    posts_policy.foreign_key = ConflictPolicy::CascadeDefer {
        max_retries: 3,
        ttl_secs: 60,
    };
    policies.set("posts", posts_policy);
    // `users` keeps default (ephemeral) — UNIQUE rename, FK n/a here.
    let mut validator = Validator::new_with_policies(schema(), 100, policies, 100);

    let started_ms = wall_now_ms();
    let post = post_change("post-from-a", "user-a");

    // Agent A's child delta arrives first.
    let r_post = validator
        .validate_with_policy(
            &leader,
            &post,
            IncomingDelta {
                peer_id: 1,
                auth: AuthCtx::default(),
                delta_bytes: b"post-delta".to_vec(),
                hlc_timestamp: started_ms,
            },
            &nodedb_crs::CrsContext::builtin_only(),
        )
        .unwrap();
    assert!(matches!(r_post, PolicyResolution::Deferred { .. }));

    // Agent B's parent delta arrives second.
    let user = user_change("user-a", "Alice", "alice@example.com");
    let r_user = validator
        .validate_with_policy(
            &leader,
            &user,
            IncomingDelta {
                peer_id: 2,
                auth: AuthCtx::default(),
                delta_bytes: b"user-delta".to_vec(),
                hlc_timestamp: started_ms + 50,
            },
            &nodedb_crs::CrsContext::builtin_only(),
        )
        .unwrap();
    assert!(
        matches!(
            r_user,
            PolicyResolution::AutoResolved(ResolvedAction::OverwriteExisting)
        ),
        "parent insert has no conflicts, got {r_user:?}"
    );
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

    // Now the deferred-queue retry: the post becomes ready, and the
    // parent it depends on is committed. Revalidation accepts.
    let retry_now = wall_now_ms() + 60_000;
    let ready = validator.deferred_mut().poll_ready(retry_now);
    assert_eq!(ready.len(), 1);
    assert_eq!(ready[0].collection, "posts");

    let r_retry = validator
        .validate_with_policy(
            &leader,
            &post,
            IncomingDelta {
                peer_id: 1,
                auth: AuthCtx::default(),
                delta_bytes: b"post-delta".to_vec(),
                hlc_timestamp: retry_now,
            },
            &nodedb_crs::CrsContext::builtin_only(),
        )
        .unwrap();
    assert!(
        matches!(
            r_retry,
            PolicyResolution::AutoResolved(ResolvedAction::OverwriteExisting)
        ),
        "child retry after parent commit must accept, got {r_retry:?}"
    );

    // Both queues empty, no DLQ — clean lifecycle.
    assert!(validator.deferred().is_empty());
    assert!(validator.dlq().is_empty());
}
