// SPDX-License-Identifier: Apache-2.0

//! Multi-agent UNIQUE auto-rename: 3 agents offline simultaneously all claim
//! the same email; under `RenameSuffix` the suffix counter advances
//! monotonically (`_1`, `_2`) and conflict-free deltas accept directly.

use loro::LoroValue;
use nodedb_crdt::policy::{PolicyResolution, ResolvedAction};
use nodedb_crdt::state::CrdtState;
use nodedb_crdt::validator::{IncomingDelta, Validator};

use crate::common::{AuthCtx, rename_suffix_policy, schema, user_change};

/// Three offline agents all claim `alice@example.com` simultaneously.
/// Under `RenameSuffix`, only the first delta to reach the leader
/// accepts as-is; subsequent deltas resolve to `RenamedField` with a
/// monotonic suffix. The suffix counter must advance per (collection,
/// field), not per delta or per agent.
#[test]
fn multi_agent_unique_rename_advances_suffix_monotonically() {
    let leader = CrdtState::new(0).unwrap();
    let mut validator = Validator::new_with_policies(schema(), 100, rename_suffix_policy(), 100);

    let now_ms = 1_000_000u64;

    // Agent A's delta arrives first — no conflict yet, accepted.
    let r_a = validator
        .validate_with_policy(
            &leader,
            &user_change("user-a", "Alice", "alice@example.com"),
            IncomingDelta {
                peer_id: 1,
                auth: AuthCtx::default(),
                delta_bytes: b"delta-a".to_vec(),
                hlc_timestamp: now_ms,
            },
            &nodedb_crs::CrsContext::builtin_only(),
        )
        .unwrap();
    assert!(
        matches!(
            r_a,
            PolicyResolution::AutoResolved(ResolvedAction::OverwriteExisting)
        ),
        "first agent's delta has no conflict yet, got {r_a:?}"
    );

    // Caller commits agent A's row to the leader state.
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

    // Agent B arrives next — conflicts with A. Suffix counter → 1.
    let r_b = validator
        .validate_with_policy(
            &leader,
            &user_change("user-b", "Bob", "alice@example.com"),
            IncomingDelta {
                peer_id: 2,
                auth: AuthCtx::default(),
                delta_bytes: b"delta-b".to_vec(),
                hlc_timestamp: now_ms + 100,
            },
            &nodedb_crs::CrsContext::builtin_only(),
        )
        .unwrap();
    let suffix_b = match r_b {
        PolicyResolution::AutoResolved(ResolvedAction::RenamedField {
            field,
            ref new_value,
        }) => {
            assert_eq!(field, "email");
            new_value.clone()
        }
        other => panic!("expected RenamedField for agent B, got {other:?}"),
    };
    assert!(
        suffix_b.ends_with("_1"),
        "first conflict must rename to suffix _1, got {suffix_b:?}"
    );

    // Caller commits B with the renamed value.
    leader
        .upsert(
            "users",
            "user-b",
            &[
                ("name", LoroValue::String("Bob".into())),
                (
                    "email",
                    LoroValue::String("alice+collision1@example.com".into()),
                ),
            ],
        )
        .unwrap();

    // Agent C — still conflicts on the original `alice@example.com`.
    // The suffix counter must advance to 2 even though B's stored
    // value used a different concrete email.
    let r_c = validator
        .validate_with_policy(
            &leader,
            &user_change("user-c", "Carol", "alice@example.com"),
            IncomingDelta {
                peer_id: 3,
                auth: AuthCtx::default(),
                delta_bytes: b"delta-c".to_vec(),
                hlc_timestamp: now_ms + 200,
            },
            &nodedb_crs::CrsContext::builtin_only(),
        )
        .unwrap();
    let suffix_c = match r_c {
        PolicyResolution::AutoResolved(ResolvedAction::RenamedField { ref new_value, .. }) => {
            new_value.clone()
        }
        other => panic!("expected RenamedField for agent C, got {other:?}"),
    };
    assert!(
        suffix_c.ends_with("_2"),
        "second conflict must rename to suffix _2 (monotonic), got {suffix_c:?}"
    );

    // No DLQ entries — every conflict was auto-resolved.
    assert!(
        validator.dlq().is_empty(),
        "rename-suffix policy must not enqueue to DLQ"
    );
}
