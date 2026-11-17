// SPDX-License-Identifier: BUSL-1.1

//! A replicated write predicate carries every session input already bound.
//!
//! The leader compiles a write policy against the writing identity, and the
//! record carries the compiled filters. A replica applier holds no identity
//! and no clock input: it decides the row from the carried filters alone.

use std::collections::HashMap;

use nodedb_physical::physical_plan::DocumentOp;
use nodedb_types::{QualifiedCollection, RlsWriteCheck, Surrogate, Value};

use super::rls_write_gate::{GatedRow, admit_stored_row};
use crate::bridge::envelope::PhysicalPlan;
use crate::bridge::scan_filter::{FilterOp, ScanFilter};
use crate::control::security::auth_context::AuthContext;
use crate::control::security::identity::{AuthMethod, AuthenticatedIdentity, DatabaseSet, Role};
use crate::control::security::predicate::{CompareOp, PolicyMode, PredicateValue, RlsPredicate};
use crate::control::security::rls::{PolicyType, RlsPolicy, RlsPolicyStore};
use crate::control::wal_replication::{
    ReplicableWrite, decode_replicated_entry, to_replicated_entry,
};
use crate::engine::document::store::RowIdentity;
use crate::types::{DatabaseId, TenantId, VShardId};

const LEADER_AUTH_TIME: u64 = 1_700_000_000;

/// `owner_id = $auth.id AND issued_after <= $auth.auth_time`, on `orders`.
fn store_with_identity_and_time_policy() -> RlsPolicyStore {
    let store = RlsPolicyStore::new();
    store
        .create_policy(RlsPolicy {
            name: "owner_and_session_time".into(),
            collection: "orders".into(),
            display_collection: "orders".into(),
            tenant_id: 1,
            policy_type: PolicyType::Write,
            compiled_predicate: Some(RlsPredicate::And(vec![
                RlsPredicate::Compare {
                    field: "owner_id".into(),
                    op: CompareOp::Eq,
                    value: PredicateValue::AuthRef("id".into()),
                },
                RlsPredicate::Compare {
                    field: "issued_after".into(),
                    op: CompareOp::Lte,
                    value: PredicateValue::AuthRef("auth_time".into()),
                },
            ])),
            mode: PolicyMode::default(),
            on_deny: Default::default(),
            enabled: true,
            created_by: "admin".into(),
            created_at: 0,
        })
        .expect("create policy");
    store
}

/// The leader's session: user 42, authenticated at `LEADER_AUTH_TIME`.
fn leader_session() -> AuthContext {
    let identity = AuthenticatedIdentity::new_regular(
        42,
        "alice",
        TenantId::new(1),
        AuthMethod::ScramSha256,
        vec![Role::ReadWrite],
        None,
        DatabaseSet::Some(smallvec::smallvec![DatabaseId::DEFAULT]),
    );
    let mut auth = AuthContext::from_identity(&identity, "s_leader".into());
    auth.auth_time = Some(LEADER_AUTH_TIME);
    auth
}

/// Every compiled filter is a literal comparison: no expression, no
/// unresolved session reference.
fn assert_bound(filters: &[ScanFilter], ids: &mut Vec<Value>) {
    for filter in filters {
        assert!(
            filter.expr.is_none(),
            "a compiled write filter carries no expression"
        );
        assert_ne!(filter.op, FilterOp::Expr);
        if let Value::String(text) = &filter.value {
            assert!(
                !text.starts_with("$auth"),
                "unbound session reference {text}"
            );
        }
        if filter.field == "owner_id" {
            ids.push(filter.value.clone());
        }
        for group in &filter.clauses {
            assert_bound(group, ids);
        }
    }
}

fn row(owner: &str, issued_after: i64) -> Vec<u8> {
    zerompk::to_msgpack_vec(&Value::Object(HashMap::from([
        ("id".to_string(), Value::String("a".into())),
        ("owner_id".to_string(), Value::String(owner.into())),
        ("issued_after".to_string(), Value::Integer(issued_after)),
    ])))
    .expect("encode row")
}

/// The replica gate, with no identity and no session in scope.
fn replica_decides(check: &RlsWriteCheck, body: &[u8]) -> bool {
    admit_stored_row(
        check,
        GatedRow {
            body,
            identity: &RowIdentity::from_user_key("a"),
            strict_schema: None,
            identity_column: "id",
        },
        1,
        "orders",
        &nodedb_crs::CrsContext::builtin_only(),
    )
    .is_ok()
}

/// A predicate on `$auth.id` and `$auth.auth_time` binds both at the leader.
/// The replicated record carries the bound filters, and the replica applier
/// decides each row from them alone.
#[test]
fn a_replica_decides_from_the_values_the_leader_bound() {
    let compiled = store_with_identity_and_time_policy()
        .combined_write_predicate_with_auth(1, "orders", &leader_session())
        .expect("compile")
        .expect("every session reference resolves");
    let filters: Vec<ScanFilter> = zerompk::from_msgpack(&compiled).expect("decode filters");
    let mut ids = Vec::new();
    assert_bound(&filters, &mut ids);
    assert_eq!(ids, vec![Value::String("42".into())]);

    let plan = PhysicalPlan::Document(DocumentOp::PointInsert {
        collection: QualifiedCollection::new(DatabaseId::DEFAULT, "orders"),
        document_id: "a".into(),
        value: row("42", 1),
        surrogate: Surrogate::new(1),
        if_absent: false,
        returning: None,
        rls_filters: Vec::new(),
        rls_write_check: RlsWriteCheck::from_injected(compiled.clone()),
        resolved_sum_targets: Vec::new(),
        deferred_sum_targets: Vec::new(),
    });
    let write = ReplicableWrite::decide_for_replication(&plan).expect("record carries policy");
    let entry = to_replicated_entry(
        TenantId::new(1),
        DatabaseId::DEFAULT,
        VShardId::new(0),
        &write,
    )
    .expect("encode")
    .expect("replicated document write");
    let (_, _, decoded, _) = decode_replicated_entry(&entry.to_bytes())
        .expect("decode")
        .expect("document plan");
    let carried = decoded
        .sole_rls_write_check()
        .expect("carried check")
        .clone();
    assert_eq!(carried, RlsWriteCheck::Predicate(compiled));

    assert!(replica_decides(&carried, &row("42", 1_650_000_000)));
    assert!(replica_decides(
        &carried,
        &row("42", LEADER_AUTH_TIME as i64)
    ));
    assert!(!replica_decides(&carried, &row("42", 1_750_000_000)));
    assert!(!replica_decides(&carried, &row("99", 1_650_000_000)));
}
