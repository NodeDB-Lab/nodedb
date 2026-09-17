// SPDX-License-Identifier: BUSL-1.1

//! RLS predicate evaluation and `$auth.*` substitution.
//!
//! **Read path**: `combined_read_predicate_with_auth` merges all
//! enabled read policies into a single `ScanFilter` list injected
//! into `DocumentScan`.
//!
//! **Write path**: `combined_write_predicate_with_auth` compiles the same
//! policies into filter bytes for the planner's write gate, which evaluates
//! them against the row image a statement is about to persist.
//! `check_write_with_auth` is the ready-made-document form of the same check,
//! used where the post-image is already materialized (CRDT admission).
//! Both deny with `Error::RejectedAuthz`.

use tracing::info;

use crate::control::security::audit::{AuditEmitContext, AuditEmitter, AuditEvent};
use crate::control::security::auth_context::AuthContext;
use crate::control::security::predicate::{PolicyMode, RlsPredicate};
use crate::control::security::predicate_eval::{combine_policies, substitute_to_scan_filters};

use super::store::RlsPolicyStore;
use super::types::RlsPolicy;

impl RlsPolicyStore {
    /// Primary read-path RLS method: substitutes `$auth.*` variables
    /// and returns the combined `ScanFilter` bytes.
    ///
    /// `Ok(Some(bytes))` is the filter set, empty when no read policy
    /// restricts this identity here. `Ok(None)` means deny: a required
    /// `$auth` field is missing. `Err` means the filter set could not be
    /// encoded; the caller must refuse the statement, never treat it as
    /// unrestricted.
    pub fn combined_read_predicate_with_auth(
        &self,
        tenant_id: u64,
        collection: &str,
        auth: &AuthContext,
    ) -> crate::Result<Option<Vec<u8>>> {
        if auth.is_superuser() {
            return Ok(Some(Vec::new()));
        }
        compile_policy_bytes(&self.read_policies(tenant_id, collection), auth)
    }

    /// Compile the collection's write policies into the `ScanFilter` bytes a
    /// write gate evaluates against a row image.
    ///
    /// The write-path twin of [`Self::combined_read_predicate_with_auth`] and
    /// resolved identically — superuser bypass, vacuous policies, the
    /// fail-closed `None` on an unresolvable `$auth.*` reference, and the
    /// `Err` on an unencodable filter set all behave the same — so a
    /// `FOR ALL` policy cannot compile to one predicate for reads and a
    /// different one for writes.
    pub fn combined_write_predicate_with_auth(
        &self,
        tenant_id: u64,
        collection: &str,
        auth: &AuthContext,
    ) -> crate::Result<Option<Vec<u8>>> {
        if auth.is_superuser() {
            return Ok(Some(Vec::new()));
        }
        compile_policy_bytes(&self.write_policies(tenant_id, collection), auth)
    }

    /// Whether any enabled, non-vacuous write policy exists in this tenant.
    ///
    /// For a write plan that names no collection, so the narrow per-collection
    /// question cannot be asked and the write cannot be shown to avoid a
    /// protected collection. A policy with no compiled predicate restricts
    /// nothing, so it is ignored here exactly as the compilers above ignore it.
    pub fn tenant_has_write_policy(&self, tenant_id: u64) -> bool {
        self.all_policies_for_tenant(tenant_id)
            .iter()
            .any(|policy| {
                policy.enabled
                    && policy.compiled_predicate.is_some()
                    && matches!(
                        policy.policy_type,
                        super::types::PolicyType::Write | super::types::PolicyType::All
                    )
            })
    }

    /// Write-path RLS check with `$auth.*` support. Evaluates compiled
    /// write policies; fail-closed on unresolved auth references.
    ///
    /// Emits `AuditEvent::RlsRejected` via `emitter` on every denial.
    pub fn check_write_with_auth(
        &self,
        tenant_id: u64,
        collection: &str,
        document: &serde_json::Value,
        auth: &AuthContext,
        emitter: &dyn AuditEmitter,
    ) -> crate::Result<()> {
        if auth.is_superuser() {
            return Ok(());
        }

        let policies = self.write_policies(tenant_id, collection);
        if policies.is_empty() {
            return Ok(());
        }

        for policy in &policies {
            if let Some(ref compiled) = policy.compiled_predicate {
                check_compiled_write(
                    policy, compiled, document, auth, tenant_id, collection, emitter,
                )?;
            }
        }

        Ok(())
    }
}

/// Combine `policies` for `auth` and encode the result.
///
/// Empty bytes when no policy carries a predicate or the combination is
/// vacuous. `None` when an `$auth.*` reference cannot be resolved. `Err`
/// when the filter set does not encode: an unencodable policy is an error
/// the caller surfaces, never an empty (admit-all) filter set.
fn compile_policy_bytes(
    policies: &[RlsPolicy],
    auth: &AuthContext,
) -> crate::Result<Option<Vec<u8>>> {
    let compiled_policies: Vec<(RlsPredicate, PolicyMode)> = policies
        .iter()
        .filter_map(|p| p.compiled_predicate.as_ref().map(|c| (c.clone(), p.mode)))
        .collect();
    if compiled_policies.is_empty() {
        return Ok(Some(Vec::new()));
    }
    let Some(all_filters) = combine_policies(&compiled_policies, auth) else {
        return Ok(None);
    };
    if all_filters.is_empty() {
        return Ok(Some(Vec::new()));
    }
    encode_scan_filters(&all_filters).map(Some)
}

/// Encode a filter set for the bridge.
fn encode_scan_filters(
    filters: &[crate::bridge::scan_filter::ScanFilter],
) -> crate::Result<Vec<u8>> {
    zerompk::to_msgpack_vec(&filters).map_err(|e| crate::Error::Serialization {
        format: "msgpack".into(),
        detail: format!("RLS filter serialization failed: {e}"),
    })
}

/// Decide one row image against ALREADY-COMPILED write-policy filter bytes.
///
/// For Control-Plane paths that hold a post-image but not the policy store: the
/// planner's plan-time write admission, and the COMMIT-time expanders that
/// rewrite a staged MERGE / `UPDATE ... FROM` into concrete point ops. Those
/// expansions run after the injection pass, so the statement's compiled
/// predicate is the only thing left that can gate their rows — without this the
/// rewrite would launder a governed write into an ungoverned one.
///
/// `image` is the MessagePack row body. Empty `compiled` means no write policy
/// restricts this identity here. Fails closed on an undecodable payload or an
/// evaluation error, so an adversarial predicate cannot become an admitted
/// write by erroring out of the check; the evaluation error is named in the
/// denial.
pub fn admit_compiled_write_image(
    compiled: &[u8],
    image: &[u8],
    tenant_id: u64,
    collection: &str,
) -> crate::Result<()> {
    if compiled.is_empty() {
        return Ok(());
    }
    let filters: Vec<crate::bridge::scan_filter::ScanFilter> = zerompk::from_msgpack(compiled)
        .map_err(|error| crate::Error::PlanError {
            detail: format!("RLS write filter deserialization failed: {error}"),
        })?;
    let deny = |detail: String| crate::Error::RejectedAuthz {
        tenant_id: crate::types::TenantId::new(tenant_id),
        resource: format!("RLS write policy on '{collection}' {detail}"),
    };
    match crate::bridge::scan_filter::ScanFilter::all_match_binary(&filters, image) {
        Ok(true) => Ok(()),
        Ok(false) => Err(deny("rejected the row".into())),
        Err(error) => Err(deny(format!("could not be evaluated: {error}"))),
    }
}

fn check_compiled_write(
    policy: &RlsPolicy,
    compiled: &RlsPredicate,
    document: &serde_json::Value,
    auth: &AuthContext,
    tenant_id: u64,
    collection: &str,
    emitter: &dyn AuditEmitter,
) -> crate::Result<()> {
    let filters = match substitute_to_scan_filters(compiled, auth) {
        Some(f) => f,
        None => {
            info!(
                policy = %policy.name,
                username = %auth.username,
                %collection,
                "RLS write policy: unresolved $auth reference → denied"
            );
            emitter.emit(
                AuditEvent::RlsRejected,
                &auth.username,
                &format!(
                    "RLS policy '{}' on '{}': unresolved session variable",
                    policy.name, collection
                ),
                AuditEmitContext::new(
                    Some(crate::types::TenantId::new(tenant_id)),
                    &auth.id,
                    &auth.username,
                ),
            );
            return Err(crate::Error::RejectedAuthz {
                tenant_id: crate::types::TenantId::new(tenant_id),
                resource: format!(
                    "RLS policy '{}' on '{}': unresolved session variable",
                    policy.name, collection
                ),
            });
        }
    };

    let doc_mp = nodedb_types::json_to_msgpack_or_empty(document);
    if !crate::bridge::scan_filter::ScanFilter::all_match_binary(&filters, &doc_mp)? {
        info!(
            policy = %policy.name,
            username = %auth.username,
            %collection,
            "RLS write policy rejected (compiled)"
        );
        emitter.emit(
            AuditEvent::RlsRejected,
            &auth.username,
            &format!(
                "RLS policy '{}' on collection '{}' rejected write",
                policy.name, collection
            ),
            AuditEmitContext::new(
                Some(crate::types::TenantId::new(tenant_id)),
                &auth.id,
                &auth.username,
            ),
        );
        return Err(crate::Error::RejectedAuthz {
            tenant_id: crate::types::TenantId::new(tenant_id),
            resource: format!(
                "RLS policy '{}' on collection '{}'",
                policy.name, collection
            ),
        });
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::control::security::audit::NoopAuditEmitter;
    use crate::control::security::auth_context::AuthContext;
    use crate::control::security::predicate::{CompareOp, PredicateValue, RlsPredicate};
    use crate::control::security::rls::types::PolicyType;

    const NOOP: &NoopAuditEmitter = &NoopAuditEmitter;

    fn make_policy(name: &str, collection: &str, policy_type: PolicyType) -> RlsPolicy {
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();
        RlsPolicy {
            name: name.into(),
            collection: collection.into(),
            display_collection: collection.into(),
            tenant_id: 1,
            policy_type,
            compiled_predicate: None,
            mode: PolicyMode::default(),
            on_deny: Default::default(),
            enabled: true,
            created_by: "admin".into(),
            created_at: now,
        }
    }

    fn nonsuper_auth() -> AuthContext {
        use crate::control::security::identity::{AuthMethod, AuthenticatedIdentity, Role};
        use crate::types::TenantId;
        let identity = AuthenticatedIdentity::new_regular(
            42,
            "alice",
            TenantId::new(1),
            AuthMethod::ScramSha256,
            vec![Role::ReadWrite],
            None,
            crate::control::security::identity::DatabaseSet::Some(smallvec::smallvec![
                nodedb_types::id::DatabaseId::DEFAULT,
            ]),
        );
        AuthContext::from_identity(&identity, "s_test".into())
    }

    /// `$auth.id` reference: resolves to user_id string "42" which must
    /// equal doc["owner_id"] = "42" → policy passes.
    #[test]
    fn write_policy_enforcement_compiled() {
        let store = RlsPolicyStore::new();
        let predicate = RlsPredicate::Compare {
            field: "owner_id".into(),
            op: CompareOp::Eq,
            value: PredicateValue::AuthRef("id".into()),
        };
        let mut policy = make_policy("require_owner", "orders", PolicyType::Write);
        policy.compiled_predicate = Some(predicate);
        store.create_policy(policy).unwrap();

        let auth = nonsuper_auth();
        // alice's user_id is 42; owner_id matches → allowed.
        let doc_ok = serde_json::json!({"owner_id": "42", "amount": 100});
        assert!(
            store
                .check_write_with_auth(1, "orders", &doc_ok, &auth, NOOP)
                .is_ok()
        );

        // Different owner → denied.
        let doc_bad = serde_json::json!({"owner_id": "99", "amount": 100});
        assert!(
            store
                .check_write_with_auth(1, "orders", &doc_bad, &auth, NOOP)
                .is_err()
        );
    }

    #[test]
    fn no_policies_allows_everything() {
        let store = RlsPolicyStore::new();
        let auth = nonsuper_auth();
        let doc = serde_json::json!({"anything": "goes"});
        assert!(
            store
                .check_write_with_auth(1, "whatever", &doc, &auth, NOOP)
                .is_ok()
        );
    }

    /// A policy with an unresolvable `$auth.*` reference must deny
    /// the write (fail-closed).
    #[test]
    fn check_write_denies_on_unresolvable_auth_ref() {
        let store = RlsPolicyStore::new();
        let predicate = RlsPredicate::Compare {
            field: "org_id".into(),
            op: CompareOp::Eq,
            value: PredicateValue::AuthRef("nonexistent_field".into()),
        };
        let mut policy = make_policy("require_org", "orders", PolicyType::Write);
        policy.compiled_predicate = Some(predicate);
        store.create_policy(policy).unwrap();

        let auth = nonsuper_auth();
        let doc = serde_json::json!({"org_id": "some_org"});
        let result = store.check_write_with_auth(1, "orders", &doc, &auth, NOOP);
        assert!(
            result.is_err(),
            "RLS must fail-closed on unresolvable $auth ref; got {result:?}"
        );
    }

    /// `check_write_with_auth` must also fail-closed when an `$auth` reference
    /// cannot be resolved.
    #[test]
    fn check_write_with_auth_denies_on_unresolvable_ref() {
        let store = RlsPolicyStore::new();
        let predicate = RlsPredicate::Compare {
            field: "secret_scope".into(),
            op: CompareOp::Eq,
            value: PredicateValue::AuthRef("nonexistent_field".into()),
        };
        let mut policy = make_policy("require_scope", "orders", PolicyType::Write);
        policy.compiled_predicate = Some(predicate);
        store.create_policy(policy).unwrap();

        let doc = serde_json::json!({"secret_scope": "admin"});
        let auth = nonsuper_auth();
        let result = store.check_write_with_auth(1, "orders", &doc, &auth, NOOP);
        assert!(
            result.is_err(),
            "write path must fail-closed on unresolvable auth ref; got {result:?}"
        );
    }

    #[test]
    fn rls_rejection_emits_audit_row() {
        use crate::control::security::audit::emitter::test_helpers::CapturingEmitter;

        let store = RlsPolicyStore::new();
        let predicate = RlsPredicate::Compare {
            field: "owner_id".into(),
            op: CompareOp::Eq,
            value: PredicateValue::AuthRef("id".into()),
        };
        let mut policy = make_policy("owner_gate", "items", PolicyType::Write);
        policy.compiled_predicate = Some(predicate);
        store.create_policy(policy).unwrap();

        let auth = nonsuper_auth();
        // alice's user_id is 42; owner_id is 99 → denied.
        let doc_bad = serde_json::json!({"owner_id": "99", "qty": 1});
        let emitter = CapturingEmitter::new();
        let result = store.check_write_with_auth(1, "items", &doc_bad, &auth, &emitter);
        assert!(result.is_err());

        let recorded = emitter.recorded();
        assert_eq!(recorded.len(), 1);
        assert_eq!(recorded[0].0, AuditEvent::RlsRejected);
    }

    #[test]
    fn rls_allow_does_not_emit_audit_row() {
        use crate::control::security::audit::emitter::test_helpers::CapturingEmitter;

        let store = RlsPolicyStore::new();
        let predicate = RlsPredicate::Compare {
            field: "owner_id".into(),
            op: CompareOp::Eq,
            value: PredicateValue::AuthRef("id".into()),
        };
        let mut policy = make_policy("owner_gate", "items", PolicyType::Write);
        policy.compiled_predicate = Some(predicate);
        store.create_policy(policy).unwrap();

        let auth = nonsuper_auth();
        let doc_ok = serde_json::json!({"owner_id": "42", "qty": 1});
        let emitter = CapturingEmitter::new();
        let result = store.check_write_with_auth(1, "items", &doc_ok, &auth, &emitter);
        assert!(result.is_ok());
        assert!(emitter.recorded().is_empty());
    }

    /// Read-path: `combined_read_predicate_with_auth` with a policy whose
    /// compiled predicate requires a literal filter. A policy that has no
    /// `compiled_predicate` is vacuous (passes all rows). With a predicate
    /// present the combined result must be non-empty (filter applied).
    #[test]
    fn combined_read_with_auth_applies_compiled_predicate() {
        let store = RlsPolicyStore::new();
        let predicate = RlsPredicate::Compare {
            field: "user_id".into(),
            op: CompareOp::Eq,
            value: PredicateValue::AuthRef("id".into()),
        };
        let mut policy = make_policy("user_scope", "orders", PolicyType::Read);
        policy.compiled_predicate = Some(predicate);
        store.create_policy(policy).unwrap();

        let auth = nonsuper_auth();
        let result = store
            .combined_read_predicate_with_auth(1, "orders", &auth)
            .expect("filters encode");
        // Must return Some with non-empty filter bytes (not unrestricted).
        assert!(
            result.is_some_and(|b| !b.is_empty()),
            "read path must produce filter bytes when compiled predicate is set"
        );
    }

    /// Read-path: a policy with an unresolvable `$auth.*` reference must
    /// deny the read (fail-closed: returns None).
    #[test]
    fn combined_read_with_auth_fails_closed_on_unresolvable_ref() {
        let store = RlsPolicyStore::new();
        let predicate = RlsPredicate::Compare {
            field: "org_id".into(),
            op: CompareOp::Eq,
            value: PredicateValue::AuthRef("nonexistent_field".into()),
        };
        let mut policy = make_policy("org_scope", "orders", PolicyType::Read);
        policy.compiled_predicate = Some(predicate);
        store.create_policy(policy).unwrap();

        let auth = nonsuper_auth();
        let result = store
            .combined_read_predicate_with_auth(1, "orders", &auth)
            .expect("nothing to encode");
        assert!(
            result.is_none(),
            "read path must fail-closed on unresolvable auth ref; got {result:?}"
        );
    }

    /// An unencodable filter set is an `Err`, never an empty (admit-all)
    /// filter set. `encode_scan_filters` is the one seam every policy byte
    /// set passes through, so the `Result` plumbing is pinned on it directly
    /// with a filter set of the largest size and shape the encoder accepts.
    #[test]
    fn filter_encoding_result_is_propagated_not_defaulted() {
        let filters = vec![crate::bridge::scan_filter::ScanFilter {
            field: "owner".into(),
            op: crate::bridge::scan_filter::FilterOp::Eq,
            value: nodedb_types::Value::String("42".into()),
            clauses: Vec::new(),
            expr: None,
        }];
        let bytes = encode_scan_filters(&filters).expect("a plain filter encodes");
        assert!(!bytes.is_empty());
        let decoded: Vec<crate::bridge::scan_filter::ScanFilter> =
            zerompk::from_msgpack(&bytes).expect("round-trips");
        assert_eq!(decoded.len(), 1);
        assert_eq!(decoded[0].field, "owner");
    }

    /// A write gate that cannot evaluate its filters denies, naming the
    /// evaluation error, instead of admitting the row.
    #[test]
    fn admit_compiled_write_image_denies_when_evaluation_errors() {
        use nodedb_query::{BinaryOp, SqlExpr};
        // `1 / 0` is the one expression the evaluator refuses to decide.
        let filters = vec![crate::bridge::scan_filter::ScanFilter {
            field: String::new(),
            op: crate::bridge::scan_filter::FilterOp::Expr,
            value: nodedb_types::Value::Null,
            clauses: Vec::new(),
            expr: Some(SqlExpr::BinaryOp {
                left: Box::new(SqlExpr::Literal(nodedb_types::Value::Integer(1))),
                op: BinaryOp::Div,
                right: Box::new(SqlExpr::Literal(nodedb_types::Value::Integer(0))),
            }),
        }];
        let compiled = encode_scan_filters(&filters).expect("encodes");
        let image = nodedb_types::json_to_msgpack_or_empty(&serde_json::json!({"qty": 1}));
        let result = admit_compiled_write_image(&compiled, &image, 1, "items");
        match result {
            Err(crate::Error::RejectedAuthz { resource, .. }) => {
                assert!(
                    resource.contains("could not be evaluated"),
                    "denial must name the evaluation error: {resource}"
                );
            }
            other => panic!("expected RejectedAuthz, got {other:?}"),
        }
    }
}
