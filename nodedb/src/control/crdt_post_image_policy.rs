// SPDX-License-Identifier: BUSL-1.1

//! Exact external CRDT post-merge RLS policy.
//!
//! The Data Plane preview is the sole source of a CRDT write's post-image.
//! External transports evaluate RLS only against that canonical representation;
//! raw client delta bytes are never interpreted as a document for authorization.

use crate::control::crdt_admission::CrdtPostImagePolicy;
use crate::control::security::audit::{AuditEmitContext, AuditEmitter, AuditEvent};
use crate::control::security::auth_context::AuthContext;
use crate::control::security::catalog::SystemCatalog;
use crate::control::security::rls::RlsPolicyStore;
use crate::control::state::SharedState;
use crate::types::TenantId;
use nodedb_types::{CrdtPreviewResult, Value};

/// RLS policy for a single authenticated external CRDT admission.
pub struct ExternalCrdtPostImagePolicy<'a> {
    tenant_id: TenantId,
    collection: &'a str,
    auth: AuthContext,
    services: PolicyServices<'a>,
    /// Where the collection's typeguards and transition rules are read from.
    /// `None` judges no rule.
    guards: Option<GuardSource<'a>>,
}

/// The shared services a CRDT post-image policy evaluates against: the RLS
/// store, the audit sink, and the user-defined CRSs write predicates resolve
/// SRIDs against.
#[derive(Clone, Copy)]
pub struct PolicyServices<'a> {
    pub rls: &'a RlsPolicyStore,
    pub audit: &'a dyn AuditEmitter,
    pub crs_set: &'a nodedb_crs::CustomCrsSet,
}

/// The catalog row a CRDT admission reads the collection's typeguards and
/// transition rules from.
struct GuardSource<'a> {
    catalog: &'a SystemCatalog,
    database_id: nodedb_types::DatabaseId,
    /// Resolves the CRS context CHECK and transition predicates evaluate
    /// under.
    state: &'a SharedState,
}

impl<'a> ExternalCrdtPostImagePolicy<'a> {
    fn with_auth(
        tenant_id: TenantId,
        collection: &'a str,
        auth: AuthContext,
        services: PolicyServices<'a>,
    ) -> Self {
        Self {
            tenant_id,
            collection,
            auth,
            services,
            guards: None,
        }
    }

    /// Judge the merge against the collection's typeguards and transition
    /// rules too.
    ///
    /// A CRDT collection declares no guard `DEFAULT` / `VALUE`: every replica
    /// merges the state on its own, so the server injects nothing. REQUIRED,
    /// type and CHECK validate the post-image the merge produces. State
    /// constraints and transition checks judge the stored row → post-image
    /// change, with the writer's roles.
    pub fn with_type_guards_from(
        mut self,
        state: &'a SharedState,
        database_id: nodedb_types::DatabaseId,
    ) -> Self {
        self.guards = Some(GuardSource {
            catalog: state.credentials.catalog(),
            database_id,
            state,
        });
        self
    }

    /// Validate the merge against the collection's declared rules: the
    /// typeguards on the merged post-image, and the state constraints and
    /// transition checks on the change from the stored row to it.
    fn check_collection_rules(
        &self,
        pre_image: Option<&Value>,
        post_image: Option<&Value>,
    ) -> crate::Result<()> {
        let Some(source) = self.guards.as_ref() else {
            return Ok(());
        };
        let Some(document) = post_image else {
            // A delete leaves no post-image for a rule to judge.
            return Ok(());
        };
        let bare = crate::control::target_identity::naming::bare_collection_name(
            source.database_id,
            self.collection,
        );
        let Some(coll) =
            source
                .catalog
                .get_collection(source.database_id, self.tenant_id.as_u64(), &bare)?
        else {
            return Ok(());
        };
        let crs = crate::control::crs_context::crs_context_for(source.state);
        if !coll.type_guards.is_empty() {
            let compiled =
                crate::data::executor::enforcement::typeguard::CompiledTypeGuard::compile_all(
                    &bare,
                    &coll.type_guards,
                );
            crate::data::executor::enforcement::typeguard::check_type_guards(
                &compiled, document, None, &crs,
            )
            .map_err(crate::Error::DataPlane)?;
        }
        // An insert has no stored row, so no transition to judge.
        if let Some(previous) = pre_image {
            use crate::data::executor::enforcement::update_transitions::{
                UpdateTransitionRules, check_update_transitions,
            };
            let rules = UpdateTransitionRules {
                state_constraints: &coll.state_constraints,
                transition_checks: &coll.transition_checks,
            };
            if !rules.is_empty() {
                check_update_transitions(
                    &bare,
                    rules,
                    &serde_json::Value::from(previous.clone()),
                    &serde_json::Value::from(document.clone()),
                    &self.auth.roles,
                    &crs,
                )
                .map_err(crate::Error::DataPlane)?;
            }
        }
        Ok(())
    }

    /// Build the policy from the immutable authenticated identity and bind its
    /// RLS context to the database selected by the server-side dispatch path.
    pub fn from_identity(
        tenant_id: TenantId,
        database_id: nodedb_types::DatabaseId,
        collection: &'a str,
        identity: &crate::control::security::identity::AuthenticatedIdentity,
        session_id: String,
        services: PolicyServices<'a>,
    ) -> Self {
        let mut auth = AuthContext::from_identity(identity, session_id);
        auth.database_id = Some(database_id);
        Self::with_auth(tenant_id, collection, auth, services)
    }

    fn deny(&self) -> crate::Result<()> {
        self.services.audit.emit(
            AuditEvent::RlsRejected,
            &self.auth.username,
            "CRDT post-merge write policy rejected",
            AuditEmitContext::new(Some(self.tenant_id), &self.auth.id, &self.auth.username),
        );
        Err(crate::Error::RejectedAuthz {
            tenant_id: self.tenant_id,
            resource: format!("RLS write policy denied collection '{}'", self.collection),
        })
    }
}

impl CrdtPostImagePolicy for ExternalCrdtPostImagePolicy<'_> {
    fn evaluate(&self, preview: &CrdtPreviewResult) -> crate::Result<()> {
        // Typeguards and transition rules bind every writer, a superuser
        // included.
        if self.guards.is_some() {
            let decode = |bytes: &[u8], which: &str| -> crate::Result<Option<Value>> {
                zerompk::from_msgpack(bytes).map_err(|e| crate::Error::Serialization {
                    format: "msgpack".into(),
                    detail: format!("CRDT {which} does not decode: {e}"),
                })
            };
            let pre_image = decode(&preview.pre_image_msgpack, "pre-image")?;
            let post_image = decode(&preview.post_image_msgpack, "post-image")?;
            self.check_collection_rules(pre_image.as_ref(), post_image.as_ref())?;
        }
        if self.auth.is_superuser() {
            return Ok(());
        }
        let policies = self
            .services
            .rls
            .write_policies(self.tenant_id.as_u64(), self.collection);
        if policies.is_empty() {
            return Ok(());
        }
        if preview.post_image_msgpack.len() > nodedb_crdt::DEFAULT_MAX_POST_IMAGE_BYTES {
            return self.deny();
        }
        let post_image: Option<Value> = match zerompk::from_msgpack(&preview.post_image_msgpack) {
            Ok(value) => value,
            Err(_) => return self.deny(),
        };
        let Some(value) = post_image else {
            // An absent post-image cannot prove any field predicate. Vacuous
            // policies still allow deletes; all others deny fail-closed.
            return if policies
                .iter()
                .all(|policy| policy.compiled_predicate.is_none())
            {
                Ok(())
            } else {
                self.deny()
            };
        };
        let document = serde_json::Value::from(value);
        self.services.rls.check_write_with_auth(
            self.tenant_id.as_u64(),
            self.collection,
            &document,
            &self.auth,
            self.services.audit,
            &nodedb_crs::CrsContext::new(self.services.crs_set),
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::control::security::audit::NoopAuditEmitter;
    use crate::control::security::identity::{AuthenticatedIdentity, Role};
    use crate::control::security::predicate::{
        CompareOp, PolicyMode, PredicateValue, RlsPredicate,
    };
    use crate::control::security::rls::{PolicyType, RlsPolicy};

    fn identity(superuser: bool) -> AuthenticatedIdentity {
        AuthenticatedIdentity::new_internal_service(
            42,
            "alice",
            TenantId::new(1),
            if superuser {
                vec![Role::Superuser]
            } else {
                vec![Role::ReadWrite]
            },
            superuser,
            None,
            AuthenticatedIdentity::default_database_set(superuser),
        )
    }

    fn store_with(predicate: Option<RlsPredicate>) -> RlsPolicyStore {
        let store = RlsPolicyStore::new();
        store
            .create_policy(RlsPolicy {
                name: "write_policy".into(),
                collection: "orders".into(),
                display_collection: "orders".into(),
                tenant_id: 1,
                policy_type: PolicyType::Write,
                compiled_predicate: predicate,
                mode: PolicyMode::default(),
                on_deny: Default::default(),
                enabled: true,
                created_by: "admin".into(),
                created_at: 0,
            })
            .expect("create policy");
        store
    }

    fn preview(post_image: Option<Value>) -> CrdtPreviewResult {
        CrdtPreviewResult {
            post_image_msgpack: zerompk::to_msgpack_vec(&post_image).expect("encode post-image"),
            pre_image_msgpack: vec![0xc0],
            imported_ops: 1,
            trimmed_ops: 0,
            frontier_digest: [7; 32],
        }
    }

    fn status_predicate() -> RlsPredicate {
        RlsPredicate::Compare {
            field: "status".into(),
            op: CompareOp::Eq,
            value: PredicateValue::Literal(serde_json::json!("active")),
        }
    }

    #[test]
    fn authoritative_matching_post_image_allows_and_mismatch_denies() {
        let store = store_with(Some(status_predicate()));
        let principal = identity(false);
        let audit = NoopAuditEmitter;
        let policy = ExternalCrdtPostImagePolicy::with_auth(
            principal.tenant_id,
            "orders",
            AuthContext::from_identity(&principal, "test".into()),
            PolicyServices {
                rls: &store,
                audit: &audit,
                crs_set: nodedb_crs::CustomCrsSet::empty(),
            },
        );
        assert!(
            policy
                .evaluate(&preview(Some(Value::from(
                    serde_json::json!({"status": "active"})
                ))))
                .is_ok()
        );
        assert!(matches!(
            policy.evaluate(&preview(Some(Value::from(
                serde_json::json!({"status": "draft"})
            )))),
            Err(crate::Error::RejectedAuthz { .. })
        ));
    }

    #[test]
    fn malformed_or_oversized_authoritative_post_image_denies() {
        let store = store_with(Some(status_predicate()));
        let principal = identity(false);
        let audit = NoopAuditEmitter;
        let policy = ExternalCrdtPostImagePolicy::with_auth(
            principal.tenant_id,
            "orders",
            AuthContext::from_identity(&principal, "test".into()),
            PolicyServices {
                rls: &store,
                audit: &audit,
                crs_set: nodedb_crs::CustomCrsSet::empty(),
            },
        );
        let malformed = CrdtPreviewResult {
            post_image_msgpack: vec![0x8f],
            pre_image_msgpack: vec![0xc0],
            imported_ops: 1,
            trimmed_ops: 0,
            frontier_digest: [0; 32],
        };
        assert!(matches!(
            policy.evaluate(&malformed),
            Err(crate::Error::RejectedAuthz { .. })
        ));
        let oversized = CrdtPreviewResult {
            post_image_msgpack: vec![0; nodedb_crdt::DEFAULT_MAX_POST_IMAGE_BYTES + 1],
            ..malformed
        };
        assert!(matches!(
            policy.evaluate(&oversized),
            Err(crate::Error::RejectedAuthz { .. })
        ));
    }

    #[test]
    fn absent_post_image_denies_field_policy_but_allows_vacuous_policy() {
        let principal = identity(false);
        let audit = NoopAuditEmitter;
        let constrained = store_with(Some(status_predicate()));
        let constrained_policy = ExternalCrdtPostImagePolicy::with_auth(
            principal.tenant_id,
            "orders",
            AuthContext::from_identity(&principal, "test".into()),
            PolicyServices {
                rls: &constrained,
                audit: &audit,
                crs_set: nodedb_crs::CustomCrsSet::empty(),
            },
        );
        assert!(matches!(
            constrained_policy.evaluate(&preview(None)),
            Err(crate::Error::RejectedAuthz { .. })
        ));

        let vacuous = store_with(None);
        let vacuous_policy = ExternalCrdtPostImagePolicy::with_auth(
            principal.tenant_id,
            "orders",
            AuthContext::from_identity(&principal, "test".into()),
            PolicyServices {
                rls: &vacuous,
                audit: &audit,
                crs_set: nodedb_crs::CustomCrsSet::empty(),
            },
        );
        assert!(vacuous_policy.evaluate(&preview(None)).is_ok());
    }

    #[test]
    fn auth_reference_uses_authenticated_context_and_superuser_bypasses_decode() {
        let store = store_with(Some(RlsPredicate::Compare {
            field: "owner_id".into(),
            op: CompareOp::Eq,
            value: PredicateValue::AuthRef("id".into()),
        }));
        let principal = identity(false);
        let audit = NoopAuditEmitter;
        let policy = ExternalCrdtPostImagePolicy::with_auth(
            principal.tenant_id,
            "orders",
            AuthContext::from_identity(&principal, "test".into()),
            PolicyServices {
                rls: &store,
                audit: &audit,
                crs_set: nodedb_crs::CustomCrsSet::empty(),
            },
        );
        assert!(
            policy
                .evaluate(&preview(Some(Value::from(
                    serde_json::json!({"owner_id": "42"})
                ))))
                .is_ok()
        );
        assert!(matches!(
            policy.evaluate(&preview(Some(Value::from(
                serde_json::json!({"owner_id": "7"})
            )))),
            Err(crate::Error::RejectedAuthz { .. })
        ));

        let root = identity(true);
        let root_policy = ExternalCrdtPostImagePolicy::with_auth(
            root.tenant_id,
            "orders",
            AuthContext::from_identity(&root, "test".into()),
            PolicyServices {
                rls: &store,
                audit: &audit,
                crs_set: nodedb_crs::CustomCrsSet::empty(),
            },
        );
        assert!(
            root_policy
                .evaluate(&CrdtPreviewResult {
                    post_image_msgpack: vec![0; nodedb_crdt::DEFAULT_MAX_POST_IMAGE_BYTES + 1],
                    pre_image_msgpack: vec![0xc0],
                    imported_ops: 1,
                    trimmed_ops: 0,
                    frontier_digest: [0; 32],
                })
                .is_ok()
        );
    }

    #[test]
    fn identity_constructor_binds_server_selected_database_for_rls() {
        let store = store_with(Some(RlsPredicate::Compare {
            field: "database_id".into(),
            op: CompareOp::Eq,
            value: PredicateValue::AuthRef("database_id".into()),
        }));
        let principal = identity(false);
        let audit = NoopAuditEmitter;
        let policy = ExternalCrdtPostImagePolicy::from_identity(
            principal.tenant_id,
            nodedb_types::DatabaseId::new(7),
            "orders",
            &principal,
            "test".into(),
            PolicyServices {
                rls: &store,
                audit: &audit,
                crs_set: nodedb_crs::CustomCrsSet::empty(),
            },
        );

        assert!(
            policy
                .evaluate(&preview(Some(Value::from(
                    serde_json::json!({"database_id": 7})
                ))))
                .is_ok()
        );
        assert!(matches!(
            policy.evaluate(&preview(Some(Value::from(
                serde_json::json!({"database_id": 8})
            )))),
            Err(crate::Error::RejectedAuthz { .. })
        ));
    }
}
