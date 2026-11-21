// SPDX-License-Identifier: BUSL-1.1

//! Constraint-set installation, version fencing, and bitemporal registration.

use nodedb_crdt::ConstraintKind;
use nodedb_types::QualifiedCollection;

use crate::error::ConstraintScopeRefusal;

use super::core::TenantCrdtEngine;

/// Refuse `constraint` unless it names `install_key` as its collection and
/// any foreign-key referent is a stored name of the same database.
///
/// The validator, the UNIQUE probe and the foreign-key row lookup all key by
/// stored name. A constraint in another form never meets them.
fn check_constraint_scope(
    install_key: &str,
    constraint: &nodedb_crdt::Constraint,
) -> crate::Result<()> {
    let refuse = |refusal| crate::Error::ConstraintScope {
        install_key: install_key.to_owned(),
        refusal,
    };
    if constraint.collection != install_key {
        return Err(refuse(ConstraintScopeRefusal::CollectionMismatch {
            constraint: constraint.name.clone(),
            collection: constraint.collection.clone(),
        }));
    }
    match &constraint.kind {
        ConstraintKind::ForeignKey { ref_collection, .. }
        | ConstraintKind::BiTemporalFK { ref_collection, .. } => {
            let referent_db = QualifiedCollection::database_of_stored(ref_collection).ok();
            let own_db = QualifiedCollection::database_of_stored(install_key).ok();
            if referent_db.is_none() || referent_db != own_db {
                return Err(refuse(ConstraintScopeRefusal::ForeignReferent {
                    constraint: constraint.name.clone(),
                    ref_collection: ref_collection.clone(),
                }));
            }
            Ok(())
        }
        ConstraintKind::Unique | ConstraintKind::NotNull | ConstraintKind::Check { .. } => Ok(()),
    }
}

impl TenantCrdtEngine {
    /// Set the conflict-resolution policy for a collection from a typed
    /// `CollectionPolicy`. The JSON-accepting variant in `policy.rs` is the
    /// DDL-facing path; this one is for in-process callers (tests, engine
    /// setup).
    pub fn set_collection_policy_typed(
        &mut self,
        collection: &str,
        policy: nodedb_crdt::policy::CollectionPolicy,
    ) {
        self.validator.policies_mut().set(collection, policy);
    }

    /// Checks whether `constraint_version >= installed` for `collection` and,
    /// if so, advances the stored version to `constraint_version`. Returns
    /// `true` when the caller should proceed with the constraint mutation,
    /// `false` when the incoming version is stale and the call should be
    /// ignored.
    fn advance_constraint_version(&mut self, collection: &str, constraint_version: u64) -> bool {
        let installed = self
            .constraint_versions
            .get(collection)
            .copied()
            .unwrap_or(0);
        if constraint_version >= installed {
            self.constraint_versions
                .insert(collection.to_owned(), constraint_version);
            true
        } else {
            false
        }
    }

    /// The constraint-set version this replica has installed for `collection`
    /// (via `SetConstraints`/`DropConstraints` on the per-vshard data Raft
    /// log). `0` means no constraints are installed. The apply-time write-gate
    /// compares a delta's admitted `constraint_version_required` against this
    /// to fence a delta that outran its constraint install.
    pub fn installed_constraint_version(&self, collection: &str) -> u64 {
        self.constraint_versions
            .get(collection)
            .copied()
            .unwrap_or(0)
    }

    /// Install the constraint set for `collection` into this tenant's
    /// validator, replacing any constraints previously scoped to it. Mutates
    /// only the validator — no per-collection CRDT state is created, since
    /// constraints govern future writes rather than existing rows.
    ///
    /// Fenced by `constraint_version`: the install proceeds only when the
    /// incoming version is `>=` the version last installed for `collection`.
    /// An older version is rejected as stale and the existing constraints are
    /// left untouched. The `>=` (rather than `>`) lets an idempotent
    /// re-delivery of the same version harmlessly re-apply. Returns `true`
    /// when the change was applied, `false` when rejected as stale.
    ///
    /// `collection` is the stored name. Each constraint names it as its
    /// `collection`. A foreign key references a stored name of the same
    /// database. Any other form is [`crate::Error::ConstraintScope`]. A
    /// refused set leaves the installed set and its version untouched.
    pub fn set_collection_constraints(
        &mut self,
        collection: &str,
        constraint_version: u64,
        constraints: Vec<nodedb_crdt::Constraint>,
    ) -> crate::Result<bool> {
        for constraint in &constraints {
            check_constraint_scope(collection, constraint)?;
        }
        if !self.advance_constraint_version(collection, constraint_version) {
            return Ok(false);
        }
        self.validator
            .set_collection_constraints(collection, constraints);
        Ok(true)
    }

    /// Remove every constraint scoped to `collection` from this tenant's
    /// validator. Fenced identically to [`TenantCrdtEngine::set_collection_constraints`]:
    /// applies only when `constraint_version` is `>=` the version last
    /// installed for `collection`. Returns `true` when applied, `false` when
    /// rejected as stale.
    pub fn drop_collection_constraints(
        &mut self,
        collection: &str,
        constraint_version: u64,
    ) -> bool {
        if !self.advance_constraint_version(collection, constraint_version) {
            return false;
        }
        self.validator.clear_collection_constraints(collection);
        true
    }

    /// Names of collections that currently have an installed constraint set
    /// (constraint_version > 0). Used by the snapshot builder to capture
    /// constraint state so a snapshot-installed follower reconstructs its
    /// validator instead of coming up empty.
    pub fn collections_with_constraints(&self) -> Vec<String> {
        self.constraint_versions
            .iter()
            .filter(|&(_, &v)| v > 0)
            .map(|(c, _)| c.clone())
            .collect()
    }

    /// Clone the constraints currently scoped to `collection` from this
    /// tenant's validator. Empty when the collection has no constraints.
    pub fn constraints_for_collection(&self, collection: &str) -> Vec<nodedb_crdt::Constraint> {
        self.validator
            .constraints_for(collection)
            .into_iter()
            .cloned()
            .collect()
    }

    /// Register a collection as bitemporal on this tenant's validator.
    ///
    /// Bitemporal collections get (a) UNIQUE constraints scoped to live
    /// rows only and (b) receiver-stamped `_ts_system` on apply.
    pub fn mark_bitemporal(&mut self, collection: impl Into<String>) {
        self.validator.mark_bitemporal(collection);
    }

    /// Is the named collection bitemporal?
    pub fn is_bitemporal(&self, collection: &str) -> bool {
        self.validator.is_bitemporal(collection)
    }

    /// Require signed, replay-protected peer deltas for this tenant's
    /// collection. The caller must also install a tenant signing verifier.
    pub fn require_delta_signing(&mut self, collection: impl Into<String>) {
        self.validator.require_delta_signing(collection);
    }

    /// Install the tenant's registered user/device signing keys.
    pub fn set_delta_verifier(&mut self, verifier: nodedb_crdt::DeltaSigner) {
        self.validator.set_delta_verifier(verifier);
    }
}

#[cfg(test)]
mod tests {
    use loro::LoroValue;
    use nodedb_crdt::constraint::ConstraintSet;
    use nodedb_crdt::policy::CollectionPolicy;
    use nodedb_crdt::validator::ProposedChange;

    use crate::types::TenantId;

    use super::*;

    #[test]
    fn installed_constraints_are_enforced_and_droppable() {
        // Engine starts with NO constraints; U2 installs them at runtime.
        let mut engine = TenantCrdtEngine::new(TenantId::new(1), 0, ConstraintSet::new()).unwrap();
        // Strict policy so a UNIQUE violation escalates (returns Err) instead of
        // auto-resolving.
        engine
            .validator
            .policies_mut()
            .set("users", CollectionPolicy::strict());

        let unique_email = nodedb_crdt::Constraint {
            name: "users_email_unique".into(),
            collection: "users".into(),
            field: "email".into(),
            kind: nodedb_crdt::ConstraintKind::Unique,
        };
        assert!(
            engine
                .set_collection_constraints("users", 1, vec![unique_email.clone()])
                .unwrap()
        );

        let mk = |row: &str, coll: &str, email: &str| ProposedChange {
            collection: coll.into(),
            row_id: row.into(),
            surrogate: nodedb_types::Surrogate::new(1),
            fields: vec![("email".into(), LoroValue::String(email.into()))],
        };

        // First insert with the email succeeds.
        engine
            .validate_and_apply(
                1,
                nodedb_crdt::CrdtAuthContext::default(),
                &mk("u1", "users", "a@b.com"),
                b"d".to_vec(),
                &nodedb_crs::CrsContext::builtin_only(),
            )
            .unwrap();

        // Duplicate email in the same collection is rejected.
        assert!(
            engine
                .validate_and_apply(
                    1,
                    nodedb_crdt::CrdtAuthContext::default(),
                    &mk("u2", "users", "a@b.com"),
                    b"d".to_vec(),
                    &nodedb_crs::CrsContext::builtin_only()
                )
                .is_err(),
            "duplicate email must violate the installed UNIQUE constraint"
        );

        // A different collection carries no such constraint — same value is fine.
        engine
            .validate_and_apply(
                1,
                nodedb_crdt::CrdtAuthContext::default(),
                &mk("p1", "posts", "a@b.com"),
                b"d".to_vec(),
                &nodedb_crs::CrsContext::builtin_only(),
            )
            .unwrap();

        // After dropping the constraint, the duplicate is accepted.
        assert!(engine.drop_collection_constraints("users", 2));
        engine
            .validate_and_apply(
                1,
                nodedb_crdt::CrdtAuthContext::default(),
                &mk("u3", "users", "a@b.com"),
                b"d".to_vec(),
                &nodedb_crs::CrsContext::builtin_only(),
            )
            .unwrap();
        assert!(engine.row_exists("users", "u3"));
    }

    #[test]
    fn set_collection_constraints_replaces_rather_than_accumulates() {
        let mut engine = TenantCrdtEngine::new(TenantId::new(1), 0, ConstraintSet::new()).unwrap();
        let c = nodedb_crdt::Constraint {
            name: "users_email_unique".into(),
            collection: "users".into(),
            field: "email".into(),
            kind: nodedb_crdt::ConstraintKind::Unique,
        };
        engine
            .set_collection_constraints("users", 1, vec![c.clone()])
            .unwrap();
        engine
            .set_collection_constraints("users", 1, vec![c.clone()])
            .unwrap();
        // Setting twice (same version, allowed by the `>=` fence) leaves exactly
        // one rule scoped to "users".
        assert_eq!(engine.validator.constraints_for("users").len(), 1);

        // An empty set clears the collection's constraints.
        engine
            .set_collection_constraints("users", 2, Vec::<nodedb_crdt::Constraint>::new())
            .unwrap();
        assert_eq!(engine.validator.constraints_for("users").len(), 0);
    }

    /// Builds a UNIQUE constraint named `name` on `users.email`.
    fn unique_named(name: &str) -> nodedb_crdt::Constraint {
        nodedb_crdt::Constraint {
            name: name.into(),
            collection: "users".into(),
            field: "email".into(),
            kind: nodedb_crdt::ConstraintKind::Unique,
        }
    }

    #[test]
    fn set_constraint_version_fence_rejects_stale_and_accepts_newer() {
        let mut engine = TenantCrdtEngine::new(TenantId::new(1), 0, ConstraintSet::new()).unwrap();

        // Install version 5: the constraint is visible.
        let v5 = unique_named("rule_v5");
        assert!(
            engine
                .set_collection_constraints("users", 5, vec![v5.clone()])
                .unwrap()
        );
        let installed = engine.constraints_for_collection("users");
        assert_eq!(installed.len(), 1);
        assert_eq!(installed[0].name, "rule_v5");

        // An older version 3 with a different set is rejected as stale; the
        // version-5 constraints remain untouched.
        let v3 = unique_named("rule_v3");
        assert!(
            !engine
                .set_collection_constraints("users", 3, vec![v3.clone()])
                .unwrap()
        );
        let unchanged = engine.constraints_for_collection("users");
        assert_eq!(unchanged.len(), 1);
        assert_eq!(unchanged[0].name, "rule_v5");

        // A newer version 7 applies and replaces.
        let v7 = unique_named("rule_v7");
        assert!(
            engine
                .set_collection_constraints("users", 7, vec![v7.clone()])
                .unwrap()
        );
        let replaced = engine.constraints_for_collection("users");
        assert_eq!(replaced.len(), 1);
        assert_eq!(replaced[0].name, "rule_v7");
    }

    #[test]
    fn drop_constraint_version_fence_rejects_stale_and_accepts_newer() {
        let mut engine = TenantCrdtEngine::new(TenantId::new(1), 0, ConstraintSet::new()).unwrap();
        assert!(
            engine
                .set_collection_constraints("users", 5, vec![unique_named("rule_v5")])
                .unwrap()
        );

        // A drop at version 4 (older than the installed 5) is rejected; the
        // constraints survive.
        assert!(!engine.drop_collection_constraints("users", 4));
        assert_eq!(engine.constraints_for_collection("users").len(), 1);

        // A drop at version 6 applies and clears.
        assert!(engine.drop_collection_constraints("users", 6));
        assert_eq!(
            engine.constraints_for_collection("users"),
            Vec::<nodedb_crdt::Constraint>::new()
        );
    }

    #[test]
    fn set_constraint_same_version_is_idempotent() {
        let mut engine = TenantCrdtEngine::new(TenantId::new(1), 0, ConstraintSet::new()).unwrap();
        let rule = unique_named("rule_v5");

        // The same version delivered twice both apply (the `>=` fence) and leave a
        // single rule per name — re-delivery is harmless.
        for _ in 0..2 {
            assert!(
                engine
                    .set_collection_constraints("users", 5, vec![rule.clone()])
                    .unwrap()
            );
        }
        let installed = engine.constraints_for_collection("users");
        assert_eq!(installed.len(), 1);
        assert_eq!(installed[0].name, "rule_v5");
    }

    #[test]
    fn purge_clears_constraints_and_resets_version_fence() {
        let mut engine = TenantCrdtEngine::new(TenantId::new(1), 0, ConstraintSet::new()).unwrap();

        // Install a constraint at version 5, then drop the whole collection.
        assert!(
            engine
                .set_collection_constraints("users", 5, vec![unique_named("old_rule")])
                .unwrap()
        );
        engine.purge_collection("users").unwrap();

        // Purge clears the constraints outright.
        assert_eq!(
            engine.constraints_for_collection("users"),
            Vec::<nodedb_crdt::Constraint>::new()
        );

        // A re-created collection of the same name restarts its descriptor version
        // at 1. Because purge also reset the fence, that fresh low-version install
        // is accepted rather than rejected as stale against the dropped 5.
        assert!(
            engine
                .set_collection_constraints("users", 1, vec![unique_named("new_rule")])
                .unwrap()
        );
        let installed = engine.constraints_for_collection("users");
        assert_eq!(installed.len(), 1);
        assert_eq!(installed[0].name, "new_rule");
    }

    /// Builds a foreign key on `collection.user_id` that references
    /// `ref_collection`.
    fn foreign_key(collection: &str, ref_collection: &str) -> nodedb_crdt::Constraint {
        nodedb_crdt::Constraint {
            name: "orders_user_fk".into(),
            collection: collection.into(),
            field: "user_id".into(),
            kind: nodedb_crdt::ConstraintKind::ForeignKey {
                ref_collection: ref_collection.into(),
                ref_key: "id".into(),
            },
        }
    }

    #[test]
    fn a_constraint_that_names_the_bare_collection_under_a_stored_key_is_refused() {
        let mut engine = TenantCrdtEngine::new(TenantId::new(1), 0, ConstraintSet::new()).unwrap();
        let error = engine
            .set_collection_constraints("7/users", 1, vec![unique_named("users_email_unique")])
            .expect_err("a bare collection under a stored key is refused");
        match error {
            crate::Error::ConstraintScope {
                install_key,
                refusal:
                    ConstraintScopeRefusal::CollectionMismatch {
                        constraint,
                        collection,
                    },
            } => {
                assert_eq!(install_key, "7/users");
                assert_eq!(constraint, "users_email_unique");
                assert_eq!(collection, "users");
            }
            other => panic!("expected a collection mismatch, got {other:?}"),
        }
        // The refused set installs nothing and leaves the version fence alone.
        assert!(engine.constraints_for_collection("7/users").is_empty());
        assert!(engine.constraints_for_collection("users").is_empty());
        assert_eq!(engine.installed_constraint_version("7/users"), 0);
    }

    #[test]
    fn a_foreign_key_that_references_another_database_is_refused() {
        let mut engine = TenantCrdtEngine::new(TenantId::new(1), 0, ConstraintSet::new()).unwrap();
        for referent in ["8/users", "users", "x/users", "7/a/b"] {
            let error = engine
                .set_collection_constraints("7/orders", 1, vec![foreign_key("7/orders", referent)])
                .expect_err(referent);
            match error {
                crate::Error::ConstraintScope {
                    install_key,
                    refusal:
                        ConstraintScopeRefusal::ForeignReferent {
                            constraint,
                            ref_collection,
                        },
                } => {
                    assert_eq!(install_key, "7/orders");
                    assert_eq!(constraint, "orders_user_fk");
                    assert_eq!(ref_collection, referent);
                }
                other => panic!("{referent}: expected a foreign referent, got {other:?}"),
            }
        }
        assert!(engine.constraints_for_collection("7/orders").is_empty());
        assert_eq!(engine.installed_constraint_version("7/orders"), 0);
    }

    #[test]
    fn a_foreign_key_that_references_its_own_database_installs() {
        let mut engine = TenantCrdtEngine::new(TenantId::new(1), 0, ConstraintSet::new()).unwrap();
        assert!(
            engine
                .set_collection_constraints("7/orders", 1, vec![foreign_key("7/orders", "7/users")])
                .unwrap()
        );
        assert!(
            engine
                .set_collection_constraints("orders", 1, vec![foreign_key("orders", "users")])
                .unwrap()
        );
        assert_eq!(engine.constraints_for_collection("7/orders").len(), 1);
        assert_eq!(engine.constraints_for_collection("orders").len(), 1);
    }
}
