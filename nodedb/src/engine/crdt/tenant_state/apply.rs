// SPDX-License-Identifier: BUSL-1.1

//! Delta apply paths: Raft-committed import and peer-validated upsert.

use loro::LoroValue;
use nodedb_types::columnar::schema::TS_SYSTEM;

use nodedb_crdt::pre_validate::{self, PreValidationResult};
use nodedb_crdt::validator::ProposedChange;

use super::core::{TenantCrdtEngine, TenantRowLookup};

impl TenantCrdtEngine {
    /// Pre-validate a proposed change (fast-reject before Raft).
    pub fn pre_validate(
        &self,
        change: &ProposedChange,
        crs: &nodedb_crs::CrsContext<'_>,
    ) -> PreValidationResult {
        let view = TenantRowLookup {
            collections: &self.collections,
            array_surrogate_ids: &self.array_surrogate_ids,
        };
        pre_validate::pre_validate(&self.validator, &view, change, crs)
    }

    /// Test-only raw import used to seed transaction rollback fixtures.
    ///
    /// Production applies always go through the validated / authenticated paths
    /// ([`Self::apply_committed_delta_validated`] /
    /// [`Self::apply_committed_delta_authenticated`]), which apply into a
    /// detached candidate and enforce constraints and signing before any
    /// authoritative state moves — so no unvalidated peer delta can mutate it.
    #[cfg(test)]
    pub(crate) fn apply_committed_delta(
        &mut self,
        collection: &str,
        delta: &[u8],
    ) -> crate::Result<()> {
        self.state_mut(collection)?
            .import(delta)
            .map(|_admission| ())
            .map_err(crate::Error::Crdt)
    }

    /// Validate and attempt to apply a delta from a peer.
    ///
    /// If constraints are violated, the delta is routed to the DLQ.
    /// Returns `Ok(())` on success, or the constraint violation error.
    ///
    /// For bitemporal collections, `_ts_system` is always stamped with the
    /// receiving node's clock, overwriting any value the sender supplied.
    /// This keeps system-time receiver-authoritative so convergence does
    /// not depend on clock agreement between peers.
    pub fn validate_and_apply(
        &mut self,
        peer_id: u64,
        auth: nodedb_crdt::CrdtAuthContext,
        change: &ProposedChange,
        delta_bytes: Vec<u8>,
        crs: &nodedb_crs::CrsContext<'_>,
    ) -> crate::Result<()> {
        // Tenant-wide view over all collections + array surrogates. The view
        // borrows `self.collections` / `self.array_surrogate_ids` immutably
        // while `self.validator` is borrowed mutably — disjoint fields, so both
        // borrows coexist. The view borrow ends before the upsert below.
        {
            let view = TenantRowLookup {
                collections: &self.collections,
                array_surrogate_ids: &self.array_surrogate_ids,
            };
            self.validator
                .validate_or_reject(&view, peer_id, auth, change, delta_bytes, crs)
                .map_err(crate::Error::Crdt)?;
        }

        let is_bitemporal = self.validator.is_bitemporal(&change.collection);
        // no-determinism: peer delta validation path, not Calvin apply_committed_delta path
        let now_ms = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as i64;

        let mut fields: Vec<(&str, LoroValue)> = change
            .fields
            .iter()
            .filter(|(k, _)| !(is_bitemporal && k == TS_SYSTEM))
            .map(|(k, v)| (k.as_str(), v.clone()))
            .collect();

        let state = self.state_mut(&change.collection)?;
        if is_bitemporal {
            fields.push((TS_SYSTEM, LoroValue::I64(now_ms)));
            state
                .upsert_versioned(&change.collection, &change.row_id, &fields)
                .map_err(crate::Error::Crdt)
        } else {
            state
                .upsert(&change.collection, &change.row_id, &fields)
                .map_err(crate::Error::Crdt)
        }
    }
}

#[cfg(test)]
mod tests {
    use nodedb_crdt::constraint::ConstraintSet;
    use nodedb_crdt::policy::CollectionPolicy;

    use crate::types::TenantId;

    use super::*;

    fn test_constraints() -> ConstraintSet {
        let mut cs = ConstraintSet::new();
        cs.add_unique("users_email_unique", "users", "email");
        cs.add_not_null("users_name_nn", "users", "name");
        cs
    }

    #[test]
    fn valid_write_applies() {
        let mut engine = TenantCrdtEngine::new(TenantId::new(1), 0, test_constraints()).unwrap();

        let change = ProposedChange {
            collection: "users".into(),
            row_id: "u1".into(),
            surrogate: nodedb_types::Surrogate::new(1),
            fields: vec![
                ("name".into(), LoroValue::String("Alice".into())),
                (
                    "email".into(),
                    LoroValue::String("alice@example.com".into()),
                ),
            ],
        };

        engine
            .validate_and_apply(
                1,
                nodedb_crdt::CrdtAuthContext::default(),
                &change,
                b"delta".to_vec(),
                &nodedb_crs::CrsContext::builtin_only(),
            )
            .unwrap();

        assert!(engine.row_exists("users", "u1"));
        assert_eq!(engine.dlq_len(), 0);
    }

    #[test]
    fn constraint_violation_routes_to_dlq() {
        let mut engine = TenantCrdtEngine::new(TenantId::new(1), 0, test_constraints()).unwrap();
        // Use strict policy so violations escalate to DLQ instead of auto-resolving.
        engine
            .validator
            .policies_mut()
            .set("users", CollectionPolicy::strict());

        // Missing "name" field violates NOT NULL.
        let change = ProposedChange {
            collection: "users".into(),
            row_id: "u1".into(),
            surrogate: nodedb_types::Surrogate::new(1),
            fields: vec![("email".into(), LoroValue::String("a@b.com".into()))],
        };

        let err = engine
            .validate_and_apply(
                42,
                nodedb_crdt::CrdtAuthContext::default(),
                &change,
                b"delta".to_vec(),
                &nodedb_crs::CrsContext::builtin_only(),
            )
            .unwrap_err();

        assert!(matches!(err, crate::Error::Crdt(_)));
        assert_eq!(engine.dlq_len(), 1);
    }

    #[test]
    fn pre_validate_fast_rejects() {
        let engine = TenantCrdtEngine::new(TenantId::new(1), 0, test_constraints()).unwrap();

        let change = ProposedChange {
            collection: "users".into(),
            row_id: "u1".into(),
            surrogate: nodedb_types::Surrogate::new(1),
            fields: vec![("email".into(), LoroValue::String("a@b.com".into()))],
        };

        match engine.pre_validate(&change, &nodedb_crs::CrsContext::builtin_only()) {
            PreValidationResult::FastReject { constraint, .. } => {
                assert_eq!(constraint, "users_name_nn");
            }
            _ => panic!("expected fast reject"),
        }
    }

    #[test]
    fn unique_violation_after_first_write() {
        let mut engine = TenantCrdtEngine::new(TenantId::new(1), 0, test_constraints()).unwrap();
        // Strict mode: UNIQUE violations escalate to DLQ.
        engine
            .validator
            .policies_mut()
            .set("users", CollectionPolicy::strict());

        let first = ProposedChange {
            collection: "users".into(),
            row_id: "u1".into(),
            surrogate: nodedb_types::Surrogate::new(1),
            fields: vec![
                ("name".into(), LoroValue::String("Alice".into())),
                (
                    "email".into(),
                    LoroValue::String("alice@example.com".into()),
                ),
            ],
        };
        engine
            .validate_and_apply(
                1,
                nodedb_crdt::CrdtAuthContext::default(),
                &first,
                b"d1".to_vec(),
                &nodedb_crs::CrsContext::builtin_only(),
            )
            .unwrap();

        // Second write with same email should fail.
        let second = ProposedChange {
            collection: "users".into(),
            row_id: "u2".into(),
            surrogate: nodedb_types::Surrogate::new(2),
            fields: vec![
                ("name".into(), LoroValue::String("Bob".into())),
                (
                    "email".into(),
                    LoroValue::String("alice@example.com".into()),
                ),
            ],
        };
        assert!(
            engine
                .validate_and_apply(
                    2,
                    nodedb_crdt::CrdtAuthContext::default(),
                    &second,
                    b"d2".to_vec(),
                    &nodedb_crs::CrsContext::builtin_only()
                )
                .is_err()
        );
        assert_eq!(engine.dlq_len(), 1);
    }

    // ── cross-collection FK via tenant-wide validator ─────────────────────────────

    fn fk_constraints() -> ConstraintSet {
        let mut cs = ConstraintSet::new();
        cs.add_foreign_key("posts_author_fk", "posts", "author_id", "users", "id");
        cs
    }

    fn apply_change(
        engine: &mut TenantCrdtEngine,
        collection: &str,
        row_id: &str,
        fields: Vec<(String, LoroValue)>,
    ) -> crate::Result<()> {
        let change = ProposedChange {
            collection: collection.into(),
            row_id: row_id.into(),
            surrogate: nodedb_types::Surrogate::new(1),
            fields,
        };
        engine.validate_and_apply(
            1,
            nodedb_crdt::CrdtAuthContext::default(),
            &change,
            b"d".to_vec(),
            &nodedb_crs::CrsContext::builtin_only(),
        )
    }

    #[test]
    fn cross_collection_fk_rejects_missing_referent() {
        let mut engine = TenantCrdtEngine::new(TenantId::new(2), 0, fk_constraints()).unwrap();
        engine.set_collection_policy_typed("posts", CollectionPolicy::strict());

        let result = apply_change(
            &mut engine,
            "posts",
            "p1",
            vec![
                ("title".into(), LoroValue::String("Hello".into())),
                ("author_id".into(), LoroValue::String("u1".into())),
            ],
        );

        assert!(result.is_err());
        assert_eq!(engine.dlq_len(), 1);
        assert!(!engine.row_exists("posts", "p1"));
    }

    #[test]
    fn cross_collection_fk_accepts_after_referent_inserted() {
        let mut engine = TenantCrdtEngine::new(TenantId::new(3), 0, fk_constraints()).unwrap();
        engine.set_collection_policy_typed("posts", CollectionPolicy::strict());

        apply_change(
            &mut engine,
            "users",
            "u1",
            vec![("name".into(), LoroValue::String("Alice".into()))],
        )
        .unwrap();

        apply_change(
            &mut engine,
            "posts",
            "p1",
            vec![
                ("title".into(), LoroValue::String("Hello".into())),
                ("author_id".into(), LoroValue::String("u1".into())),
            ],
        )
        .unwrap();

        assert!(engine.row_exists("users", "u1"));
        assert!(engine.row_exists("posts", "p1"));
        assert_eq!(engine.dlq_len(), 0);
    }

    // ── array-surrogate FK via tenant registry ────────────────────────────────────

    #[test]
    fn array_surrogate_satisfies_cross_engine_fk() {
        let mut cs = ConstraintSet::new();
        cs.add_foreign_key("posts_author_fk", "posts", "author_id", "users", "id");
        let mut engine = TenantCrdtEngine::new(TenantId::new(4), 0, cs).unwrap();
        engine.set_collection_policy_typed("posts", CollectionPolicy::strict());

        engine.register_array_surrogate("arr_42");

        apply_change(
            &mut engine,
            "posts",
            "p1",
            vec![
                ("title".into(), LoroValue::String("Hello".into())),
                ("author_id".into(), LoroValue::String("arr_42".into())),
            ],
        )
        .unwrap();

        assert!(engine.row_exists("posts", "p1"));
        assert_eq!(engine.dlq_len(), 0);
    }

    // ── an apply that did not apply must not report success ──────────────────────

    /// Build a peer document spanning two collections and export one incremental
    /// delta per write, mirroring how an embedded client that keeps a single Loro
    /// document for the whole database produces its deltas.
    ///
    /// Returns `(first_delta_for_target, later_delta_for_target)` where the later
    /// delta causally depends on an intervening write to the *other* collection.
    fn interleaved_collection_deltas(peer: u64, target: &str, other: &str) -> (Vec<u8>, Vec<u8>) {
        let state = nodedb_crdt::state::CrdtState::new(peer).unwrap();

        let v0 = state.oplog_version_vector();
        state
            .upsert(target, "first", &[("v", LoroValue::I64(1))])
            .unwrap();
        let first = state.export_updates_since(&v0).unwrap();

        state
            .upsert(other, "aside", &[("v", LoroValue::I64(2))])
            .unwrap();

        let v2 = state.oplog_version_vector();
        state
            .upsert(target, "later", &[("v", LoroValue::I64(3))])
            .unwrap();
        let later = state.export_updates_since(&v2).unwrap();

        (first, later)
    }

    /// `apply_committed_delta` runs AFTER Raft consensus: the entry is already in
    /// the log on every replica. If the import leaves its operations causally
    /// pending, this replica's state silently diverges from a committed log entry
    /// while returning `Ok` — the divergence is undetectable and permanent.
    #[test]
    fn raft_committed_apply_does_not_report_success_without_applying() {
        let mut engine = TenantCrdtEngine::new(TenantId::new(1), 0, ConstraintSet::new()).unwrap();
        let (first, later) = interleaved_collection_deltas(31, "users", "signals");

        engine.apply_committed_delta("users", &first).unwrap();
        assert!(engine.row_exists("users", "first"));

        let result = engine.apply_committed_delta("users", &later);

        assert!(
            result.is_err() || engine.row_exists("users", "later"),
            "a Raft-committed apply reported success while its operations stayed \
             causally pending — this replica has silently diverged from the log"
        );
    }
}
