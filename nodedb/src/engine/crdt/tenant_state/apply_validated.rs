// SPDX-License-Identifier: BUSL-1.1

//! Apply-and-validate a peer delta on the sync path.
//!
//! Unlike the bare [`TenantCrdtEngine::apply_committed_delta`] import, this
//! path applies into a detached candidate, re-reads the rows the delta
//! *actually* wrote, and validates each against installed constraints. Only a
//! clean candidate replaces authoritative state. A violation is routed to the
//! dead-letter queue and surfaced as a deterministic [`ViolationType`].

use nodedb_crdt::state::CrdtState;
use nodedb_crdt::validator::{ValidationOutcome, Violation};
use nodedb_types::Surrogate;
use nodedb_types::sync::violation::ViolationType;

use super::core::TenantCrdtEngine;

/// Server-derived signing context for an externally synchronized delta.
pub struct DeltaSigningAdmission {
    pub auth: nodedb_crdt::CrdtAuthContext,
    pub required: bool,
    /// The Control Plane verified this signature against the authenticated
    /// session's catalog-backed key before constructing the authenticated
    /// physical-plan variant. WAL/Raft replay preserves that admission result.
    pub preverified: bool,
}

/// What a validated delta apply writes into.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ApplyTarget<'a> {
    /// One document's delta, under the row's bound surrogate. The caller
    /// refuses `Surrogate::ZERO` before it builds this target. A delta that
    /// writes any other row is refused as malformed.
    Document {
        document_id: &'a str,
        surrogate: Surrogate,
    },
    /// A per-collection snapshot import. It can write any row of the
    /// collection and binds no row identity.
    Collection,
}

impl ApplyTarget<'_> {
    /// Whether a delta applied to this target can write `row`.
    fn admits_row(&self, row: &str) -> bool {
        match self {
            Self::Document { document_id, .. } => row == *document_id,
            Self::Collection => true,
        }
    }

    /// The identity `row` validates under: the document's surrogate for a
    /// document target. A snapshot import names no row identity, and the
    /// validator's change record carries `Surrogate::ZERO` for it. The
    /// validator never reads that field.
    fn validation_surrogate(&self, row: &str) -> Surrogate {
        match self {
            Self::Document {
                document_id,
                surrogate,
            } if row == *document_id => *surrogate,
            Self::Document { .. } | Self::Collection => Surrogate::ZERO,
        }
    }
}

/// Outcome of applying and validating one peer delta.
#[derive(Debug)]
pub enum ValidatedApplyOutcome {
    /// The delta imported and every row it wrote satisfied its constraints.
    ///
    /// `write_set` lists the `(collection, row_id)` pairs the delta actually
    /// wrote. The caller inspects it to enforce the one-document-per-delta
    /// sync contract: cross-engine identity binds exactly one Control-Plane
    /// surrogate per delta, so the Data Plane can only materialize the single
    /// frame-declared row. A delta that wrote other or additional rows (a
    /// client that coalesced N upserts into one delta) must be rejected
    /// loudly — materializing just one row would silently drop the rest.
    ///
    /// `imported_ops` is how many operations the delta actually contributed.
    /// Zero means the CRDT merge trimmed the whole delta as already-known: the
    /// document did not move, so the caller must not report the write as
    /// applied. That is correct and expected for an idempotent replay, and it
    /// is also exactly what a peer-id collision looks like — two replicas
    /// claiming one peer id write into overlapping `(peer, counter)` ranges and
    /// the second one's operations are discarded as duplicates of the first's.
    Clean {
        write_set: Vec<(String, String)>,
        imported_ops: usize,
    },
    /// The candidate violated a constraint and was discarded. The violation
    /// is in the DLQ and translated for the caller.
    Rejected(ViolationType),
    /// The candidate violated a constraint and was discarded, and the DLQ
    /// refused its entry. Nothing applied and nothing on this node records
    /// the delta, so the caller refuses it as an error and the sender keeps it.
    DeadLetterRefused {
        violation: ViolationType,
        error: nodedb_crdt::CrdtError,
    },
    /// The delta bytes could not be imported (corrupt / undecodable). Treated
    /// as an idempotent no-op so the stream is not wedged.
    Malformed,
    /// The delta's causal predecessors are absent from this collection's
    /// document, so Loro buffered its operations as pending and the applied
    /// state did not move.
    ///
    /// Distinct from [`Self::Malformed`]: malformed bytes can never apply and
    /// are safely skipped, whereas these operations are well-formed and simply
    /// arrived without their history. Treating them as a no-op would advance
    /// the high-water-mark past a write that was never applied — the silent
    /// data-loss path. The caller must refuse the delta instead.
    PendingDependencies,
    /// This node could not build the detached candidate the delta validates
    /// in: exporting or seeding the collection's document failed. Nothing
    /// applied, and the failure says nothing about the delta, so the caller
    /// refuses it retryably and never as malformed.
    CandidateUnavailable,
}

impl TenantCrdtEngine {
    /// Import a peer delta, then validate the rows it wrote against installed
    /// constraints.
    ///
    /// Import and validation occur on a detached candidate. A violating row is
    /// routed to the DLQ without mutating authoritative state, and a corrupt
    /// blob returns [`ValidatedApplyOutcome::Malformed`].
    ///
    /// `target` names what the delta writes. A document target binds the
    /// sender's claimed row, so its UNIQUE / FK probes reference the correct
    /// cross-engine identity. A snapshot import targets the whole collection.
    pub fn apply_committed_delta_validated(
        &mut self,
        collection: &str,
        delta: &[u8],
        target: ApplyTarget<'_>,
        peer_id: u64,
    ) -> ValidatedApplyOutcome {
        self.apply_committed_delta_authenticated(
            collection,
            delta,
            target,
            peer_id,
            DeltaSigningAdmission {
                auth: nodedb_crdt::CrdtAuthContext::default(),
                required: false,
                preverified: false,
            },
        )
    }

    /// Apply a sync delta after enforcing the catalog-owned signing policy.
    pub fn apply_committed_delta_authenticated(
        &mut self,
        collection: &str,
        delta: &[u8],
        target: ApplyTarget<'_>,
        peer_id: u64,
        admission: DeltaSigningAdmission,
    ) -> ValidatedApplyOutcome {
        self.last_dead_letter = None;
        if admission.required && admission.auth.delta_signature == [0; 32] {
            return ValidatedApplyOutcome::Malformed;
        }
        if !admission.preverified
            && self
                .validator
                .verify_delta_auth(collection, &admission.auth, delta)
                .is_err()
        {
            return ValidatedApplyOutcome::Malformed;
        }
        let candidate = match self.take_apply_candidate(collection) {
            Ok(candidate) => candidate,
            Err(()) => return ValidatedApplyOutcome::CandidateUnavailable,
        };
        let imported = candidate.import_with_write_set(delta);
        let admission = match imported.outcome {
            Ok(admission) => admission,
            // Well-formed operations that arrived without their causal history
            // (their predecessors live in another collection's document, absent
            // from this candidate). Some of the delta stayed pending, so this is
            // NOT a no-op the caller may acknowledge — refusing it preserves the
            // row instead of advancing the high-water-mark past a write that
            // never applied. The candidate is dropped with whatever part did
            // apply, so authoritative state stays unchanged.
            Err(nodedb_crdt::CrdtError::ImportPendingDependencies) => {
                return ValidatedApplyOutcome::PendingDependencies;
            }
            Err(_) => return ValidatedApplyOutcome::Malformed,
        };
        // A write outside the root-map-of-row-maps shape names no row the
        // constraints can check. The candidate is dropped unapplied.
        let write_set = match imported.write_set {
            Ok(write_set) => write_set,
            Err(fault) => return self.reject_shape_fault(collection, delta, peer_id, &fault),
        };
        if write_set
            .iter()
            .any(|(written, row)| written != collection || !target.admits_row(row))
        {
            return ValidatedApplyOutcome::Malformed;
        }

        // Install the candidate only while validation reads it. Keep the exact
        // previous state available for a no-fail rollback on rejection.
        let previous = self.collections.insert(collection.to_owned(), candidate);
        for (coll, row) in &write_set {
            let sg = target.validation_surrogate(row);
            let violation = match self.validate_committed_row(coll, row, sg) {
                ValidationOutcome::Accepted => continue,
                ValidationOutcome::Rejected(violations) => match violations.into_iter().next() {
                    Some(v) => v,
                    None => continue,
                },
                // A CHECK predicate that could not be evaluated (division/modulo
                // by zero) fails closed: roll back and route to the DLQ
                // exactly like a genuine violation — never silently
                // treated as accepted (which the old `let-else` would have done).
                ValidationOutcome::EvalError {
                    constraint_name,
                    error,
                } => Violation {
                    constraint_name,
                    reason: format!("CHECK predicate failed to evaluate: {error}"),
                    hint: nodedb_crdt::dead_letter::CompensationHint::ManualIntervention {
                        reason: format!("CHECK predicate raised an evaluation error: {error}"),
                    },
                },
            };
            let wire = violation_to_type(&violation);
            let outcome = self.dead_letter(coll, delta, peer_id, &violation, wire);
            match previous {
                Some(previous) => {
                    self.collections.insert(collection.to_owned(), previous);
                }
                None => {
                    self.collections.remove(collection);
                }
            }
            return outcome;
        }

        // The candidate is now authoritative. Bring the doc it displaced up to
        // the same version and keep it as the next delta's candidate: it starts
        // identical to authoritative, so the next apply pays the delta rather
        // than another full copy of the collection.
        //
        // Both docs held the same state and take the same delta, so the CRDT
        // merge leaves them identical. If that second import somehow does not
        // land, no candidate is retained and the next apply rebuilds one —
        // slower, never wrong.
        if let Some(previous) = previous
            && previous.import(delta).is_ok()
        {
            self.apply_candidates
                .insert(collection.to_owned(), previous);
        }

        ValidatedApplyOutcome::Clean {
            write_set,
            imported_ops: admission.new_operations,
        }
    }

    /// A document identical to `collection`'s authoritative state, to import a
    /// delta into before deciding whether to keep it.
    ///
    /// Reuses the candidate left behind by the previous apply when there is
    /// one. Building it costs a full encode and decode of the collection —
    /// Loro's own `fork` is implemented the same way, so there is no cheaper
    /// copy available — and a run of deltas (WAL replay, a committed batch, a
    /// sync burst) would otherwise pay that per delta rather than once.
    ///
    /// The candidate is only ever returned to the pool after a clean apply. A
    /// delta that was refused for any reason leaves its operations in the
    /// candidate — Loro buffers even causally-pending ones — so a poisoned
    /// candidate is dropped rather than reused.
    fn take_apply_candidate(&mut self, collection: &str) -> Result<CrdtState, ()> {
        if let Some(candidate) = self.apply_candidates.remove(collection) {
            // A candidate is only usable while it still matches the state it
            // was cloned from. Anything else that touches this collection — a
            // local write through `state_mut`, a transaction rollback, a
            // snapshot import, a purge — leaves it behind, and installing a
            // behind candidate as authoritative would silently discard those
            // writes. Checking the frontier here means no other mutation site
            // has to remember this one exists; a stale candidate is simply
            // rebuilt.
            let current_frontier = self.collections.get(collection).map(|s| s.frontier());
            if current_frontier.is_some_and(|frontier| frontier == candidate.frontier()) {
                return Ok(candidate);
            }
        }
        // The candidate becomes authoritative on a clean apply, so it must be
        // born with the same derived peer id `state_mut` would have given the
        // collection. The node's base peer id would mint operation ids that
        // collide across collections — the silent row-drop `collection_peer_id`
        // exists to prevent.
        let peer_id = Self::collection_peer_id(self.peer_id, collection);
        match self.collections.get(collection) {
            // Its own snapshot, exported on the line above: admitted as local,
            // because under the peer ceilings a collection that outgrew them
            // would fail to seed and every subsequent delta would report
            // `Malformed` — silently unwritable, with the sender blamed for it.
            Some(current) => {
                let snapshot = current.export_snapshot().map_err(|_| ())?;
                CrdtState::from_local_snapshot(peer_id, &snapshot).map_err(|_| ())
            }
            None => CrdtState::new(peer_id).map_err(|_| ()),
        }
    }

    /// Drop every retained apply candidate.
    ///
    /// Callers that apply a run of deltas — WAL replay, a committed batch —
    /// call this when the run ends, so the second document per collection does
    /// not outlive the work it accelerates.
    pub fn clear_apply_candidates(&mut self) {
        self.apply_candidates.clear();
    }

    /// How many collections are currently holding a retained candidate.
    #[cfg(test)]
    pub(crate) fn apply_candidate_count(&self) -> usize {
        self.apply_candidates.len()
    }

    /// The Loro peer id of a collection's authoritative document.
    #[cfg(test)]
    pub(crate) fn collection_peer_id_for_test(&self, collection: &str) -> Option<u64> {
        self.collections.get(collection).map(|s| s.peer_id())
    }

    /// Dead-letter a delta whose writes break the collection shape. The
    /// caller gets the fault as a schema violation.
    fn reject_shape_fault(
        &mut self,
        collection: &str,
        delta: &[u8],
        peer_id: u64,
        fault: &nodedb_crdt::CrdtError,
    ) -> ValidatedApplyOutcome {
        let reason = fault.to_string();
        let violation = Violation {
            constraint_name: ROW_SHAPE_CONSTRAINT.to_string(),
            reason: reason.clone(),
            hint: nodedb_crdt::CompensationHint::ManualIntervention {
                reason: reason.clone(),
            },
        };
        let wire = ViolationType::SchemaViolation {
            field: shape_fault_target(fault),
            reason,
        };
        self.dead_letter(collection, delta, peer_id, &violation, wire)
    }

    /// Enqueue a rejected delta to the DLQ and report it as `wire`.
    ///
    /// The DLQ entry carries the INTERNAL compensation hint verbatim. The wire
    /// hint the client sees derives from `wire`, never from the DLQ. The DLQ
    /// id and timestamp are node-local and non-deterministic, so they are not
    /// returned. A delta the DLQ cannot hold is recorded nowhere, so the apply
    /// reports [`ValidatedApplyOutcome::DeadLetterRefused`].
    fn dead_letter(
        &mut self,
        collection: &str,
        delta: &[u8],
        peer_id: u64,
        violation: &Violation,
        wire: ViolationType,
    ) -> ValidatedApplyOutcome {
        // No authenticated user identity is threaded to the apply path in this
        // layer, so the DLQ records `0` (unauthenticated/legacy).
        let user_id = 0u64;
        let tenant_id = self.tenant_id().as_u64();

        // The violated constraint by name, so the DLQ entry records the real
        // collection and field. An unresolved name records a
        // ManualIntervention entry under a placeholder CHECK constraint.
        let resolved = self
            .constraints_for_collection(collection)
            .into_iter()
            .find(|c| c.name == violation.constraint_name);
        let (constraint, hint) = match resolved {
            Some(constraint) => (constraint, violation.hint.clone()),
            None => (
                nodedb_crdt::Constraint {
                    name: violation.constraint_name.clone(),
                    collection: collection.to_string(),
                    field: String::new(),
                    kind: nodedb_crdt::ConstraintKind::Check {
                        expr: String::new(),
                        description: "unresolved constraint".to_string(),
                    },
                },
                nodedb_crdt::CompensationHint::ManualIntervention {
                    reason: violation.reason.clone(),
                },
            ),
        };
        let enqueued = self
            .validator
            .dlq_mut()
            .enqueue(nodedb_crdt::EnqueueDeadLetterArgs {
                peer_id,
                user_id,
                tenant_id,
                delta: delta.to_vec(),
                constraint: &constraint,
                reason: violation.reason.clone(),
                hint,
            });
        match enqueued {
            Ok(id) => {
                self.last_dead_letter = Some(id);
                ValidatedApplyOutcome::Rejected(wire)
            }
            Err(error) => {
                crate::diag::crdt_dead_letter_not_enqueued(
                    &error,
                    tenant_id,
                    collection,
                    &constraint.name,
                );
                ValidatedApplyOutcome::DeadLetterRefused {
                    violation: wire,
                    error,
                }
            }
        }
    }
}

/// Name the dead-letter entry of a delta that breaks the collection shape.
const ROW_SHAPE_CONSTRAINT: &str = "crdt_row_shape";

/// The container or row a shape fault names, as `collection/row` for a row.
fn shape_fault_target(fault: &nodedb_crdt::CrdtError) -> String {
    match fault {
        nodedb_crdt::CrdtError::NonMapRootContainer { container, .. } => container.clone(),
        nodedb_crdt::CrdtError::NonMapRowValue {
            collection, row_id, ..
        } => format!("{collection}/{row_id}"),
        other => other.to_string(),
    }
}

/// Translate an internal [`Violation`] into the deterministic wire
/// [`ViolationType`] by matching the internal compensation hint.
///
/// `ViolationType` is `#[non_exhaustive]`; any hint we do not model maps to the
/// generic `ConstraintViolation` carrying the human-readable reason.
fn violation_to_type(violation: &Violation) -> ViolationType {
    use nodedb_crdt::CompensationHint;
    match &violation.hint {
        CompensationHint::RetryWithDifferentValue {
            field,
            conflicting_value,
            ..
        } => ViolationType::UniqueViolation {
            field: field.clone(),
            value: conflicting_value.clone(),
        },
        CompensationHint::CreateReferencedRow { ref_key, .. } => ViolationType::ForeignKeyMissing {
            referenced_id: ref_key.clone(),
        },
        CompensationHint::ProvideRequiredField { field } => ViolationType::NotNullViolation {
            field: field.clone(),
        },
        CompensationHint::DeleteThenRetry { .. } | CompensationHint::ManualIntervention { .. } => {
            ViolationType::ConstraintViolation {
                detail: violation.reason.clone(),
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use loro::LoroValue;
    use nodedb_crdt::CompensationHint;
    use nodedb_crdt::constraint::ConstraintSet;
    use nodedb_crdt::policy::CollectionPolicy;
    use nodedb_crdt::state::CrdtState;
    use nodedb_crdt::validator::Violation;

    use super::*;
    use crate::types::TenantId;

    fn unique_engine() -> TenantCrdtEngine {
        let mut cs = ConstraintSet::new();
        cs.add_unique("users_email_unique", "users", "email");
        let mut engine = TenantCrdtEngine::new(TenantId::new(1), 0, cs).unwrap();
        // Strict policy so a UNIQUE clash escalates to a rejection rather than
        // auto-resolving.
        engine.set_collection_policy_typed("users", CollectionPolicy::strict());
        engine
    }

    /// Build a delta that writes a single row with the given fields.
    fn row_delta(peer: u64, row_id: &str, email: &str, name: &str) -> Vec<u8> {
        let state = CrdtState::new(peer).unwrap();
        state
            .upsert(
                "users",
                row_id,
                &[
                    ("email", LoroValue::String(email.into())),
                    ("name", LoroValue::String(name.into())),
                ],
            )
            .unwrap();
        state.export_snapshot().unwrap()
    }

    /// A raw Loro snapshot built by `write` on a fresh document.
    fn raw_delta(peer: u64, write: impl FnOnce(&loro::LoroDoc)) -> Vec<u8> {
        let doc = loro::LoroDoc::new();
        doc.set_peer_id(peer).unwrap();
        write(&doc);
        doc.export(loro::ExportMode::Snapshot).unwrap()
    }

    #[test]
    fn a_scalar_row_is_rejected_as_a_schema_violation() {
        let mut engine = unique_engine();
        let delta = raw_delta(5, |doc| {
            doc.get_map("users").insert("s1", 5).unwrap();
        });
        let outcome =
            engine.apply_committed_delta_validated("users", &delta, ApplyTarget::Collection, 5);
        match outcome {
            ValidatedApplyOutcome::Rejected(ViolationType::SchemaViolation { field, reason }) => {
                assert_eq!(field, "users/s1");
                assert!(reason.contains("s1"), "{reason}");
            }
            other => panic!("expected SchemaViolation, got {other:?}"),
        }
        assert_eq!(engine.dlq_len(), 1);
        assert!(!engine.row_exists("users", "s1"));
    }

    #[test]
    fn a_root_text_delta_is_rejected_as_a_schema_violation() {
        let mut engine = unique_engine();
        let delta = raw_delta(6, |doc| {
            doc.get_text("notes").insert(0, "hi").unwrap();
        });
        let outcome =
            engine.apply_committed_delta_validated("users", &delta, ApplyTarget::Collection, 6);
        match outcome {
            ValidatedApplyOutcome::Rejected(ViolationType::SchemaViolation { field, .. }) => {
                assert_eq!(field, "notes");
            }
            other => panic!("expected SchemaViolation, got {other:?}"),
        }
    }

    #[test]
    fn valid_delta_is_clean() {
        let mut engine = unique_engine();
        let delta = row_delta(2, "a", "x@y.com", "A");
        let outcome = engine.apply_committed_delta_validated(
            "users",
            &delta,
            ApplyTarget::Document {
                document_id: "a",
                surrogate: nodedb_types::Surrogate::new(1),
            },
            2,
        );
        assert!(matches!(outcome, ValidatedApplyOutcome::Clean { .. }));
        assert!(engine.row_exists("users", "a"));
        assert_eq!(engine.dlq_len(), 0);
    }

    /// A snapshot import targets the collection: it writes every row it
    /// carries, where a document target refuses the same bytes.
    #[test]
    fn a_collection_target_imports_every_row_a_document_target_refuses() {
        let state = CrdtState::new(9).unwrap();
        for (row, email) in [("a", "a@y.com"), ("b", "b@y.com")] {
            state
                .upsert("users", row, &[("email", LoroValue::String(email.into()))])
                .unwrap();
        }
        let snapshot = state.export_snapshot().unwrap();

        let mut engine = unique_engine();
        let refused = engine.apply_committed_delta_validated(
            "users",
            &snapshot,
            ApplyTarget::Document {
                document_id: "a",
                surrogate: nodedb_types::Surrogate::new(1),
            },
            9,
        );
        assert!(matches!(refused, ValidatedApplyOutcome::Malformed));
        assert!(!engine.row_exists("users", "a"));

        let imported =
            engine.apply_committed_delta_validated("users", &snapshot, ApplyTarget::Collection, 9);
        assert!(matches!(imported, ValidatedApplyOutcome::Clean { .. }));
        assert!(engine.row_exists("users", "a") && engine.row_exists("users", "b"));
    }

    /// A multi-row frame is rejected before its detached candidate can replace
    /// authoritative state.
    #[test]
    fn multi_doc_delta_does_not_mutate_authoritative_state() {
        let mut engine = unique_engine();
        // One Loro delta that writes two distinct rows.
        let state = CrdtState::new(7).unwrap();
        state
            .upsert(
                "users",
                "a",
                &[("email", LoroValue::String("a@y.com".into()))],
            )
            .unwrap();
        state
            .upsert(
                "users",
                "b",
                &[("email", LoroValue::String("b@y.com".into()))],
            )
            .unwrap();
        let delta = state.export_snapshot().unwrap();

        // Frame claims only row "a".
        let outcome = engine.apply_committed_delta_validated(
            "users",
            &delta,
            ApplyTarget::Document {
                document_id: "a",
                surrogate: nodedb_types::Surrogate::new(2),
            },
            7,
        );
        assert!(matches!(outcome, ValidatedApplyOutcome::Malformed));
        assert!(!engine.row_exists("users", "a"));
        assert!(!engine.row_exists("users", "b"));
    }

    /// A peer whose local CRDT document spans several collections exports each
    /// delta as an incremental slice of ONE shared oplog. Origin keeps one Loro
    /// document PER COLLECTION, so a delta for collection `probe` can causally
    /// depend on operations that were routed into a different collection's
    /// document and are therefore absent here.
    ///
    /// Loro accepts such a delta and buffers its operations as causally
    /// pending: the import returns success, but the applied state never
    /// advances and the row is never written. That MUST NOT be reported as a
    /// clean apply — a clean apply means the caller may advance the
    /// high-water-mark and acknowledge the write as durable, which turns a
    /// deferred operation into permanent, silent data loss.
    #[test]
    fn causally_pending_delta_is_not_clean() {
        let mut engine = unique_engine();

        // Peer-local document spanning two collections, exporting one
        // incremental delta per write — the shape every multi-collection
        // embedded client produces.
        let peer = CrdtState::new(11).unwrap();

        let v0 = peer.oplog_version_vector();
        peer.upsert(
            "users",
            "u1",
            &[("email", LoroValue::String("u1@y.com".into()))],
        )
        .unwrap();
        let delta_u1 = peer.export_updates_since(&v0).unwrap();

        let v1 = peer.oplog_version_vector();
        peer.upsert("signals", "s1", &[("value", LoroValue::String("v".into()))])
            .unwrap();
        let _delta_s1 = peer.export_updates_since(&v1).unwrap();

        let v2 = peer.oplog_version_vector();
        peer.upsert(
            "users",
            "u2",
            &[("email", LoroValue::String("u2@y.com".into()))],
        )
        .unwrap();
        let delta_u2 = peer.export_updates_since(&v2).unwrap();

        // The first `users` delta has no missing predecessor and applies.
        let first = engine.apply_committed_delta_validated(
            "users",
            &delta_u1,
            ApplyTarget::Document {
                document_id: "u1",
                surrogate: nodedb_types::Surrogate::new(3),
            },
            11,
        );
        assert!(matches!(first, ValidatedApplyOutcome::Clean { .. }));
        assert!(engine.row_exists("users", "u1"));

        // The second `users` delta depends on the intervening `signals`
        // operation, which lives in a different document on this side.
        let second = engine.apply_committed_delta_validated(
            "users",
            &delta_u2,
            ApplyTarget::Document {
                document_id: "u2",
                surrogate: nodedb_types::Surrogate::new(4),
            },
            11,
        );

        // The load-bearing assertion: an apply that did not actually write the
        // row must not be indistinguishable from one that did.
        assert!(
            !matches!(second, ValidatedApplyOutcome::Clean { .. }),
            "a delta whose operations stayed causally pending must not report a \
             clean apply; got {second:?}"
        );

        // Guard against the specific silent-loss shape: reporting
        // `Clean { write_set: [] }` passes every downstream contract check
        // (an empty write-set is a legal no-op delete) while dropping the row.
        if let ValidatedApplyOutcome::Clean { write_set, .. } = &second {
            assert!(
                !write_set.is_empty(),
                "clean apply with an empty write-set silently drops the row"
            );
        }
    }

    /// A delta must never be reported `Clean` while the row it claimed to
    /// write is absent. Paired with [`causally_pending_delta_is_not_clean`],
    /// this pins the observable consequence rather than only the outcome enum:
    /// whatever remedy the apply path chooses, "reported clean" and "row
    /// readable" must agree.
    ///
    /// This models a peer that keeps ONE document for several collections and
    /// therefore emits deltas that are not self-contained. Such a delta cannot
    /// be materialized here — its predecessors live in another document — so
    /// the correct outcome is a loud refusal, not a clean apply over a missing
    /// row. A peer that keeps one document per collection emits self-contained
    /// deltas and is covered end to end on the client side.
    #[test]
    fn causally_pending_delta_does_not_lose_its_row() {
        let mut engine = unique_engine();
        let peer = CrdtState::new(12).unwrap();

        let v0 = peer.oplog_version_vector();
        peer.upsert(
            "users",
            "a",
            &[("email", LoroValue::String("a@y.com".into()))],
        )
        .unwrap();
        let delta_a = peer.export_updates_since(&v0).unwrap();

        let v1 = peer.oplog_version_vector();
        peer.upsert("audit", "e1", &[("op", LoroValue::String("w".into()))])
            .unwrap();

        let v2 = peer.oplog_version_vector();
        peer.upsert(
            "users",
            "b",
            &[("email", LoroValue::String("b@y.com".into()))],
        )
        .unwrap();
        let delta_b = peer.export_updates_since(&v2).unwrap();
        let _ = v1;

        engine.apply_committed_delta_validated(
            "users",
            &delta_a,
            ApplyTarget::Document {
                document_id: "a",
                surrogate: nodedb_types::Surrogate::new(5),
            },
            12,
        );
        let outcome = engine.apply_committed_delta_validated(
            "users",
            &delta_b,
            ApplyTarget::Document {
                document_id: "b",
                surrogate: nodedb_types::Surrogate::new(6),
            },
            12,
        );

        let reported_clean = matches!(outcome, ValidatedApplyOutcome::Clean { .. });
        assert_eq!(
            reported_clean,
            engine.row_exists("users", "b"),
            "apply reported clean={reported_clean} but row_exists={}; a clean \
             apply must leave its row readable and an unreadable row must not \
             be reported clean",
            engine.row_exists("users", "b")
        );
    }

    /// Two replicas claiming the same Loro peer id write into overlapping
    /// `(peer, counter)` ranges. The CRDT merge trims the second replica's
    /// operations as already-known, so its row is discarded while the import
    /// itself succeeds.
    ///
    /// The apply cannot refuse this — the same trim is what makes an honest
    /// resync idempotent — but it must not report the delta as having applied
    /// anything. `imported_ops == 0` is the fact that separates "this delta put
    /// the operations there" from "they were already there", and it is the only
    /// thing standing between a collision and an `Applied` ack.
    #[test]
    fn a_delta_trimmed_by_a_colliding_peer_reports_no_imported_operations() {
        let mut engine = unique_engine();

        let delta_a = row_delta(1, "a", "a@y.com", "A");
        let first = engine.apply_committed_delta_validated(
            "users",
            &delta_a,
            ApplyTarget::Document {
                document_id: "a",
                surrogate: nodedb_types::Surrogate::new(7),
            },
            1,
        );
        match first {
            ValidatedApplyOutcome::Clean { imported_ops, .. } => {
                assert!(imported_ops > 0, "the first delta genuinely applied")
            }
            other => panic!("expected a clean apply, got {other:?}"),
        }

        // A fresh replica reusing peer id 1 restarts its counters at 0.
        let delta_b = row_delta(1, "b", "b@y.com", "B");
        let second = engine.apply_committed_delta_validated(
            "users",
            &delta_b,
            ApplyTarget::Document {
                document_id: "b",
                surrogate: nodedb_types::Surrogate::new(8),
            },
            1,
        );
        match second {
            ValidatedApplyOutcome::Clean {
                write_set,
                imported_ops,
            } => {
                assert_eq!(
                    imported_ops, 0,
                    "a fully-trimmed delta imported nothing; reporting otherwise is \
                     what turns a peer-id collision into a silent loss"
                );
                assert!(write_set.is_empty());
            }
            other => panic!("expected a clean, zero-import apply, got {other:?}"),
        }
        assert!(
            !engine.row_exists("users", "b"),
            "this test only means something while the row is genuinely lost"
        );
    }

    /// The counterpart that keeps the zero-import signal honest: a delta that
    /// really does write its row must report a non-zero import, so
    /// `imported_ops == 0` never fires on a healthy apply.
    #[test]
    fn a_delta_that_writes_its_row_reports_imported_operations() {
        let mut engine = unique_engine();
        let delta = row_delta(4, "solo", "solo@y.com", "S");
        match engine.apply_committed_delta_validated(
            "users",
            &delta,
            ApplyTarget::Document {
                document_id: "solo",
                surrogate: nodedb_types::Surrogate::new(9),
            },
            4,
        ) {
            ValidatedApplyOutcome::Clean {
                write_set,
                imported_ops,
            } => {
                assert!(imported_ops > 0);
                assert_eq!(write_set, vec![("users".into(), "solo".into())]);
            }
            other => panic!("expected a clean apply, got {other:?}"),
        }
        assert!(engine.row_exists("users", "solo"));
    }

    #[test]
    fn unique_dup_is_rejected_and_dlqd() {
        let mut engine = unique_engine();
        // Seed row A.
        let delta_a = row_delta(2, "a", "x@y.com", "A");
        let clean = engine.apply_committed_delta_validated(
            "users",
            &delta_a,
            ApplyTarget::Document {
                document_id: "a",
                surrogate: nodedb_types::Surrogate::new(10),
            },
            2,
        );
        assert!(matches!(clean, ValidatedApplyOutcome::Clean { .. }));

        // Row B reuses A's email — UNIQUE violation.
        let delta_b = row_delta(3, "b", "x@y.com", "B");
        let outcome = engine.apply_committed_delta_validated(
            "users",
            &delta_b,
            ApplyTarget::Document {
                document_id: "b",
                surrogate: nodedb_types::Surrogate::new(11),
            },
            3,
        );
        match outcome {
            ValidatedApplyOutcome::Rejected(ViolationType::UniqueViolation { field, value }) => {
                assert_eq!(field, "email");
                assert_eq!(value, "x@y.com");
            }
            other => panic!("expected UniqueViolation, got {other:?}"),
        }
        assert_eq!(engine.dlq_len(), 1);
        assert!(
            engine.read_row("users", "b").is_none(),
            "constraint-rejected delta must not mutate authoritative state"
        );
    }

    /// A rejected delta the full DLQ refuses surfaces as an error carrying
    /// the DLQ failure. Nothing applies and nothing is left to bind.
    #[test]
    fn a_rejection_the_full_dlq_refuses_surfaces_as_an_error() {
        let mut engine = unique_engine();
        let seed = row_delta(2, "a", "x@y.com", "A");
        let seeded = engine.apply_committed_delta_validated(
            "users",
            &seed,
            ApplyTarget::Document {
                document_id: "a",
                surrogate: nodedb_types::Surrogate::new(10),
            },
            2,
        );
        assert!(matches!(seeded, ValidatedApplyOutcome::Clean { .. }));
        let capacity = engine.fill_dead_letter_queue_for_test();
        assert!(capacity > 0);

        let duplicate = row_delta(3, "b", "x@y.com", "B");
        let outcome = engine.apply_committed_delta_validated(
            "users",
            &duplicate,
            ApplyTarget::Document {
                document_id: "b",
                surrogate: nodedb_types::Surrogate::new(11),
            },
            3,
        );
        match outcome {
            ValidatedApplyOutcome::DeadLetterRefused {
                violation: ViolationType::UniqueViolation { field, .. },
                error: nodedb_crdt::CrdtError::DlqFull { .. },
            } => assert_eq!(field, "email"),
            other => panic!("expected DeadLetterRefused(DlqFull), got {other:?}"),
        }
        assert_eq!(engine.dlq_len(), capacity, "the refused entry is not queued");
        assert!(engine.read_row("users", "b").is_none());
        assert!(
            engine.bind_dead_letter_source(12).is_none(),
            "a refused apply leaves no entry to bind"
        );
    }

    /// The load-bearing safety property of retaining a candidate across
    /// applies: the candidate becomes authoritative on a clean apply, so a
    /// candidate that missed a local write would silently erase it.
    ///
    /// Nothing tells the candidate a local write happened — `doc_upsert` goes
    /// straight to `state_mut`. It is caught by the candidate no longer
    /// matching the state it was cloned from.
    #[test]
    fn a_local_write_between_applies_is_not_erased_by_the_candidate() {
        let mut engine = unique_engine();

        // The first apply creates the collection, so there is no displaced
        // document to keep; the second leaves a candidate behind.
        let delta_a = row_delta(2, "a", "a@y.com", "A");
        engine.apply_committed_delta_validated(
            "users",
            &delta_a,
            ApplyTarget::Document {
                document_id: "a",
                surrogate: nodedb_types::Surrogate::new(12),
            },
            2,
        );
        let delta_seed = row_delta(9, "seed", "seed@y.com", "S");
        engine.apply_committed_delta_validated(
            "users",
            &delta_seed,
            ApplyTarget::Document {
                document_id: "seed",
                surrogate: nodedb_types::Surrogate::new(13),
            },
            9,
        );
        assert_eq!(engine.apply_candidate_count(), 1, "candidate is retained");

        // A local write the candidate knows nothing about.
        engine
            .doc_upsert(
                "users",
                "local",
                &[("email", LoroValue::String("local@y.com".into()))],
            )
            .expect("local write");

        // Second apply must not install a candidate that predates it.
        let delta_b = row_delta(3, "b", "b@y.com", "B");
        let outcome = engine.apply_committed_delta_validated(
            "users",
            &delta_b,
            ApplyTarget::Document {
                document_id: "b",
                surrogate: nodedb_types::Surrogate::new(14),
            },
            3,
        );

        assert!(matches!(outcome, ValidatedApplyOutcome::Clean { .. }));
        assert!(
            engine.row_exists("users", "local"),
            "the local write was erased by a stale validation candidate"
        );
        assert!(engine.row_exists("users", "a"));
        assert!(engine.row_exists("users", "b"));
    }

    /// A refused delta leaves its operations inside the candidate — Loro
    /// buffers even causally-pending ones — so the candidate must be discarded
    /// rather than handed to the next delta.
    #[test]
    fn a_rejected_delta_does_not_poison_the_next_apply() {
        let mut engine = unique_engine();
        let seed = row_delta(2, "a", "x@y.com", "A");
        engine.apply_committed_delta_validated(
            "users",
            &seed,
            ApplyTarget::Document {
                document_id: "a",
                surrogate: nodedb_types::Surrogate::new(15),
            },
            2,
        );

        // Rejected: reuses row a's email under a strict UNIQUE policy.
        let dup = row_delta(3, "b", "x@y.com", "B");
        let rejected = engine.apply_committed_delta_validated(
            "users",
            &dup,
            ApplyTarget::Document {
                document_id: "b",
                surrogate: nodedb_types::Surrogate::new(16),
            },
            3,
        );
        assert!(matches!(rejected, ValidatedApplyOutcome::Rejected(_)));
        assert_eq!(
            engine.apply_candidate_count(),
            0,
            "a candidate holding refused operations must not be kept"
        );

        // The next delta must see a clean base: neither row b nor its email.
        let next = row_delta(4, "c", "c@y.com", "C");
        let outcome = engine.apply_committed_delta_validated(
            "users",
            &next,
            ApplyTarget::Document {
                document_id: "c",
                surrogate: nodedb_types::Surrogate::new(17),
            },
            4,
        );
        assert!(matches!(outcome, ValidatedApplyOutcome::Clean { .. }));
        assert!(engine.row_exists("users", "c"));
        assert!(
            !engine.row_exists("users", "b"),
            "the refused row resurfaced through a reused candidate"
        );
    }

    /// A run of applies reuses one candidate rather than copying the
    /// collection per delta, and releasing it is what stops the second
    /// document outliving the run.
    #[test]
    fn a_run_of_applies_reuses_one_candidate() {
        let mut engine = unique_engine();
        for i in 0..8 {
            let delta = row_delta(
                10 + i,
                &format!("r{i}"),
                &format!("r{i}@y.com"),
                &format!("R{i}"),
            );
            let outcome = engine.apply_committed_delta_validated(
                "users",
                &delta,
                ApplyTarget::Document {
                    document_id: &format!("r{i}"),
                    surrogate: nodedb_types::Surrogate::new(18),
                },
                10 + i,
            );
            assert!(
                matches!(outcome, ValidatedApplyOutcome::Clean { .. }),
                "delta {i} should apply cleanly, got {outcome:?}"
            );
        }
        for i in 0..8 {
            assert!(engine.row_exists("users", &format!("r{i}")));
        }
        assert_eq!(
            engine.apply_candidate_count(),
            1,
            "one candidate serves the whole run"
        );

        engine.clear_apply_candidates();
        assert_eq!(engine.apply_candidate_count(), 0);
    }

    /// A collection first created by a delta apply must get the same derived
    /// peer id `state_mut` would have given it. The node's base peer id mints
    /// operation identities that collide across collections, which is the
    /// silent row-drop `collection_peer_id` exists to prevent.
    #[test]
    fn a_collection_created_by_apply_gets_the_derived_peer_id() {
        let mut engine = unique_engine();
        let delta = row_delta(5, "a", "a@y.com", "A");
        engine.apply_committed_delta_validated(
            "users",
            &delta,
            ApplyTarget::Document {
                document_id: "a",
                surrogate: nodedb_types::Surrogate::new(19),
            },
            5,
        );

        let expected = TenantCrdtEngine::collection_peer_id(engine.peer_id(), "users");
        assert_eq!(
            engine.collection_peer_id_for_test("users"),
            Some(expected),
            "a collection born from a delta apply must carry the derived peer \
             id, not the node's base id"
        );
    }

    #[test]
    fn corrupt_delta_is_malformed() {
        let mut engine = unique_engine();
        let outcome = engine.apply_committed_delta_validated(
            "users",
            b"not a valid loro snapshot",
            ApplyTarget::Document {
                document_id: "z",
                surrogate: nodedb_types::Surrogate::new(20),
            },
            9,
        );
        assert!(matches!(outcome, ValidatedApplyOutcome::Malformed));
        assert_eq!(engine.dlq_len(), 0);
    }

    fn violation_with(hint: CompensationHint) -> Violation {
        Violation {
            constraint_name: "c".into(),
            reason: "boom".into(),
            hint,
        }
    }

    #[test]
    fn translator_maps_each_hint() {
        assert_eq!(
            violation_to_type(&violation_with(CompensationHint::RetryWithDifferentValue {
                field: "email".into(),
                conflicting_value: "x".into(),
                suggestion: "x2".into(),
            })),
            ViolationType::UniqueViolation {
                field: "email".into(),
                value: "x".into(),
            }
        );
        assert_eq!(
            violation_to_type(&violation_with(CompensationHint::CreateReferencedRow {
                ref_collection: "orgs".into(),
                ref_key: "org-7".into(),
                missing_value: "org-7".into(),
            })),
            ViolationType::ForeignKeyMissing {
                referenced_id: "org-7".into(),
            }
        );
        assert_eq!(
            violation_to_type(&violation_with(CompensationHint::ProvideRequiredField {
                field: "name".into(),
            })),
            ViolationType::NotNullViolation {
                field: "name".into(),
            }
        );
        assert_eq!(
            violation_to_type(&violation_with(CompensationHint::ManualIntervention {
                reason: "nope".into(),
            })),
            ViolationType::ConstraintViolation {
                detail: "boom".into(),
            }
        );
        assert_eq!(
            violation_to_type(&violation_with(CompensationHint::DeleteThenRetry {
                collection: "users".into(),
                conflicting_key: "a".into(),
            })),
            ViolationType::ConstraintViolation {
                detail: "boom".into(),
            }
        );
    }

    /// Contract guard: the peer-delta apply module stays deterministic.
    ///
    /// The Raft-committed apply path (`apply_committed_delta_validated` →
    /// pure `Validator::validate`) must never reach for the local write
    /// path's signed/seq-gated check (the `validate` + `or_reject` helper on
    /// `core.rs`), which is nondeterministic per replica (SystemTime + HMAC
    /// signature + seq monotonicity). Pinning this to the source stops a
    /// future edit from silently diverging replicas at identical log indices.
    #[test]
    fn apply_module_stays_deterministic() {
        const SRC: &str = include_str!("apply_validated.rs");
        // Concatenated so this test's own source carries no contiguous token
        // that would self-match the guard.
        let forbidden = concat!("validate", "_or_", "reject");
        assert!(
            !SRC.contains(forbidden),
            "apply_validated.rs must not reference the local write path's \
             signed/seq-gated check — the Raft-applied peer-delta path must \
             stay deterministic (pure Validator::validate only)"
        );
    }
}
