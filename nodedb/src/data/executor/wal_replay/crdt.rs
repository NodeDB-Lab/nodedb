// SPDX-License-Identifier: BUSL-1.1

//! CRDT WAL replay: rebuilds authoritative Loro and sparse document state after crash.

use crate::data::executor::core_loop::CoreLoop;

impl CoreLoop {
    /// Try to replay one WAL CRDT delta record.
    ///
    /// This single-record entrypoint lets the startup coordinator preserve the
    /// global LSN order across CRDT delta and intent record classes. The bulk
    /// replayer is the implementation, so both entrypoints share one decode,
    /// tombstone, fence, and projection path.
    pub(in crate::data::executor) fn try_replay_crdt_delta(
        &mut self,
        record: &nodedb_wal::WalRecord,
        num_cores: usize,
        tombstones: &nodedb_wal::TombstoneSet,
    ) -> Option<usize> {
        use nodedb_wal::record::RecordType;

        if RecordType::from_raw(record.logical_record_type()) != Some(RecordType::CrdtDelta) {
            return None;
        }
        self.replay_crdt_wal(std::slice::from_ref(record), num_cores, tombstones);
        Some(1)
    }

    /// Replay WAL CRDT delta records to rebuild CRDT state after crash.
    ///
    /// A document delta rebuilds both the authoritative Loro row and its sparse
    /// document projection under the surrogate the record carries. A snapshot
    /// import carries no row identity and rebuilds Loro state only. A payload
    /// that decodes to neither is refused as unapplied. CRDT records use
    /// `RecordType::CrdtDelta`; the payload is a
    /// `CrdtDeltaWalPayload` as written by `append_crdt_delta` for both
    /// `CrdtOp::Apply` and `CrdtOp::ImportSnapshot`. Loro `import` is
    /// idempotent and commutative, so there is no LSN gate: re-importing a
    /// delta already folded into a loaded checkpoint is a safe no-op.
    ///
    /// Collection lifecycle tombstones are external to Loro and must suppress
    /// older deltas so a hard-purged collection cannot be resurrected.
    pub fn replay_crdt_wal(
        &mut self,
        records: &[nodedb_wal::WalRecord],
        num_cores: usize,
        tombstones: &nodedb_wal::TombstoneSet,
    ) {
        use nodedb_wal::record::RecordType;
        use tracing::{debug, warn};

        let mut replayed = 0usize;

        for record in records {
            if self.replay_halted() {
                break;
            }
            if RecordType::from_raw(record.logical_record_type()) != Some(RecordType::CrdtDelta) {
                continue;
            }

            // Route to the correct core by vShard.
            let vshard_id = record.header.vshard_id as usize;
            let target_core = if num_cores > 0 {
                vshard_id % num_cores
            } else {
                0
            };
            if target_core != self.core_id {
                continue;
            }

            let tid = crate::types::TenantId::new(record.header.tenant_id);

            // Single self-describing decode. The delta is routed to its
            // per-collection LoroDoc by `payload.collection`.
            let payload = match crate::wal::CrdtDeltaWalPayload::decode(&record.payload) {
                Ok(payload) => payload,
                Err(error) => {
                    self.replay_record_unapplied(
                        "crdt",
                        "delta_decode",
                        record.header.lsn,
                        &error.to_string(),
                    );
                    continue;
                }
            };
            let collection = payload.collection.as_str();
            if tombstones.is_tombstoned(
                record.header.database_id,
                tid.as_u64(),
                collection,
                record.header.lsn,
            ) {
                continue;
            }

            let database_id = crate::types::DatabaseId::new(record.header.database_id);
            // The live apply never reads this metadata, so it applied the
            // delta. A record its writer cannot produce halts replay: skipping
            // it drops a committed delta.
            if let Err(error) = signed_record_metadata(&payload, record.header.lsn) {
                self.replay_record_unapplied(
                    "crdt",
                    "signing_metadata",
                    record.header.lsn,
                    &error.to_string(),
                );
                continue;
            }
            if let Some(expected) = payload.expected_frontier_digest {
                let actual = self
                    .crdt_engines
                    .get(&(database_id, tid))
                    .map(|engine| engine.frontier_digest(database_id, collection))
                    .unwrap_or_else(|| {
                        nodedb_crdt::state::frontier_digest::domain_frontier_digest(
                            tid.as_u64(),
                            database_id.as_u64(),
                            collection,
                            None,
                        )
                    });
                if actual != expected {
                    // Replay rebuilds the collection in LSN order, so the
                    // frontier here is the one the live apply fenced against.
                    // The live apply refused the delta, applied nothing, and
                    // held the stream's mark so the sender's re-push is
                    // admitted. Replay skips it and holds the mark the same way.
                    debug!(
                        core = self.core_id,
                        tenant = tid.as_u64(),
                        %collection,
                        lsn = record.header.lsn,
                        "replay skips a CRDT delta its live apply refused at the frontier fence"
                    );
                    continue;
                }
            }

            // Opening the engine restores the tenant's stored dead-letter
            // entries. A record one of them names was rejected by its live
            // apply, which stored the entry and changed no state, so it
            // replays as that rejection. Applying it again would enqueue a
            // second entry, which a queue full of restored entries refuses.
            let already_rejected = match self.get_crdt_engine(database_id, tid) {
                Ok(engine) => engine.dead_letter_recorded(record.header.lsn),
                Err(error) => {
                    self.replay_record_unapplied(
                        "crdt",
                        "engine_open",
                        record.header.lsn,
                        &error.to_string(),
                    );
                    continue;
                }
            };

            let projection = match &payload.target {
                _ if already_rejected => None,
                crate::wal::CrdtDeltaTarget::Document {
                    document_id,
                    surrogate,
                } => {
                    let surrogate = *surrogate;
                    let applied = match self.get_crdt_engine(database_id, tid) {
                        Ok(engine) => engine.apply_committed_delta_authenticated(
                            collection,
                            &payload.bytes,
                            crate::engine::crdt::tenant_state::ApplyTarget::Document {
                                document_id,
                                surrogate,
                            },
                            payload.peer_id,
                            replayed_admission(&payload),
                        ),
                        Err(error) => {
                            self.replay_record_unapplied(
                                "crdt",
                                "engine_open",
                                record.header.lsn,
                                &error.to_string(),
                            );
                            continue;
                        }
                    };
                    match applied {
                        crate::engine::crdt::tenant_state::ValidatedApplyOutcome::Clean {
                            write_set,
                            ..
                        } => {
                            // The engine refuses a delta writing any row but
                            // its target before it installs, so a clean apply
                            // wrote the target alone. A wider write set is an
                            // engine invariant broken after Loro installed the
                            // delta: skipping its projection drops rows.
                            if let Err(error) =
                                Self::single_document_write_set(collection, document_id, &write_set)
                            {
                                self.replay_record_unapplied(
                                    "crdt",
                                    "one_document_contract",
                                    record.header.lsn,
                                    &error.to_string(),
                                );
                                continue;
                            }
                            let Some(engine) = self.crdt_engines.get(&(database_id, tid)) else {
                                self.replay_record_unapplied(
                                    "crdt",
                                    "engine_missing",
                                    record.header.lsn,
                                    "the engine that applied the delta is gone before its row \
                                     projected",
                                );
                                continue;
                            };
                            Some((
                                document_id.as_str(),
                                surrogate,
                                Self::encode_crdt_row(engine, collection, document_id),
                            ))
                        }
                        crate::engine::crdt::tenant_state::ValidatedApplyOutcome::Rejected(
                            reason,
                        ) => {
                            // The detached candidate was rejected and discarded.
                            // The committed record remains a deterministic no-op
                            // whose collection floor advances on every replica.
                            warn!(core = self.core_id, tenant = tid.as_u64(), %collection, %reason, "CRDT WAL delta rejected during replay");
                            if !self.store_replayed_dead_letter(database_id, tid, record.header.lsn) {
                                continue;
                            }
                            None
                        }
                        crate::engine::crdt::tenant_state::ValidatedApplyOutcome::DeadLetterRefused {
                            violation,
                            error,
                        } => {
                            // Rejected, and the queue refused its entry, so
                            // nothing but this record holds the delta. Replay
                            // stops at the record: no checkpoint covers it, and
                            // boot refuses until the queue takes the entry.
                            self.replay_record_unapplied(
                                "crdt",
                                "dead_letter_refused",
                                record.header.lsn,
                                &format!(
                                    "delta for {collection} violates {violation}, and the \
                                     dead-letter queue refused it: {error}"
                                ),
                            );
                            continue;
                        }
                        crate::engine::crdt::tenant_state::ValidatedApplyOutcome::Malformed => {
                            // The bytes, their rows, or a missing required
                            // signature decide this, the same at replay as live.
                            // The live apply refused the delta, applied nothing,
                            // and advanced a sender's mark past it. Replay skips
                            // it and advances the mark the same way.
                            if let Some(provenance) = payload.provenance.as_ref() {
                                self.sync_commit(provenance);
                            }
                            debug!(core = self.core_id, tenant = tid.as_u64(), %collection, lsn = record.header.lsn, "replay skips a CRDT delta its live apply refused as malformed");
                            continue;
                        }
                        crate::engine::crdt::tenant_state::ValidatedApplyOutcome::PendingDependencies => {
                            // Replay rebuilds the collection in LSN order, so
                            // the predecessors are absent here exactly when they
                            // were absent live. The live apply applied nothing
                            // and held the sender's mark for the re-push.
                            // Replay skips the delta and holds the mark the
                            // same way.
                            debug!(
                                core = self.core_id,
                                tenant = tid.as_u64(),
                                %collection,
                                %document_id,
                                lsn = record.header.lsn,
                                "replay skips a CRDT delta its live apply held for missing \
                                 predecessors"
                            );
                            continue;
                        }
                        crate::engine::crdt::tenant_state::ValidatedApplyOutcome::CandidateUnavailable { error } => {
                            // This node failed, not the delta: skipping it
                            // drops a delta the live apply may have applied.
                            self.replay_record_unapplied(
                                "crdt",
                                "candidate_unavailable",
                                record.header.lsn,
                                &format!("no apply candidate for {collection}: {error}"),
                            );
                            continue;
                        }
                    }
                }
                crate::wal::CrdtDeltaTarget::Collection => {
                    // A per-collection snapshot import binds no row identity.
                    // It rebuilds Loro state only; each row's projection comes
                    // from the document records that carry its surrogate.
                    match self.get_crdt_engine(database_id, tid) {
                        Ok(engine) => match engine.apply_committed_delta_authenticated(
                            collection,
                            &payload.bytes,
                            crate::engine::crdt::tenant_state::ApplyTarget::Collection,
                            payload.peer_id,
                            replayed_admission(&payload),
                        ) {
                            crate::engine::crdt::tenant_state::ValidatedApplyOutcome::Clean {
                                ..
                            } => None,
                            crate::engine::crdt::tenant_state::ValidatedApplyOutcome::Rejected(
                                reason,
                            ) => {
                                warn!(core = self.core_id, tenant = tid.as_u64(), %collection, %reason, "CRDT WAL snapshot import rejected during replay");
                                if !self.store_replayed_dead_letter(database_id, tid, record.header.lsn) {
                                    continue;
                                }
                                None
                            }
                            // See the document arm: nothing but this record
                            // holds the delta, so replay stops at it.
                            crate::engine::crdt::tenant_state::ValidatedApplyOutcome::DeadLetterRefused {
                                violation,
                                error,
                            } => {
                                self.replay_record_unapplied(
                                    "crdt",
                                    "dead_letter_refused",
                                    record.header.lsn,
                                    &format!(
                                        "snapshot import for {collection} violates {violation}, \
                                         and the dead-letter queue refused it: {error}"
                                    ),
                                );
                                continue;
                            }
                            // See the document arm: the live import refused
                            // the snapshot on the same state, so replay skips it.
                            crate::engine::crdt::tenant_state::ValidatedApplyOutcome::Malformed => {
                                debug!(core = self.core_id, tenant = tid.as_u64(), %collection, lsn = record.header.lsn, "replay skips a CRDT snapshot import its live apply refused as malformed");
                                continue;
                            }
                            crate::engine::crdt::tenant_state::ValidatedApplyOutcome::PendingDependencies => {
                                debug!(core = self.core_id, tenant = tid.as_u64(), %collection, lsn = record.header.lsn, "replay skips a CRDT snapshot import its live apply refused for missing predecessors");
                                continue;
                            }
                            crate::engine::crdt::tenant_state::ValidatedApplyOutcome::CandidateUnavailable { error } => {
                                self.replay_record_unapplied(
                                    "crdt",
                                    "candidate_unavailable",
                                    record.header.lsn,
                                    &format!("no apply candidate for {collection}: {error}"),
                                );
                                continue;
                            }
                        },
                        Err(error) => {
                            self.replay_record_unapplied(
                                "crdt",
                                "engine_open",
                                record.header.lsn,
                                &error.to_string(),
                            );
                            continue;
                        }
                    }
                }
            };

            if let Some((document_id, surrogate, Some(bytes))) = projection {
                let task = Self::replay_task(
                    tid,
                    database_id,
                    crate::types::VShardId::new(record.header.vshard_id),
                    nodedb_physical::physical_plan::PhysicalPlan::Crdt(
                        nodedb_physical::physical_plan::CrdtOp::ImportSnapshot {
                            tenant_id: tid.as_u64(),
                            collection: nodedb_types::QualifiedCollection::from_stored(
                                collection.to_owned(),
                            ),
                            bytes: Vec::new(),
                        },
                    ),
                    Some(crate::types::Lsn::new(record.header.lsn)),
                );
                self.materialize_synced_document(
                    &task,
                    tid.as_u64(),
                    collection,
                    document_id,
                    surrogate,
                    &bytes,
                );
            }
            if let Some(provenance) = payload.provenance.as_ref() {
                self.sync_commit(provenance);
            }
            // Every successfully imported payload changed authoritative Loro
            // state, including a snapshot import that projects no row. Record
            // its exact durable collection floor either way.
            self.note_replay_write(
                record.header.database_id,
                tid.as_u64(),
                collection,
                None,
                record.header.lsn,
            );
            replayed += 1;
        }

        // Replay is the longest run of deltas the engine ever sees, and each
        // apply validates into a copy of its collection. Holding that copy
        // across the run is what keeps recovery linear in the number of
        // deltas instead of quadratic; it has no reason to outlive the run.
        self.release_crdt_apply_candidates();

        if replayed > 0 {
            tracing::info!(core = self.core_id, replayed, "WAL CRDT replay complete");
        }
    }
}

/// Check the admission metadata of a signed record against what its writer
/// stamps. The writer copies the session's producer id and the frame's seq
/// into both the provenance and the signing fields, so a signed record
/// without provenance, or with fields that disagree, is not one it wrote.
/// Such a record at `lsn` is a corrupt WAL record.
fn signed_record_metadata(
    payload: &crate::wal::CrdtDeltaWalPayload,
    lsn: u64,
) -> crate::Result<()> {
    let corrupt =
        |detail: String| crate::Error::Wal(nodedb_wal::WalError::CorruptRecord { lsn, detail });
    let Some(signing) = payload.signing else {
        return Ok(());
    };
    let Some(provenance) = payload.provenance.as_ref() else {
        return Err(corrupt(format!(
            "signed delta for {} carries no provenance",
            payload.collection
        )));
    };
    if provenance.producer_id != signing.auth_device_id || provenance.seq != signing.auth_seq_no {
        return Err(corrupt(format!(
            "signed delta for {} names producer {} seq {} in its provenance and device {} seq {} \
             in its signing fields",
            payload.collection,
            provenance.producer_id,
            provenance.seq,
            signing.auth_device_id,
            signing.auth_seq_no
        )));
    }
    Ok(())
}

/// The signing admission a committed record replays under: the one its live
/// apply ran under.
///
/// A signed record carries the signing fields the session admitted, already
/// verified, so it replays preverified. An absent required signature still
/// refuses it, as it did live. An unsigned record replays the live local
/// apply's admission: no signature, checked against the collection's signing
/// policy.
fn replayed_admission(
    payload: &crate::wal::CrdtDeltaWalPayload,
) -> crate::engine::crdt::tenant_state::DeltaSigningAdmission {
    match payload.signing {
        Some(signing) => crate::engine::crdt::tenant_state::DeltaSigningAdmission {
            auth: nodedb_crdt::CrdtAuthContext {
                user_id: signing.auth_user_id,
                device_id: signing.auth_device_id,
                seq_no: signing.auth_seq_no,
                delta_signature: signing.delta_signature,
                ..nodedb_crdt::CrdtAuthContext::default()
            },
            required: signing.required,
            preverified: true,
        },
        None => crate::engine::crdt::tenant_state::DeltaSigningAdmission {
            auth: nodedb_crdt::CrdtAuthContext::default(),
            required: false,
            preverified: false,
        },
    }
}

#[cfg(test)]
mod crdt_replay_tests {
    use super::CoreLoop;
    use crate::types::{DatabaseId, TenantId};
    use loro::LoroValue;
    use nodedb_wal::record::RecordType;

    /// Holds the bridge endpoints + tempdir alive for the core's lifetime.
    /// The tests drive replay directly and never tick the event loop, so the
    /// far ends are unused — they just must not be dropped.
    struct CoreHarness {
        core: CoreLoop,
        _req_tx: nodedb_bridge::buffer::Producer<crate::bridge::dispatch::BridgeRequest>,
        _resp_rx: nodedb_bridge::buffer::Consumer<crate::bridge::dispatch::BridgeResponse>,
        _dir: tempfile::TempDir,
    }

    fn make_core(core_id: usize) -> CoreHarness {
        use crate::bridge::dispatch::{BridgeRequest, BridgeResponse};
        use nodedb_bridge::buffer::RingBuffer;

        let dir = tempfile::tempdir().expect("tempdir");
        let (req_tx, req_rx) = RingBuffer::channel::<BridgeRequest>(64);
        let (resp_tx, resp_rx) = RingBuffer::channel::<BridgeResponse>(64);
        let core = CoreLoop::open(
            core_id,
            req_rx,
            resp_tx,
            dir.path(),
            std::sync::Arc::new(nodedb_types::OrdinalClock::new()),
            crate::data::executor::core_loop::test_governor(),
        )
        .expect("open core");
        CoreHarness {
            core,
            _req_tx: req_tx,
            _resp_rx: resp_rx,
            _dir: dir,
        }
    }

    /// Build a CRDT snapshot for `tid` containing one row, then wrap it in a
    /// `CrdtDelta` WAL record exactly as `append_crdt_delta` does
    /// (`CrdtDeltaWalPayload` msgpack payload). Snapshot import and delta
    /// apply share the same idempotent Loro `state.import`, so a snapshot rides
    /// the delta record identically.
    fn make_crdt_record(
        database_id: u64,
        tid: TenantId,
        vshard_id: u32,
        collection: &str,
        row_id: &str,
    ) -> nodedb_wal::WalRecord {
        // Build one collection's CRDT doc directly; the WAL record carries the
        // collection so replay routes the import to the matching per-collection
        // LoroDoc.
        let state = nodedb_crdt::state::CrdtState::new(0).expect("state");
        state
            .upsert(
                collection,
                row_id,
                &[("name", LoroValue::String("alice".into()))],
            )
            .expect("upsert");
        let snapshot = state.export_snapshot().expect("export");
        assert!(!snapshot.is_empty(), "snapshot must be non-empty");

        let wal_payload = crate::wal::CrdtDeltaWalPayload::new(
            snapshot,
            collection.to_string(),
            None,
            None,
            document_target(row_id, 1),
        );
        let payload = wal_payload.encode().expect("encode payload");
        nodedb_wal::WalRecord::new(nodedb_wal::WalRecordArgs {
            record_type: RecordType::CrdtDelta as u32,
            lsn: 1,
            tenant_id: tid.as_u64(),
            vshard_id,
            database_id,
            payload,
            encryption_key: None,
            preamble_bytes: None,
        })
        .expect("wal record")
    }

    fn document_target(document_id: &str, surrogate: u32) -> crate::wal::CrdtDeltaTarget {
        crate::wal::CrdtDeltaTarget::document(
            document_id.to_owned(),
            nodedb_types::Surrogate::new(surrogate),
        )
        .expect("bound document target")
    }

    #[test]
    fn replay_crdt_wal_restores_state() {
        let tid = TenantId::new(7);
        let record = make_crdt_record(0, tid, 0, "notes", "row1");

        // Fresh core with empty CRDT state, mimicking a restart with no
        // checkpoint — only the WAL is available.
        let mut h = make_core(0);
        let tombstones = nodedb_wal::TombstoneSet::new();

        h.core
            .replay_crdt_wal(std::slice::from_ref(&record), 1, &tombstones);

        let engine = h
            .core
            .get_crdt_engine(crate::types::DatabaseId::DEFAULT, tid)
            .expect("engine");
        assert!(
            engine.row_exists("notes", "row1"),
            "CRDT row must be restored from WAL replay"
        );
    }

    /// The live apply refuses a stale fenced frame before admission, so the
    /// sender's mark stays put and its re-push at the same seq is admitted.
    /// Replay holds the mark the same way: advancing it would turn the
    /// re-push after a restart into a `Duplicate` and drop the write.
    #[test]
    fn replay_stale_v4_holds_authenticated_sequence_watermark() {
        let tid = TenantId::new(9);
        let provenance = nodedb_types::sync::wire::SyncProvenance {
            producer_id: 77,
            epoch: 3,
            stream_id: 5,
            seq: 11,
        };
        let state = nodedb_crdt::state::CrdtState::new(77).expect("state");
        state
            .upsert(
                "secure_notes",
                "doc",
                &[("body", LoroValue::String("signed".into()))],
            )
            .expect("upsert");
        let payload = crate::wal::CrdtDeltaWalPayload::new(
            state.export_snapshot().expect("snapshot"),
            "secure_notes".into(),
            Some(provenance.clone()),
            Some([0xee; 32]),
            document_target("doc", 1),
        )
        .with_signing(crate::wal::CrdtDeltaSigning {
            auth_user_id: 42,
            auth_device_id: provenance.producer_id,
            auth_seq_no: provenance.seq,
            delta_signature: [7; 32],
            required: true,
        });
        let record = nodedb_wal::WalRecord::new(nodedb_wal::WalRecordArgs {
            record_type: RecordType::CrdtDelta as u32,
            lsn: 10,
            tenant_id: tid.as_u64(),
            vshard_id: 0,
            database_id: DatabaseId::DEFAULT.as_u64(),
            payload: payload.encode().expect("encode"),
            encryption_key: None,
            preamble_bytes: None,
        })
        .expect("record");

        let mut h = make_core(0);
        h.core
            .replay_crdt_wal(&[record], 1, &nodedb_wal::TombstoneSet::new());
        assert_eq!(
            h.core
                .sync_hwm_value(provenance.producer_id, provenance.stream_id),
            0,
            "a stale fenced frame leaves the mark where the live apply left it"
        );
        assert!(!h.core.is_fail_stopped(), "a stale fence is no halt");
    }

    /// A signed record of `secure_notes/doc` at LSN 5, unfenced.
    fn signed_record(
        tid: TenantId,
        provenance: Option<nodedb_types::sync::wire::SyncProvenance>,
        signing: crate::wal::CrdtDeltaSigning,
    ) -> nodedb_wal::WalRecord {
        let state = nodedb_crdt::state::CrdtState::new(77).expect("state");
        state
            .upsert(
                "secure_notes",
                "doc",
                &[("body", LoroValue::String("signed".into()))],
            )
            .expect("upsert");
        let payload = crate::wal::CrdtDeltaWalPayload::new(
            state.export_snapshot().expect("snapshot"),
            "secure_notes".into(),
            provenance,
            None,
            document_target("doc", 1),
        )
        .with_signing(signing);
        nodedb_wal::WalRecord::new(nodedb_wal::WalRecordArgs {
            record_type: RecordType::CrdtDelta as u32,
            lsn: 5,
            tenant_id: tid.as_u64(),
            vshard_id: 0,
            database_id: DatabaseId::DEFAULT.as_u64(),
            payload: payload.encode().expect("encode"),
            encryption_key: None,
            preamble_bytes: None,
        })
        .expect("record")
    }

    fn session_provenance() -> nodedb_types::sync::wire::SyncProvenance {
        nodedb_types::sync::wire::SyncProvenance {
            producer_id: 77,
            epoch: 3,
            stream_id: 5,
            seq: 11,
        }
    }

    fn signing(user: u64, signature: [u8; 32], required: bool) -> crate::wal::CrdtDeltaSigning {
        crate::wal::CrdtDeltaSigning {
            auth_user_id: user,
            auth_device_id: 77,
            auth_seq_no: 11,
            delta_signature: signature,
            required,
        }
    }

    fn doc_replayed(h: &mut CoreHarness, tid: TenantId) -> bool {
        h.core
            .get_crdt_engine(DatabaseId::DEFAULT, tid)
            .expect("engine")
            .row_exists("secure_notes", "doc")
    }

    /// The writer stamps provenance on every signed record and the live apply
    /// applied it, so a signed record without provenance halts replay.
    #[test]
    fn a_signed_record_without_provenance_halts_replay() {
        let tid = TenantId::new(21);
        let mut h = make_core(0);
        h.core.replay_crdt_wal(
            &[signed_record(tid, None, signing(42, [7; 32], true))],
            1,
            &nodedb_wal::TombstoneSet::new(),
        );
        assert!(h.core.is_fail_stopped());
        assert!(h.core.replay_halt_error().is_some());
    }

    /// The writer copies one producer id and seq into the provenance and the
    /// signing fields, so a record where they disagree halts replay.
    #[test]
    fn a_signed_record_with_disagreeing_metadata_halts_replay() {
        let tid = TenantId::new(22);
        let mut provenance = session_provenance();
        provenance.seq = 12;
        let mut h = make_core(0);
        h.core.replay_crdt_wal(
            &[signed_record(
                tid,
                Some(provenance),
                signing(42, [7; 32], true),
            )],
            1,
            &nodedb_wal::TombstoneSet::new(),
        );
        assert!(h.core.is_fail_stopped());
        assert!(!doc_replayed(&mut h, tid));
    }

    /// The live apply reads no user id from the signing fields, so a record
    /// whose user id is zero applied live and applies on replay.
    #[test]
    fn a_signed_record_with_a_zero_user_id_replays() {
        let tid = TenantId::new(23);
        let provenance = session_provenance();
        let mut h = make_core(0);
        h.core.replay_crdt_wal(
            &[signed_record(
                tid,
                Some(provenance.clone()),
                signing(0, [7; 32], true),
            )],
            1,
            &nodedb_wal::TombstoneSet::new(),
        );
        assert!(!h.core.is_fail_stopped());
        assert!(doc_replayed(&mut h, tid));
        assert_eq!(
            h.core
                .sync_hwm_value(provenance.producer_id, provenance.stream_id),
            provenance.seq
        );
    }

    /// A required signature that is absent made the live apply refuse the
    /// delta as malformed and advance the mark. Replay reaches the same
    /// refusal: nothing applies, the mark advances, and replay continues.
    #[test]
    fn a_record_missing_its_required_signature_replays_as_its_live_refusal() {
        let tid = TenantId::new(24);
        let provenance = session_provenance();
        let mut h = make_core(0);
        h.core.replay_crdt_wal(
            &[signed_record(
                tid,
                Some(provenance.clone()),
                signing(42, [0; 32], true),
            )],
            1,
            &nodedb_wal::TombstoneSet::new(),
        );
        assert!(!h.core.is_fail_stopped());
        assert!(!doc_replayed(&mut h, tid));
        assert_eq!(
            h.core
                .sync_hwm_value(provenance.producer_id, provenance.stream_id),
            provenance.seq
        );
    }

    /// An unsigned `secure_notes/doc` record at LSN 5 that carries `delta`
    /// under the session's provenance.
    fn provenance_record(tid: TenantId, delta: Vec<u8>) -> nodedb_wal::WalRecord {
        let payload = crate::wal::CrdtDeltaWalPayload::new(
            delta,
            "secure_notes".into(),
            Some(session_provenance()),
            None,
            document_target("doc", 1),
        );
        nodedb_wal::WalRecord::new(nodedb_wal::WalRecordArgs {
            record_type: RecordType::CrdtDelta as u32,
            lsn: 5,
            tenant_id: tid.as_u64(),
            vshard_id: 0,
            database_id: DatabaseId::DEFAULT.as_u64(),
            payload: payload.encode().expect("encode"),
            encryption_key: None,
            preamble_bytes: None,
        })
        .expect("record")
    }

    /// Delta bytes that do not decode made the live apply refuse the delta
    /// as malformed and advance the mark. Replay reaches the same refusal:
    /// nothing applies, the mark advances, and replay continues.
    #[test]
    fn a_malformed_delta_replays_as_its_live_refusal() {
        let tid = TenantId::new(25);
        let provenance = session_provenance();
        let mut h = make_core(0);
        h.core.replay_crdt_wal(
            &[provenance_record(tid, b"not a loro delta".to_vec())],
            1,
            &nodedb_wal::TombstoneSet::new(),
        );
        assert!(!h.core.is_fail_stopped());
        assert!(!doc_replayed(&mut h, tid));
        assert_eq!(
            h.core
                .sync_hwm_value(provenance.producer_id, provenance.stream_id),
            provenance.seq
        );
    }

    /// A delta whose predecessors were absent made the live apply hold the
    /// mark for the sender's re-push. Replay in LSN order meets the same
    /// absence: nothing applies, the mark holds, and replay continues. The
    /// re-push lands at a later LSN once the predecessors arrived.
    #[test]
    fn a_delta_missing_its_predecessors_replays_as_its_live_hold() {
        let tid = TenantId::new(26);
        let source = nodedb_crdt::state::CrdtState::new(77).expect("state");
        source
            .upsert(
                "secure_notes",
                "doc",
                &[("body", LoroValue::String("base".into()))],
            )
            .expect("base write");
        let base_vv = source.oplog_version_vector();
        source
            .upsert(
                "secure_notes",
                "doc",
                &[("body", LoroValue::String("next".into()))],
            )
            .expect("next write");
        let dependent = source
            .export_updates_since(&base_vv)
            .expect("dependent delta");
        let provenance = session_provenance();
        let mut h = make_core(0);
        h.core.replay_crdt_wal(
            &[provenance_record(tid, dependent)],
            1,
            &nodedb_wal::TombstoneSet::new(),
        );
        assert!(!h.core.is_fail_stopped(), "a held delta is no halt");
        assert!(!doc_replayed(&mut h, tid));
        assert_eq!(
            h.core
                .sync_hwm_value(provenance.producer_id, provenance.stream_id),
            0,
            "the mark stays where the live apply held it"
        );
    }

    /// The WAL writer's own record for an apply carries the row's surrogate,
    /// and a restart replay rebuilds the row's projection under that identity.
    #[test]
    fn a_written_apply_keeps_its_identity_across_replay() {
        let tid = TenantId::new(12);
        let db = DatabaseId::DEFAULT;
        let collection = "notes";
        let surrogate = nodedb_types::Surrogate::new(4_321);
        let source = nodedb_crdt::state::CrdtState::new(5).expect("source state");
        source
            .upsert(
                collection,
                "doc",
                &[("body", LoroValue::String("kept".into()))],
            )
            .expect("write");
        let plan = nodedb_physical::physical_plan::CrdtOp::Apply {
            collection: nodedb_types::QualifiedCollection::new(db, collection),
            document_id: "doc".into(),
            delta: source.export_snapshot().expect("snapshot"),
            peer_id: 5,
            mutation_id: 1,
            surrogate,
            provenance: None,
            constraint_version_required: 0,
            expected_frontier_digest: None,
        };
        let (_, payload) = crate::control::server::wal_dispatch::encode_crdt_op_record(&plan)
            .expect("encode")
            .expect("an apply writes a record");
        let record = nodedb_wal::WalRecord::new(nodedb_wal::WalRecordArgs {
            record_type: RecordType::CrdtDelta as u32,
            lsn: 4,
            tenant_id: tid.as_u64(),
            vshard_id: 0,
            database_id: db.as_u64(),
            payload,
            encryption_key: None,
            preamble_bytes: None,
        })
        .expect("wal record");

        let mut h = make_core(0);
        h.core
            .replay_crdt_wal(&[record], 1, &nodedb_wal::TombstoneSet::new());

        let sparse_key = crate::engine::document::store::StorageKey::for_surrogate(surrogate);
        assert!(
            h.core
                .sparse
                .get(db.as_u64(), tid.as_u64(), collection, &sparse_key)
                .expect("sparse read")
                .is_some(),
            "replay must project the row under the surrogate its record carries"
        );
    }

    #[test]
    fn replay_skips_stale_fence_then_applies_correctly_fenced_retry() {
        let tid = TenantId::new(11);
        let db = DatabaseId::DEFAULT;
        let collection = "notes";
        let source = nodedb_crdt::state::CrdtState::new(1).expect("source state");
        source
            .upsert(
                collection,
                "doc",
                &[("body", LoroValue::String("base".into()))],
            )
            .expect("base write");
        let base_snapshot = source.export_snapshot().expect("base snapshot");
        let base = crate::wal::CrdtDeltaWalPayload::new(
            base_snapshot,
            collection.into(),
            None,
            None,
            document_target("doc", 1),
        );

        let frontier = nodedb_crdt::state::frontier_digest::domain_frontier_digest(
            tid.as_u64(),
            db.as_u64(),
            collection,
            Some(&source),
        );
        let base_vv = source.oplog_version_vector();
        source
            .upsert(
                collection,
                "doc",
                &[("body", LoroValue::String("retry".into()))],
            )
            .expect("retry write");
        let retry_delta = source.export_updates_since(&base_vv).expect("retry delta");
        let stale = crate::wal::CrdtDeltaWalPayload::new(
            retry_delta.clone(),
            collection.into(),
            None,
            Some([0xde; 32]),
            document_target("doc", 1),
        );
        let retry = crate::wal::CrdtDeltaWalPayload::new(
            retry_delta,
            collection.into(),
            None,
            Some(frontier),
            document_target("doc", 1),
        );
        let record = |lsn, payload: crate::wal::CrdtDeltaWalPayload| {
            nodedb_wal::WalRecord::new(nodedb_wal::WalRecordArgs {
                record_type: RecordType::CrdtDelta as u32,
                lsn,
                tenant_id: tid.as_u64(),
                vshard_id: 0,
                database_id: db.as_u64(),
                payload: payload.encode().expect("encode payload"),
                encryption_key: None,
                preamble_bytes: None,
            })
            .expect("wal record")
        };

        let mut h = make_core(0);
        h.core.replay_crdt_wal(
            &[record(1, base), record(2, stale), record(3, retry)],
            1,
            &nodedb_wal::TombstoneSet::new(),
        );
        let engine = h.core.get_crdt_engine(db, tid).expect("replayed engine");
        let row = engine
            .read_row(collection, "doc")
            .expect("retry row must exist");
        let LoroValue::Map(fields) = row else {
            panic!("retry row must be a map");
        };
        assert_eq!(
            fields.get("body"),
            Some(&LoroValue::String("retry".into())),
            "stale fenced record must be a no-op while matching retry applies"
        );
        let sparse_key = crate::engine::document::store::StorageKey::for_surrogate(
            nodedb_types::Surrogate::new(1),
        );
        assert!(
            h.core
                .sparse
                .get(db.as_u64(), tid.as_u64(), collection, &sparse_key)
                .expect("sparse read")
                .is_some(),
            "matching fenced retry must rebuild its sparse projection"
        );
        assert_eq!(
            h.core.write_index.collection_version(
                &crate::data::executor::core_loop::write_index::CollKey {
                    vshard: crate::data::executor::core_loop::write_index::tests::replay_home(
                        db, collection
                    ),
                    db,
                    tenant: tid,
                    collection: Box::from(collection),
                }
            ),
            Some(crate::data::executor::core_loop::write_index::tests::local(
                3
            )),
            "only the correctly fenced retry advances the replay write version"
        );
    }

    #[test]
    fn replay_crdt_wal_honors_database_scoped_collection_tombstones() {
        let tid = TenantId::new(7);
        let dropped_db = crate::types::DatabaseId::new(1);
        let retained_db = crate::types::DatabaseId::new(2);
        // Records in a named database carry the database-qualified name.
        let dropped_name = nodedb_types::QualifiedCollection::new(dropped_db, "notes");
        let retained_name = nodedb_types::QualifiedCollection::new(retained_db, "notes");
        let dropped = make_crdt_record(1, tid, 0, dropped_name.as_str(), "dropped-row");
        let retained = make_crdt_record(2, tid, 0, retained_name.as_str(), "retained-row");
        // The tombstone names the collection by its bare catalog name.
        let mut tombstones = nodedb_wal::TombstoneSet::new();
        tombstones.insert(
            nodedb_types::CollectionKey::from_bare(dropped_db, "notes"),
            tid.as_u64(),
            2,
        );

        let mut h = make_core(0);
        h.core.replay_crdt_wal(&[dropped, retained], 1, &tombstones);

        let dropped_engine = h
            .core
            .get_crdt_engine(crate::types::DatabaseId::new(1), tid)
            .expect("dropped database engine");
        assert!(!dropped_engine.row_exists(dropped_name.as_str(), "dropped-row"));
        let retained_engine = h
            .core
            .get_crdt_engine(crate::types::DatabaseId::new(2), tid)
            .expect("retained database engine");
        assert!(retained_engine.row_exists(retained_name.as_str(), "retained-row"));
    }

    #[test]
    fn replay_crdt_wal_skips_other_cores() {
        // vshard 1 with num_cores 2 routes to core 1, so core 0 must skip it.
        let tid = TenantId::new(9);
        let record = make_crdt_record(0, tid, 1, "notes", "row1");

        let mut h = make_core(0);
        let tombstones = nodedb_wal::TombstoneSet::new();
        h.core
            .replay_crdt_wal(std::slice::from_ref(&record), 2, &tombstones);

        let engine = h
            .core
            .get_crdt_engine(crate::types::DatabaseId::DEFAULT, tid)
            .expect("engine");
        assert!(
            !engine.row_exists("notes", "row1"),
            "core 0 must not replay a record routed to core 1"
        );
    }

    /// A `users` row record at `lsn` whose delta sets `email`. The row's
    /// surrogate is its producing peer, so each row carries its own identity.
    fn email_record(
        tid: TenantId,
        row_id: &str,
        email: &str,
        peer: u64,
        lsn: u64,
    ) -> nodedb_wal::WalRecord {
        let state = nodedb_crdt::state::CrdtState::new(peer).expect("state");
        state
            .upsert(
                "users",
                row_id,
                &[("email", LoroValue::String(email.into()))],
            )
            .expect("upsert");
        let payload = crate::wal::CrdtDeltaWalPayload::new(
            state.export_snapshot().expect("snapshot"),
            "users".into(),
            None,
            None,
            document_target(row_id, u32::try_from(peer).expect("peer fits a surrogate")),
        )
        .with_peer_id(peer);
        nodedb_wal::WalRecord::new(nodedb_wal::WalRecordArgs {
            record_type: RecordType::CrdtDelta as u32,
            lsn,
            tenant_id: tid.as_u64(),
            vshard_id: 0,
            database_id: DatabaseId::DEFAULT.as_u64(),
            payload: payload.encode().expect("encode"),
            encryption_key: None,
            preamble_bytes: None,
        })
        .expect("record")
    }

    /// A record whose delta a constraint rejects leaves one stored entry,
    /// however often it replays.
    #[test]
    fn a_replayed_rejection_stores_one_dead_letter_for_its_record() {
        let tid = TenantId::new(7);
        let mut h = make_core(0);
        {
            let engine = h
                .core
                .get_crdt_engine(DatabaseId::DEFAULT, tid)
                .expect("engine");
            assert!(engine.set_collection_constraints(
                "users",
                1,
                vec![nodedb_crdt::Constraint {
                    name: "users_email_unique".into(),
                    collection: "users".into(),
                    field: "email".into(),
                    kind: nodedb_crdt::ConstraintKind::Unique,
                }],
            ));
            engine.set_collection_policy_typed(
                "users",
                nodedb_crdt::policy::CollectionPolicy::strict(),
            );
        }
        let tombstones = nodedb_wal::TombstoneSet::new();
        let records = [
            email_record(tid, "a", "x@y.com", 2, 19),
            email_record(tid, "b", "x@y.com", 3, 20),
        ];
        h.core.replay_crdt_wal(&records, 1, &tombstones);
        h.core.replay_crdt_wal(&records[1..], 1, &tombstones);

        let entries: Vec<_> = h
            .core
            .get_crdt_engine(DatabaseId::DEFAULT, tid)
            .expect("engine")
            .dead_letters()
            .cloned()
            .collect();
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].source_lsn, Some(20));
        assert_eq!(entries[0].peer_id, 3, "the entry names the producing peer");
        assert_eq!(
            h.core
                .sparse
                .load_crdt_dead_letters(DatabaseId::DEFAULT.as_u64(), tid.as_u64())
                .expect("load"),
            entries
        );
    }

    /// A core whose `users` collection refuses a second row with one email.
    fn unique_email_core(tid: TenantId) -> CoreHarness {
        let mut h = make_core(0);
        let engine = h
            .core
            .get_crdt_engine(DatabaseId::DEFAULT, tid)
            .expect("engine");
        assert!(engine.set_collection_constraints(
            "users",
            1,
            vec![nodedb_crdt::Constraint {
                name: "users_email_unique".into(),
                collection: "users".into(),
                field: "email".into(),
                kind: nodedb_crdt::ConstraintKind::Unique,
            }],
        ));
        engine
            .set_collection_policy_typed("users", nodedb_crdt::policy::CollectionPolicy::strict());
        h
    }

    /// The replayed version of `users`, as the WAL LSN it was written at.
    fn users_write_lsn(h: &CoreHarness, tid: TenantId) -> Option<crate::types::Lsn> {
        use crate::data::executor::core_loop::write_index::tests::{local, replay_home};
        let version = h.core.write_index.collection_version(
            &crate::data::executor::core_loop::write_index::CollKey {
                vshard: replay_home(DatabaseId::DEFAULT, "users"),
                db: DatabaseId::DEFAULT,
                tenant: tid,
                collection: Box::from("users"),
            },
        )?;
        assert_eq!(
            version,
            local(version.local),
            "a single-node replay records a local version"
        );
        Some(crate::types::Lsn::new(version.local))
    }

    /// A replayed rejection the full queue refuses stops replay at its
    /// record: only the record holds the delta, so it stays uncovered.
    #[test]
    fn a_replayed_rejection_the_full_queue_refuses_halts_replay() {
        let tid = TenantId::new(7);
        let mut h = unique_email_core(tid);
        let tombstones = nodedb_wal::TombstoneSet::new();
        h.core
            .replay_crdt_wal(&[email_record(tid, "a", "x@y.com", 2, 19)], 1, &tombstones);
        let capacity = h
            .core
            .get_crdt_engine(DatabaseId::DEFAULT, tid)
            .expect("engine")
            .fill_dead_letter_queue_for_test();

        h.core
            .replay_crdt_wal(&[email_record(tid, "b", "x@y.com", 3, 20)], 1, &tombstones);

        assert!(h.core.replay_halt_error().is_some(), "replay stopped");
        let engine = h
            .core
            .get_crdt_engine(DatabaseId::DEFAULT, tid)
            .expect("engine");
        assert_eq!(engine.dead_letters().count(), capacity, "nothing queued");
        assert!(!engine.row_exists("users", "b"));
        assert!(
            h.core
                .sparse
                .load_crdt_dead_letters(DatabaseId::DEFAULT.as_u64(), tid.as_u64())
                .expect("load")
                .is_empty()
        );
        assert_eq!(
            users_write_lsn(&h, tid),
            Some(crate::types::Lsn::new(19)),
            "the refused record is not counted as replayed"
        );
    }

    /// A replayed rejection whose entry the store refuses stops replay at
    /// its record, and the queue keeps no entry storage lacks.
    #[test]
    fn a_replayed_rejection_the_store_refuses_halts_replay() {
        let tid = TenantId::new(7);
        let mut h = unique_email_core(tid);
        let tombstones = nodedb_wal::TombstoneSet::new();
        h.core
            .replay_crdt_wal(&[email_record(tid, "a", "x@y.com", 2, 19)], 1, &tombstones);
        h.core.sparse.break_crdt_dead_letter_table_for_test();

        h.core
            .replay_crdt_wal(&[email_record(tid, "b", "x@y.com", 3, 20)], 1, &tombstones);

        assert!(h.core.replay_halt_error().is_some(), "replay stopped");
        let engine = h
            .core
            .get_crdt_engine(DatabaseId::DEFAULT, tid)
            .expect("engine");
        assert_eq!(engine.dead_letters().count(), 0, "nothing queued");
        assert_eq!(users_write_lsn(&h, tid), Some(crate::types::Lsn::new(19)));
    }

    /// A record whose stored entry the engine restored replays as that
    /// rejection, so a queue full of restored entries does not stop replay.
    #[test]
    fn a_replayed_record_with_a_restored_entry_replays_under_a_full_queue() {
        let tid = TenantId::new(7);
        let mut h = unique_email_core(tid);
        let tombstones = nodedb_wal::TombstoneSet::new();
        let records = [
            email_record(tid, "a", "x@y.com", 2, 19),
            email_record(tid, "b", "x@y.com", 3, 20),
        ];
        h.core.replay_crdt_wal(&records, 1, &tombstones);
        h.core
            .get_crdt_engine(DatabaseId::DEFAULT, tid)
            .expect("engine")
            .fill_dead_letter_queue_for_test();

        h.core.replay_crdt_wal(&records[1..], 1, &tombstones);

        assert!(h.core.replay_halt_error().is_none(), "replay continued");
        let engine = h
            .core
            .get_crdt_engine(DatabaseId::DEFAULT, tid)
            .expect("engine");
        assert_eq!(
            engine
                .dead_letters()
                .filter(|entry| entry.source_lsn == Some(20))
                .count(),
            1
        );
        assert_eq!(users_write_lsn(&h, tid), Some(crate::types::Lsn::new(20)));
    }

    /// A stored entry that does not decode fails the engine open, and replay
    /// stops at the first record that needs the engine.
    #[test]
    fn an_undecodable_stored_entry_halts_replay() {
        let tid = TenantId::new(8);
        let mut h = make_core(0);
        h.core.sparse.put_raw_crdt_dead_letter_for_test(
            DatabaseId::DEFAULT.as_u64(),
            tid.as_u64(),
            4,
            b"\xc1",
        );

        let record = make_crdt_record(0, tid, 0, "notes", "row1");
        h.core
            .replay_crdt_wal(&[record], 1, &nodedb_wal::TombstoneSet::new());

        assert!(h.core.replay_halt_error().is_some(), "replay stopped");
        assert!(
            !h.core
                .crdt_engines
                .contains_key(&(DatabaseId::DEFAULT, tid))
        );
    }
}
