// SPDX-License-Identifier: BUSL-1.1

//! CRDT WAL replay: rebuilds authoritative Loro and sparse document state after crash.

use crate::data::executor::core_loop::CoreLoop;

impl CoreLoop {
    /// Try to replay one WAL CRDT delta record.
    ///
    /// This single-record entrypoint lets the startup coordinator preserve the
    /// global LSN order across CRDT delta and intent record classes. The
    /// established bulk replayer remains the implementation so legacy callers
    /// retain identical decode, tombstone, fence, and projection semantics.
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
    /// Current identity-bearing records rebuild both the authoritative Loro row
    /// and its sparse document projection; historical payloads without identity
    /// retain their established Loro-only replay behavior. CRDT records use
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
        use tracing::{error, warn};

        let mut replayed = 0usize;

        for record in records {
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
            let Ok(payload) = crate::wal::CrdtDeltaWalPayload::decode(&record.payload) else {
                continue;
            };

            // Every CRDT delta / snapshot-import record written by the current
            // binary carries its collection. A record with no collection cannot
            // be routed to a per-collection doc; skip it (a pre-per-collection
            // record from an earlier dev binary — there is no released data to
            // preserve).
            let Some(collection) = payload.collection.as_deref() else {
                warn!(
                    core = self.core_id,
                    tenant = tid.as_u64(),
                    "CRDT WAL record without collection; skipping (cannot route per-collection)"
                );
                continue;
            };
            if tombstones.is_tombstoned(
                record.header.database_id,
                tid.as_u64(),
                collection,
                record.header.lsn,
            ) {
                continue;
            }

            let database_id = crate::types::DatabaseId::new(record.header.database_id);
            if let Some(signing) = payload.signing {
                let Some(provenance) = payload.provenance.as_ref() else {
                    warn!(core = self.core_id, tenant = tid.as_u64(), %collection, "authenticated CRDT WAL record has no provenance");
                    continue;
                };
                if signing.auth_user_id == 0
                    || signing.auth_device_id == 0
                    || signing.auth_seq_no == 0
                    || provenance.producer_id != signing.auth_device_id
                    || provenance.seq != signing.auth_seq_no
                    || (signing.required && signing.delta_signature == [0; 32])
                {
                    warn!(core = self.core_id, tenant = tid.as_u64(), %collection, "authenticated CRDT WAL admission metadata is inconsistent");
                    continue;
                }
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
                    if payload.signing.is_some()
                        && let Some(provenance) = payload.provenance.as_ref()
                    {
                        self.sync_commit(provenance);
                    }
                    warn!(
                        core = self.core_id,
                        tenant = tid.as_u64(),
                        %collection,
                        "skipping stale fenced CRDT WAL delta during replay"
                    );
                    continue;
                }
            }

            let projection = match (&payload.document_id, payload.surrogate) {
                (Some(document_id), Some(surrogate)) => {
                    let applied = match self.get_crdt_engine(database_id, tid) {
                        Ok(engine) => match payload.signing {
                            Some(signing) => engine.apply_committed_delta_authenticated(
                                collection,
                                &payload.bytes,
                                nodedb_types::Surrogate::new(surrogate),
                                document_id,
                                0,
                                crate::engine::crdt::tenant_state::DeltaSigningAdmission {
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
                            ),
                            None => engine.apply_committed_delta_validated(
                                collection,
                                &payload.bytes,
                                nodedb_types::Surrogate::new(surrogate),
                                document_id,
                                0,
                            ),
                        },
                        Err(e) => {
                            warn!(
                                core = self.core_id,
                                tenant = tid.as_u64(),
                                error = %e,
                                "failed to create CRDT engine during WAL replay"
                            );
                            continue;
                        }
                    };
                    match applied {
                        crate::engine::crdt::tenant_state::ValidatedApplyOutcome::Clean {
                            write_set,
                            ..
                        } => {
                            if let Err(detail) =
                                Self::single_document_write_set(collection, document_id, &write_set)
                            {
                                self.note_replay_write_lsn(
                                    record.header.database_id,
                                    tid.as_u64(),
                                    collection,
                                    None,
                                    record.header.lsn,
                                );
                                warn!(
                                    core = self.core_id,
                                    tenant = tid.as_u64(),
                                    %collection,
                                    %document_id,
                                    %detail,
                                    "CRDT WAL delta violates one-document replay contract"
                                );
                                continue;
                            }
                            let Some(engine) = self.crdt_engines.get(&(database_id, tid)) else {
                                warn!(
                                    core = self.core_id,
                                    tenant = tid.as_u64(),
                                    "CRDT engine disappeared during WAL replay"
                                );
                                continue;
                            };
                            let surrogate = nodedb_types::Surrogate::new(surrogate);
                            Some((
                                document_id.as_str(),
                                surrogate,
                                (surrogate != nodedb_types::Surrogate::ZERO)
                                    .then(|| Self::encode_crdt_row(engine, collection, document_id))
                                    .flatten(),
                            ))
                        }
                        crate::engine::crdt::tenant_state::ValidatedApplyOutcome::Rejected(
                            reason,
                        ) => {
                            // The detached candidate was rejected and discarded.
                            // The committed record remains a deterministic no-op
                            // whose collection floor advances on every replica.
                            warn!(core = self.core_id, tenant = tid.as_u64(), %collection, %reason, "CRDT WAL delta rejected during replay");
                            None
                        }
                        crate::engine::crdt::tenant_state::ValidatedApplyOutcome::Malformed => {
                            if let Some(provenance) = payload.provenance.as_ref() {
                                self.sync_commit(provenance);
                            }
                            warn!(core = self.core_id, tenant = tid.as_u64(), %collection, "CRDT WAL delta malformed during replay");
                            continue;
                        }
                        crate::engine::crdt::tenant_state::ValidatedApplyOutcome::PendingDependencies => {
                            // Records replay in LSN order, so a delta whose
                            // causal predecessors are missing means the log is
                            // inconsistent with this collection's document —
                            // not a routine skip. Recovery continues so the
                            // node still starts, but this is an error-level
                            // event: the row it carried is NOT present.
                            error!(
                                core = self.core_id,
                                tenant = tid.as_u64(),
                                %collection,
                                %document_id,
                                lsn = record.header.lsn,
                                "CRDT WAL delta depends on operations absent from this \
                                 collection's document; row not recovered"
                            );
                            continue;
                        }
                    }
                }
                _ => {
                    // Historical payloads carry no row identity. Preserve their
                    // established Loro-only replay behavior rather than guessing
                    // a sparse key from attacker-controlled delta contents.
                    match self.get_crdt_engine(database_id, tid) {
                        Ok(engine) => match engine.apply_committed_delta_validated(
                            collection,
                            &payload.bytes,
                            nodedb_types::Surrogate::ZERO,
                            "",
                            0,
                        ) {
                            crate::engine::crdt::tenant_state::ValidatedApplyOutcome::Clean {
                                ..
                            } => None,
                            crate::engine::crdt::tenant_state::ValidatedApplyOutcome::Rejected(
                                reason,
                            ) => {
                                warn!(core = self.core_id, tenant = tid.as_u64(), %collection, %reason, "legacy CRDT WAL delta rejected during replay");
                                None
                            }
                            crate::engine::crdt::tenant_state::ValidatedApplyOutcome::Malformed => {
                                if let Some(provenance) = payload.provenance.as_ref() {
                                    self.sync_commit(provenance);
                                }
                                warn!(core = self.core_id, tenant = tid.as_u64(), %collection, "legacy CRDT WAL delta malformed during replay");
                                continue;
                            }
                            crate::engine::crdt::tenant_state::ValidatedApplyOutcome::PendingDependencies => {
                                // Committed record whose causal predecessors are
                                // absent from this collection's document: the row
                                // did NOT apply. Loud, and not acknowledged as a
                                // clean replay.
                                error!(core = self.core_id, tenant = tid.as_u64(), %collection, lsn = record.header.lsn, "legacy CRDT WAL delta depends on operations absent from this collection's document; row not recovered");
                                continue;
                            }
                        },
                        Err(e) => {
                            warn!(core = self.core_id, tenant = tid.as_u64(), error = %e, "failed to create CRDT engine during WAL replay");
                            continue;
                        }
                    }
                }
            };

            if let Some((_document_id, surrogate, Some(bytes))) = projection {
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
                    surrogate,
                    &bytes,
                );
            }
            if let Some(provenance) = payload.provenance.as_ref() {
                self.sync_commit(provenance);
            }
            // Every successfully imported payload changed authoritative Loro
            // state, including historical payloads that predate sparse-row
            // identity. Record its exact durable collection floor even when a
            // legacy record cannot safely reconstruct a surrogate-keyed
            // projection without inventing cross-engine identity.
            self.note_replay_write_lsn(
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
            Some(collection.to_string()),
            None,
            None,
            Some(row_id.to_owned()),
            Some(1),
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

    #[test]
    fn replay_stale_v4_restores_authenticated_sequence_watermark() {
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
            Some("secure_notes".into()),
            Some(provenance.clone()),
            Some([0xee; 32]),
            Some("doc".into()),
            Some(1),
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
        assert!(matches!(
            h.core.sync_admit(&provenance),
            crate::data::executor::sync_gate::SyncAdmit::Duplicate
        ));
    }

    #[test]
    fn replay_legacy_payload_remains_loro_only_and_decodable() {
        #[derive(zerompk::ToMessagePack, zerompk::FromMessagePack)]
        struct LegacyPayload {
            bytes: Vec<u8>,
            collection: Option<String>,
            provenance: Option<nodedb_types::sync::wire::SyncProvenance>,
        }

        let tid = TenantId::new(8);
        let state = nodedb_crdt::state::CrdtState::new(0).expect("state");
        state
            .upsert(
                "notes",
                "legacy",
                &[("body", LoroValue::String("old".into()))],
            )
            .expect("upsert");
        let payload = zerompk::to_msgpack_vec(&LegacyPayload {
            bytes: state.export_snapshot().expect("snapshot"),
            collection: Some("notes".into()),
            provenance: None,
        })
        .expect("encode legacy");
        let record = nodedb_wal::WalRecord::new(nodedb_wal::WalRecordArgs {
            record_type: RecordType::CrdtDelta as u32,
            lsn: 9,
            tenant_id: tid.as_u64(),
            vshard_id: 0,
            database_id: DatabaseId::DEFAULT.as_u64(),
            payload,
            encryption_key: None,
            preamble_bytes: None,
        })
        .expect("wal record");

        let mut h = make_core(0);
        h.core
            .replay_crdt_wal(&[record], 1, &nodedb_wal::TombstoneSet::new());
        let engine = h
            .core
            .get_crdt_engine(DatabaseId::DEFAULT, tid)
            .expect("engine");
        assert!(engine.row_exists("notes", "legacy"));
        assert_eq!(
            h.core.write_index.collection_write_lsn(
                &crate::data::executor::core_loop::write_index::CollKey {
                    db: DatabaseId::DEFAULT,
                    tenant: tid,
                    collection: Box::from("notes"),
                }
            ),
            Some(crate::types::Lsn::new(9)),
            "legacy authoritative Loro replay must restore its durable floor"
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
            Some(collection.into()),
            None,
            None,
            Some("doc".into()),
            Some(1),
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
            Some(collection.into()),
            None,
            Some([0xde; 32]),
            Some("doc".into()),
            Some(1),
        );
        let retry = crate::wal::CrdtDeltaWalPayload::new(
            retry_delta,
            Some(collection.into()),
            None,
            Some(frontier),
            Some("doc".into()),
            Some(1),
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
            h.core.write_index.collection_write_lsn(
                &crate::data::executor::core_loop::write_index::CollKey {
                    db,
                    tenant: tid,
                    collection: Box::from(collection),
                }
            ),
            Some(crate::types::Lsn::new(3)),
            "only the correctly fenced retry advances the replay write floor"
        );
    }

    #[test]
    fn replay_crdt_wal_honors_database_scoped_collection_tombstones() {
        let tid = TenantId::new(7);
        let dropped = make_crdt_record(1, tid, 0, "notes", "dropped-row");
        let retained = make_crdt_record(2, tid, 0, "notes", "retained-row");
        let mut tombstones = nodedb_wal::TombstoneSet::new();
        tombstones.insert(1, tid.as_u64(), "notes".to_string(), 2);

        let mut h = make_core(0);
        h.core.replay_crdt_wal(&[dropped, retained], 1, &tombstones);

        let dropped_engine = h
            .core
            .get_crdt_engine(crate::types::DatabaseId::new(1), tid)
            .expect("dropped database engine");
        assert!(!dropped_engine.row_exists("notes", "dropped-row"));
        let retained_engine = h
            .core
            .get_crdt_engine(crate::types::DatabaseId::new(2), tid)
            .expect("retained database engine");
        assert!(retained_engine.row_exists("notes", "retained-row"));
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
}
