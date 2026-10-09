// SPDX-License-Identifier: BUSL-1.1

//! RESTORE collection SET VERSION = 'checkpoint' WHERE id = 'doc-id'

use std::time::Duration;

#[cfg(test)]
use crate::bridge::envelope::PhysicalPlan;
use crate::control::crdt_post_image_policy::ExternalCrdtPostImagePolicy;
use crate::control::security::audit::ArcAuditEmitter;
use crate::control::security::identity::{AuthenticatedIdentity, Permission};
use crate::control::server::shared::authorization::authorize_collection;
use crate::control::server::shared::ddl::sql_parse::parse_ident_token;
use crate::control::state::SharedState;
use crate::types::DatabaseId;
#[cfg(test)]
use crate::types::TenantId;
#[cfg(test)]
use nodedb_physical::physical_plan::CrdtOp;
use nodedb_sql::parser::preprocess::lex::find_ascii_case_insensitive;
#[cfg(test)]
use nodedb_types::Surrogate;

use super::super::super::result::{DdlError, DdlResult};

fn err(sqlstate: &str, message: String) -> DdlError {
    DdlError::new(sqlstate, message)
}

/// RESTORE collection SET VERSION = 'checkpoint' WHERE id = 'doc-id'
///
/// Restores a document to a historical version by creating a forward delta.
/// History is preserved — this is a new mutation, not a rollback.
pub async fn restore_version(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
    database_id: DatabaseId,
    sql: &str,
) -> Result<Vec<DdlResult>, DdlError> {
    let (collection, checkpoint_name, doc_id) = parse_restore(sql)?;
    let tenant_id = identity.tenant_id;
    let audit = ArcAuditEmitter(std::sync::Arc::clone(&state.audit));
    authorize_collection(
        identity,
        database_id,
        &collection,
        Permission::Write,
        &state.permissions,
        &state.roles,
        &audit,
    )
    .map_err(|error| err("42501", format!("permission denied: {}", error.resource())))?;

    let vv_json = super::at_version::resolve_checkpoint_vv(
        state,
        crate::control::security::catalog::types::CheckpointDoc::new(
            database_id.as_u64(),
            tenant_id.as_u64(),
            &collection,
            &doc_id,
        ),
        &checkpoint_name,
    )?;

    let surrogate = crate::control::server::surrogate_exchange::assign_surrogate_routed(
        state,
        nodedb_types::CollectionKey::from_bare(database_id, &collection),
        tenant_id,
        doc_id.as_bytes(),
        crate::types::TraceId::ZERO,
    )
    .await
    .map_err(|e| DdlError::from_error_in_context("surrogate assign", &e))?;

    let timeout = Duration::from_secs(state.tuning.network.default_deadline_secs);
    // RLS write policies are stored keyed by `db_qualified(database_id,
    // collection)`, so the policy is handed that same key or it silently
    // misses a policy on a non-default database. The admission workflow
    // addresses the Data Plane by the same qualified name.
    let qualified_collection =
        crate::control::planner::sql_plan_convert::convert::db_qualified(database_id, &collection);
    let policy = ExternalCrdtPostImagePolicy::from_identity(
        tenant_id,
        database_id,
        &qualified_collection,
        identity,
        "sql".into(),
        &state.rls,
        &audit,
    );
    crate::control::crdt_admission::dispatch_crdt_restore_admitted(
        state,
        crate::control::crdt_admission::CrdtRestoreAdmissionRequest {
            tenant_id,
            database_id,
            collection: &qualified_collection,
            document_id: &doc_id,
            target_version_json: &vv_json,
            surrogate,
            peer_id: identity.user_id,
            timeout,
            event_source: crate::event::EventSource::User,
            policy: &policy,
        },
    )
    .await
    .map_err(|e| DdlError::from_error_in_context("restore dispatch", &e))?;

    state
        .audit
        .lock()
        .unwrap_or_else(|p| p.into_inner())
        .record(
            crate::control::security::audit::AuditEvent::AdminAction,
            Some(tenant_id),
            &identity.username,
            &format!("RESTORE {collection}/{doc_id} to version '{checkpoint_name}'"),
        );

    Ok(vec![DdlResult::Status {
        command: "RESTORE".to_string(),
        rows_affected: None,
    }])
}

/// Parameters for the legacy apply-only test seam.
#[cfg(test)]
struct RestoreDeltaParams<'a> {
    tenant_id: TenantId,
    database_id: DatabaseId,
    collection: &'a str,
    document_id: &'a str,
    surrogate: Surrogate,
    /// Attribution tag only (mirrors the local `crdt_apply` SQL path's
    /// `peer_id: identity.user_id` — see `neutral/crdt_ops.rs`). Never
    /// validated against the Loro-internal actor id embedded in `delta`.
    peer_id: u64,
    delta: Vec<u8>,
}

/// Route RESTORE's generated forward delta through the same serialized,
/// fenced CRDT admission boundary as every ordinary `CrdtOp::Apply`.
/// Loro imports are idempotent, so replaying the produced delta on the
/// local replica is safe while ensuring every replica observes the fence.
#[cfg(test)]
async fn persist_restore_delta(
    state: &SharedState,
    params: RestoreDeltaParams<'_>,
) -> crate::Result<Option<crate::types::Lsn>> {
    let RestoreDeltaParams {
        tenant_id,
        database_id,
        collection,
        document_id,
        surrogate,
        peer_id,
        delta,
    } = params;
    let qualified = nodedb_types::QualifiedCollection::new(database_id, collection);
    let plan = PhysicalPlan::Crdt(CrdtOp::Apply {
        collection: qualified.clone(),
        document_id: document_id.to_string(),
        delta,
        peer_id,
        mutation_id: 0,
        surrogate,
        provenance: None,
        constraint_version_required: 0,
        expected_frontier_digest: None,
    });

    let outcome = crate::control::crdt_admission::dispatch_crdt_apply_admitted_outcome(
        state,
        crate::control::crdt_admission::CrdtApplyAdmissionRequest {
            tenant_id,
            database_id,
            collection: qualified.as_str(),
            plan,
            timeout: Duration::from_secs(state.tuning.network.default_deadline_secs),
            event_source: crate::event::EventSource::User,
            policy: &crate::control::crdt_admission::TrustedInternalCrdtPolicy,
        },
    )
    .await?;
    Ok((outcome.write_version != crate::types::Lsn::ZERO).then_some(outcome.write_version))
}

/// Parse: RESTORE collection SET VERSION = 'checkpoint' WHERE id = 'doc-id'
fn parse_restore(sql: &str) -> Result<(String, String, String), DdlError> {
    let rest = sql["RESTORE ".len()..].trim();

    // Collection: before "SET VERSION"
    let set_pos = find_ascii_case_insensitive(rest, "SET VERSION")
        .ok_or_else(|| err("42601", "expected SET VERSION".to_string()))?;
    let collection = parse_ident_token(rest[..set_pos].trim())?;

    // Checkpoint: between "=" and "WHERE"
    let after_set = rest[set_pos + 11..].trim(); // After "SET VERSION"
    let eq_pos = after_set
        .find('=')
        .ok_or_else(|| err("42601", "expected '=' after SET VERSION".to_string()))?;
    let after_eq = after_set[eq_pos + 1..].trim();

    let where_pos = find_ascii_case_insensitive(after_eq, "WHERE")
        .ok_or_else(|| err("42601", "expected WHERE id = '<doc_id>'".to_string()))?;
    let checkpoint = after_eq[..where_pos]
        .trim()
        .trim_matches('\'')
        .trim_matches('"')
        .to_owned();

    // Doc ID from WHERE clause.
    let where_clause = after_eq[where_pos + 5..].trim();
    let id_eq = where_clause
        .find('=')
        .ok_or_else(|| err("42601", "expected 'id = <value>'".to_string()))?;
    let value_part = where_clause[id_eq + 1..]
        .trim()
        .trim_end_matches(';')
        .trim();
    let doc_id = value_part.trim_matches('\'').trim_matches('"').to_owned();

    Ok((collection, checkpoint, doc_id))
}

#[cfg(test)]
mod tests {
    use std::sync::{Arc, Mutex};

    use loro::LoroValue;
    use nodedb_crdt::state::CrdtState;

    use super::*;
    use crate::bridge::dispatch::{BridgeResponse, CoreChannelDataSide, Dispatcher};
    use crate::bridge::envelope::{Response, Status};
    use crate::types::Lsn;
    use crate::wal::WalManager;

    #[test]
    fn restore_keywords_after_unicode_values_preserve_original_offsets() {
        let (collection, checkpoint, doc_id) =
            parse_restore("RESTORE recordsﬀﬀ SET VERSION = 'versionﬀﬀ' WHERE id = 'doc-1'")
                .expect("restore statement should parse");
        assert_eq!(collection, "recordsﬀﬀ");
        assert_eq!(checkpoint, "versionﬀﬀ");
        assert_eq!(doc_id, "doc-1");
    }

    /// Build a `SharedState` with a real single-node `WalManager` and no Raft
    /// proposer configured, so `persist_restore_delta` takes the WAL-append
    /// fallback branch. The returned `TempDir` must outlive the state.
    async fn test_state() -> (Arc<SharedState>, CoreChannelDataSide, tempfile::TempDir) {
        let dir = tempfile::tempdir().expect("tempdir");
        let wal =
            Arc::new(WalManager::open_for_testing(&dir.path().join("test.wal")).expect("open wal"));
        let (dispatcher, mut data_sides) = Dispatcher::new(1, 64);
        let side = data_sides.pop().expect("one data side");
        let state = SharedState::new(dispatcher, wal).expect("shared state");
        (state, side, dir)
    }

    /// What the restore core expects and what it saw.
    #[derive(Clone)]
    struct RestoreCore {
        digest: [u8; 32],
        delta: Vec<u8>,
        seen: Arc<Mutex<Vec<&'static str>>>,
    }

    impl RestoreCore {
        fn seen(&self) -> Vec<&'static str> {
            self.seen.lock().unwrap_or_else(|p| p.into_inner()).clone()
        }
    }

    /// A Data Plane core for a one-node cluster that answers the restore
    /// admission: the generate returns `delta`, the preview returns a result
    /// with `digest`, and the fenced apply echoes its WAL LSN. It answers
    /// every other request, the cluster's Raft applies included, with `Ok`.
    /// It records each restore step it sees, marking a step whose delta or
    /// digest differs from the expected one.
    async fn answer_restore_admission(
        core: RestoreCore,
        state: Arc<SharedState>,
        mut side: CoreChannelDataSide,
    ) {
        let preview = zerompk::to_msgpack_vec(&nodedb_types::CrdtPreviewResult {
            post_image_msgpack: vec![0xc0],
            imported_ops: 1,
            trimmed_ops: 0,
            frontier_digest: core.digest,
        })
        .expect("preview payload");
        loop {
            while let Ok(request) = side.request_rx.try_pop() {
                let request = request.inner;
                let request_id = request.request_id;
                let wal_lsn = request.wal_lsn.unwrap_or(Lsn::ZERO);
                let (step, payload, read_version_lsn) = match request.plan {
                    PhysicalPlan::Crdt(CrdtOp::RestoreToVersion { .. }) => {
                        (Some("generate"), core.delta.clone(), Lsn::ZERO)
                    }
                    PhysicalPlan::Crdt(CrdtOp::PreviewApply { delta, .. }) => {
                        let step = if delta == core.delta {
                            "preview"
                        } else {
                            "preview: wrong delta"
                        };
                        (Some(step), preview.clone(), Lsn::ZERO)
                    }
                    PhysicalPlan::Crdt(CrdtOp::Apply {
                        delta,
                        expected_frontier_digest,
                        ..
                    }) => {
                        let step = if delta == core.delta
                            && expected_frontier_digest == Some(core.digest)
                        {
                            "apply"
                        } else {
                            "apply: wrong delta or digest"
                        };
                        (Some(step), Vec::new(), wal_lsn)
                    }
                    _ => (None, Vec::new(), Lsn::ZERO),
                };
                if let Some(step) = step {
                    core.seen
                        .lock()
                        .unwrap_or_else(|p| p.into_inner())
                        .push(step);
                }
                side.response_tx
                    .try_push(BridgeResponse {
                        inner: Response {
                            request_id,
                            status: Status::Ok,
                            attempt: 1,
                            partial: false,
                            payload: payload.into(),
                            watermark_lsn: Lsn::ZERO,
                            error_code: None,
                            stage_vote: None,
                            read_version_lsn,
                            write_set: Vec::new(),
                        },
                    })
                    .expect("response queue capacity");
            }
            state.poll_and_route_responses();
            tokio::task::yield_now().await;
        }
    }

    /// Generates the same read-only forward delta as `CrdtOp::RestoreToVersion`:
    /// upsert "v1", capture its version, upsert "v2", then preview restoration
    /// back to the "v1" version.
    ///
    /// Returns the pre-restore snapshot alongside the delta. A forward delta is
    /// exported relative to a version vector, so it only carries the ops after
    /// that point and is meaningful solely to a peer already holding everything
    /// before it — which is what replay reconstructs by importing every
    /// `CrdtDelta` record in order.
    fn real_restore_delta() -> (Vec<u8>, Vec<u8>) {
        let engine = CrdtState::new(1).expect("crdt state");
        engine
            .upsert("notes", "doc1", &[("body", LoroValue::String("v1".into()))])
            .expect("upsert v1");
        let vv1 = engine.oplog_version_vector();
        engine
            .upsert("notes", "doc1", &[("body", LoroValue::String("v2".into()))])
            .expect("upsert v2");
        let pre_restore = engine.export_snapshot().expect("pre-restore snapshot");
        let delta = engine
            .preview_restore_to_version("notes", "doc1", &vv1)
            .expect("restore to v1");
        (pre_restore, delta)
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn restore_delta_is_wal_durable_and_replays_to_post_restore_state() {
        let (pre_restore, delta) = real_restore_delta();
        assert!(
            !delta.is_empty(),
            "restoring to a genuinely different prior version must produce a non-empty forward delta"
        );

        let pre_restore_state = CrdtState::new(99).expect("pre-restore state");
        pre_restore_state
            .import(&pre_restore)
            .expect("import pre-restore state");
        let expected_frontier = nodedb_crdt::state::frontier_digest::domain_frontier_digest(
            5,
            DatabaseId::DEFAULT.as_u64(),
            "notes",
            Some(&pre_restore_state),
        );
        let core = RestoreCore {
            digest: expected_frontier,
            delta: delta.clone(),
            seen: Arc::new(Mutex::new(Vec::new())),
        };
        let serving = core.clone();
        let cluster = crate::control::cluster::test_one_node::boot_with_core(
            |_| {},
            move |state, side| tokio::spawn(answer_restore_admission(serving, state, side)),
        )
        .await;
        let state = Arc::clone(&cluster.state);
        let outcome = crate::control::crdt_admission::dispatch_crdt_restore_admitted(
            &state,
            crate::control::crdt_admission::CrdtRestoreAdmissionRequest {
                tenant_id: TenantId::new(5),
                database_id: DatabaseId::DEFAULT,
                collection: "notes",
                document_id: "doc1",
                target_version_json: "{}",
                surrogate: Surrogate(1),
                peer_id: 42,
                timeout: Duration::from_secs(2),
                event_source: crate::event::EventSource::User,
                policy: &crate::control::crdt_admission::TrustedInternalCrdtPolicy,
            },
        )
        .await
        .expect("persist restore delta");
        let lsn = outcome.map(|outcome| outcome.write_version);
        assert_eq!(
            core.seen(),
            ["generate", "preview", "apply"],
            "restore admission must generate, preview, then apply"
        );
        assert!(
            lsn.is_some(),
            "a restore must allocate and return a durable WAL LSN"
        );

        state.wal.sync().expect("sync wal");
        let records: Vec<_> = state
            .wal
            .replay()
            .expect("replay wal")
            .into_iter()
            .filter(|record| {
                record.header.record_type == nodedb_wal::record::RecordType::CrdtDelta as u32
            })
            .collect();
        assert_eq!(
            records.len(),
            1,
            "exactly one CrdtDelta record must be appended for the restore"
        );
        let payload = crate::wal::CrdtDeltaWalPayload::decode(&records[0].payload)
            .expect("decode wal payload");
        assert_eq!(payload.expected_frontier_digest, Some(expected_frontier));
        assert_eq!(
            payload.bytes, delta,
            "the WAL record must carry the exact delta bytes the restore handler produced"
        );
        assert_eq!(payload.collection, "notes");

        // Replay via the same idempotent Loro import `replay_crdt_wal` performs
        // in production, and confirm the result is the POST-restore value
        // ("v1"), not the pre-restore value ("v2"). The peer is first brought up
        // to the pre-restore state, mirroring replay importing every earlier
        // delta before this one.
        let fresh = CrdtState::new(99).expect("fresh crdt state");
        fresh
            .import(&pre_restore)
            .expect("import pre-restore state");
        fresh.import(&payload.bytes).expect("import replayed delta");
        let restored = fresh
            .read_field("notes", "doc1", "body")
            .expect("row must exist after replay");
        assert_eq!(restored, LoroValue::String("v1".into()));
        drop(state);
        cluster.shutdown().await;
    }

    #[tokio::test]
    async fn restore_to_current_version_appends_nothing() {
        let engine = CrdtState::new(1).expect("crdt state");
        engine
            .upsert("notes", "doc1", &[("body", LoroValue::String("v1".into()))])
            .expect("upsert v1");
        let current = engine.oplog_version_vector();

        // Restoring to the version the document is already at is a true
        // no-op: `restore_to_version` compares the historical projection
        // against the live row before mutating anything and short-circuits
        // with an empty delta when they already match.
        let delta = engine
            .restore_to_version("notes", "doc1", &current)
            .expect("restore to current version");
        assert!(
            delta.is_empty(),
            "restoring a document to the version it is already at must produce an empty delta"
        );

        // Mirrors `restore_version`'s `if !delta.is_empty()` gate.
        let (state, _side, _dir) = test_state().await;
        if !delta.is_empty() {
            persist_restore_delta(
                &state,
                RestoreDeltaParams {
                    tenant_id: TenantId::new(6),
                    database_id: DatabaseId::DEFAULT,
                    collection: "notes",
                    document_id: "doc1",
                    surrogate: Surrogate(1),
                    peer_id: 42,
                    delta,
                },
            )
            .await
            .expect("persist restore delta");
        }

        state.wal.sync().expect("sync wal");
        let records = state.wal.replay().expect("replay wal");
        assert!(
            records.is_empty(),
            "a no-op restore must append nothing to the WAL"
        );
    }
}
