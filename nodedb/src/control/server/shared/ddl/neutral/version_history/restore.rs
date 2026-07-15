// SPDX-License-Identifier: BUSL-1.1

//! RESTORE collection SET VERSION = 'checkpoint' WHERE id = 'doc-id'

use std::time::Duration;

use crate::bridge::envelope::PhysicalPlan;
use crate::control::security::identity::AuthenticatedIdentity;
use crate::control::server::shared::ddl::sync_dispatch::dispatch_async;
use crate::control::server::wal_dispatch::wal_append_if_write;
use crate::control::state::SharedState;
use crate::control::wal_replication::{propose_replicated_entry, to_replicated_entry};
use crate::types::{DatabaseId, TenantId, VShardId};
use nodedb_physical::physical_plan::CrdtOp;
use nodedb_sql::parser::preprocess::lex::find_ascii_case_insensitive;
use nodedb_types::Surrogate;

use super::super::super::result::{DdlError, DdlResult};

fn err(sqlstate: &str, message: String) -> DdlError {
    DdlError {
        sqlstate: sqlstate.to_string(),
        message,
    }
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

    let vv_json = super::at_version::resolve_checkpoint_vv(
        state,
        tenant_id.as_u64(),
        &collection,
        &doc_id,
        &checkpoint_name,
    )?;

    let surrogate = state
        .surrogate_assigner
        .assign(database_id, tenant_id, &collection, doc_id.as_bytes())
        .map_err(|e| err("XX000", format!("surrogate assign: {e}")))?;

    let plan = PhysicalPlan::Crdt(CrdtOp::RestoreToVersion {
        collection: collection.clone(),
        document_id: doc_id.clone(),
        target_version_json: vv_json,
        surrogate,
    });
    let timeout = Duration::from_secs(state.tuning.network.default_deadline_secs);
    let delta = dispatch_async(state, tenant_id, database_id, &collection, plan, timeout)
        .await
        .map_err(|e| err("XX000", format!("restore dispatch: {e}")))?;

    // `execute_crdt_restore` -> `restore_to_version` already mutated the live
    // Loro doc directly and returned the forward delta as its response
    // payload (see `data/executor/handlers/control/crdt.rs`). An empty delta
    // means the target version equals the current state (no-op restore) —
    // nothing to log or replicate. A non-empty delta must still be made
    // durable and cluster-visible, or a crash / follower read silently
    // reverts to the pre-restore state after the client was told it
    // succeeded.
    if !delta.is_empty() {
        let _lsn = persist_restore_delta(
            state,
            RestoreDeltaParams {
                tenant_id,
                database_id,
                collection: &collection,
                document_id: &doc_id,
                surrogate,
                peer_id: identity.user_id,
                delta,
            },
        )
        .await
        .map_err(|e| err("XX000", format!("restore delta persist: {e}")))?;
    }

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

/// Parameters for [`persist_restore_delta`].
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

/// Durably log and, on a cluster, replicate the forward delta a completed
/// RESTORE already applied directly to the local Loro doc.
///
/// The delta is wrapped as a `CrdtOp::Apply` plan solely so it can be fed
/// through the exact WAL-append (`wal_append_if_write`) and Raft-encode
/// (`to_replicated_entry`) machinery `CrdtOp::Apply` already uses — this
/// plan is never dispatched to the local Data Plane a second time. `surrogate`
/// and `mutation_id` are carried only to satisfy the plan's shape: neither
/// `wal_append_if_write`'s `CrdtOp::Apply` arm nor `to_replicated_entry`'s
/// reads them, and on a cluster every replica (including this one, via the
/// Raft apply loop) re-derives its own surrogate binding for `document_id`
/// deterministically rather than trusting the wire value.
///
/// Ordering is apply-then-log, not write-ahead, because the delta does not
/// exist until the Data-Plane handler has already produced it: a crash
/// before this call loses the same in-memory mutation a WAL record would
/// have described, and a crash after it replays (or a follower re-applies)
/// an idempotent, commutative Loro delta — the two converge either way.
///
/// Returns the WAL LSN allocated on the single-node path (`Some`), or `None`
/// on the cluster path — durability there comes from the Raft quorum commit
/// `propose_replicated_entry` awaits, not from this node's own local WAL.
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
    let vshard_id = VShardId::from_collection_in_database(database_id, collection);
    let plan = PhysicalPlan::Crdt(CrdtOp::Apply {
        collection: collection.to_string(),
        document_id: document_id.to_string(),
        delta,
        peer_id,
        mutation_id: 0,
        surrogate,
        provenance: None,
        constraint_version_required: 0,
    });

    if let Some(proposer) = state.async_raft_proposer.get() {
        let entry =
            to_replicated_entry(tenant_id, database_id, vshard_id, &plan).ok_or_else(|| {
                crate::Error::Internal {
                    detail: "restore: crdt apply delta did not map to a replicated write".into(),
                }
            })?;
        propose_replicated_entry(state, proposer, entry).await?;
        return Ok(None);
    }

    let outcome = wal_append_if_write(&state.wal, tenant_id, vshard_id, database_id, &plan)?;
    Ok(outcome.lsn)
}

/// Parse: RESTORE collection SET VERSION = 'checkpoint' WHERE id = 'doc-id'
fn parse_restore(sql: &str) -> Result<(String, String, String), DdlError> {
    let rest = sql["RESTORE ".len()..].trim();

    // Collection: before "SET VERSION"
    let set_pos = find_ascii_case_insensitive(rest, "SET VERSION")
        .ok_or_else(|| err("42601", "expected SET VERSION".to_string()))?;
    let collection = rest[..set_pos].trim().to_lowercase();

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
    use loro::LoroValue;
    use nodedb_crdt::state::CrdtState;

    use super::*;
    use crate::bridge::dispatch::Dispatcher;
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
    async fn test_state() -> (std::sync::Arc<SharedState>, tempfile::TempDir) {
        let dir = tempfile::tempdir().expect("tempdir");
        let wal = std::sync::Arc::new(
            WalManager::open_for_testing(&dir.path().join("test.wal")).expect("open wal"),
        );
        let (dispatcher, _data_sides) = Dispatcher::new(1, 64);
        let state = SharedState::new(dispatcher, wal).expect("shared state");
        (state, dir)
    }

    /// Drives the real `restore_to_version` handler (the function backing
    /// `CrdtOp::RestoreToVersion`) to obtain a genuine forward delta, instead
    /// of hand-rolling one: upsert "v1", capture its version, upsert "v2",
    /// then restore back to the "v1" version.
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
            .restore_to_version("notes", "doc1", &vv1)
            .expect("restore to v1");
        (pre_restore, delta)
    }

    #[tokio::test]
    async fn restore_delta_is_wal_durable_and_replays_to_post_restore_state() {
        let (pre_restore, delta) = real_restore_delta();
        assert!(
            !delta.is_empty(),
            "restoring to a genuinely different prior version must produce a non-empty forward delta"
        );

        let (state, _dir) = test_state().await;
        let lsn = persist_restore_delta(
            &state,
            RestoreDeltaParams {
                tenant_id: TenantId::new(5),
                database_id: DatabaseId::DEFAULT,
                collection: "notes",
                document_id: "doc1",
                surrogate: Surrogate(1),
                peer_id: 42,
                delta: delta.clone(),
            },
        )
        .await
        .expect("persist restore delta");
        assert!(
            lsn.is_some(),
            "the current bug: RESTORE dispatches with wal_lsn: None and appends nothing; \
             a fixed single-node path must allocate and return a durable WAL LSN"
        );

        state.wal.sync().expect("sync wal");
        let records = state.wal.replay().expect("replay wal");
        assert_eq!(
            records.len(),
            1,
            "exactly one CrdtDelta record must be appended for the restore"
        );
        assert_eq!(
            records[0].header.record_type,
            nodedb_wal::record::RecordType::CrdtDelta as u32
        );
        let payload = zerompk::from_msgpack::<crate::wal::CrdtDeltaWalPayload>(&records[0].payload)
            .expect("decode wal payload");
        assert_eq!(
            payload.bytes, delta,
            "the WAL record must carry the exact delta bytes the restore handler produced"
        );
        assert_eq!(payload.collection.as_deref(), Some("notes"));

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
        let (state, _dir) = test_state().await;
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
