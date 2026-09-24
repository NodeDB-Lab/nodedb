// SPDX-License-Identifier: BUSL-1.1

//! WAL append dispatch for `PhysicalPlan::Crdt(CrdtOp)`.

#![deny(clippy::wildcard_enum_match_arm)]

use nodedb_physical::physical_plan::CrdtOp;
use nodedb_wal::record::RecordType;

use crate::types::{DatabaseId, Lsn, TenantId, VShardId};
use crate::wal::manager::WalAppender;

/// Which CRDT WAL record class a `CrdtOp` write journals as.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum CrdtRecordKind {
    /// A Loro delta or snapshot import.
    Delta,
    /// A block-list mutation intent.
    ListOp,
    /// A document-row mutation intent.
    DocOp,
}

impl CrdtRecordKind {
    /// The WAL record type this class is journalled under.
    pub(crate) fn record_type(self) -> RecordType {
        match self {
            Self::Delta => RecordType::CrdtDelta,
            Self::ListOp => RecordType::CrdtListOp,
            Self::DocOp => RecordType::CrdtDocOp,
        }
    }
}

/// Append the WAL record for a single `CrdtOp`, returning the allocated LSN for
/// the delta / snapshot-import / block-list write variants (`Some`) or `None`
/// for every read / constraint / policy variant that carries no durable
/// per-write effect on THIS path.
pub(super) fn wal_append_crdt_op(
    wal: WalAppender<'_>,
    tenant_id: TenantId,
    vshard_id: VShardId,
    database_id: DatabaseId,
    op: &CrdtOp,
) -> crate::Result<Option<Lsn>> {
    let Some((kind, payload)) = encode_crdt_op_record(op)? else {
        return Ok(None);
    };
    let lsn = match kind {
        CrdtRecordKind::Delta => {
            wal.append_crdt_delta(tenant_id, vshard_id, database_id, &payload)?
        }
        CrdtRecordKind::ListOp => {
            wal.append_crdt_list_op(tenant_id, vshard_id, database_id, &payload)?
        }
        CrdtRecordKind::DocOp => {
            wal.append_crdt_doc_op(tenant_id, vshard_id, database_id, &payload)?
        }
    };
    Ok(Some(lsn))
}

/// Encode the WAL record a single `CrdtOp` write journals as: its record class
/// and payload. `None` for every read / constraint / policy variant that
/// carries no durable per-write effect on this path.
///
/// The match over [`CrdtOp`] is **exhaustive** (`wildcard_enum_match_arm` is
/// denied), so a future write variant cannot silently become non-durable.
/// Shared by the autocommit WAL append and the transaction resolver, so a
/// CRDT write inside a transaction journals the exact record its autocommit
/// form does.
pub(crate) fn encode_crdt_op_record(
    op: &CrdtOp,
) -> crate::Result<Option<(CrdtRecordKind, Vec<u8>)>> {
    let appended = match op {
        CrdtOp::Apply {
            collection,
            document_id,
            delta,
            surrogate,
            provenance,
            expected_frontier_digest,
            peer_id,
            ..
        } => {
            // Versioned payload preserves the admission fence for deterministic
            // crash replay; legacy records decode without a fence.
            let payload = crate::wal::CrdtDeltaWalPayload::new(
                delta.clone(),
                Some(collection.to_string()),
                provenance.clone(),
                *expected_frontier_digest,
                Some(document_id.clone()),
                Some(surrogate.as_u32()),
            )
            .with_peer_id(*peer_id);
            let crdt_payload = payload.encode().map_err(|e| crate::Error::Serialization {
                format: "msgpack".into(),
                detail: format!("wal crdt delta: {e}"),
            })?;
            Some((CrdtRecordKind::Delta, crdt_payload))
        }
        CrdtOp::ApplyAuthenticated {
            collection,
            document_id,
            delta,
            surrogate,
            provenance,
            expected_frontier_digest,
            auth_user_id,
            auth_device_id,
            auth_seq_no,
            delta_signature,
            signing_required,
            peer_id,
            ..
        } => {
            let payload = crate::wal::CrdtDeltaWalPayload::new(
                delta.clone(),
                Some(collection.to_string()),
                Some(provenance.clone()),
                *expected_frontier_digest,
                Some(document_id.clone()),
                Some(surrogate.as_u32()),
            )
            .with_signing(crate::wal::CrdtDeltaSigning {
                auth_user_id: *auth_user_id,
                auth_device_id: *auth_device_id,
                auth_seq_no: *auth_seq_no,
                delta_signature: *delta_signature,
                required: *signing_required,
            })
            .with_peer_id(*peer_id);
            let crdt_payload = payload.encode().map_err(|e| crate::Error::Serialization {
                format: "msgpack".into(),
                detail: format!("wal authenticated crdt delta: {e}"),
            })?;
            Some((CrdtRecordKind::Delta, crdt_payload))
        }
        CrdtOp::ImportSnapshot {
            collection, bytes, ..
        } => {
            // Per-collection snapshot import. `import_snapshot_bytes` and
            // `apply_committed_delta` are the same idempotent Loro `state.import`,
            // so the snapshot rides the CRDT delta record and replays identically,
            // routed to the same collection. No provenance (not a per-doc sync op).
            let payload = crate::wal::CrdtDeltaWalPayload::new(
                bytes.clone(),
                Some(collection.to_string()),
                None,
                None,
                None,
                None,
            );
            let crdt_payload = payload.encode().map_err(|e| crate::Error::Serialization {
                format: "msgpack".into(),
                detail: format!("wal crdt snapshot import: {e}"),
            })?;
            Some((CrdtRecordKind::Delta, crdt_payload))
        }
        CrdtOp::ListInsert {
            collection,
            document_id,
            list_path,
            index,
            fields_json,
            surrogate: _,
        } => {
            // The Data Plane never appends to the WAL and the Control Plane
            // has no `LoroDoc` to compute a delta from, so the intent is
            // logged here and re-executed deterministically at replay
            // (see `CrdtListOpWalRecord`'s doc comment).
            let payload = crate::wal::CrdtListOpWalRecord::Insert {
                collection: collection.to_string(),
                document_id: document_id.clone(),
                list_path: list_path.clone(),
                index: *index as u64,
                fields_json: fields_json.clone(),
            };
            let bytes = encode_crdt_list_op_payload(payload)?;
            Some((CrdtRecordKind::ListOp, bytes))
        }
        CrdtOp::ListDelete {
            collection,
            document_id,
            list_path,
            index,
            surrogate: _,
        } => {
            let payload = crate::wal::CrdtListOpWalRecord::Delete {
                collection: collection.to_string(),
                document_id: document_id.clone(),
                list_path: list_path.clone(),
                index: *index as u64,
            };
            let bytes = encode_crdt_list_op_payload(payload)?;
            Some((CrdtRecordKind::ListOp, bytes))
        }
        CrdtOp::ListMove {
            collection,
            document_id,
            list_path,
            from_index,
            to_index,
            surrogate: _,
        } => {
            let payload = crate::wal::CrdtListOpWalRecord::Move {
                collection: collection.to_string(),
                document_id: document_id.clone(),
                list_path: list_path.clone(),
                from_index: *from_index as u64,
                to_index: *to_index as u64,
            };
            let bytes = encode_crdt_list_op_payload(payload)?;
            Some((CrdtRecordKind::ListOp, bytes))
        }
        CrdtOp::DocUpsert {
            collection,
            document_id,
            fields_json,
            surrogate,
            partial,
            verb: _,
            returning: _,
            rls_filters: _,
        } => {
            // Intent-logged like the block-list ops: the Data Plane builds the
            // Loro mutation and the Control Plane has no `LoroDoc`, so the
            // record carries the fields + partial flag and replay re-executes
            // the live handler (see `CrdtDocOpWalRecord`'s doc comment).
            let payload = crate::wal::CrdtDocOpWalRecord::Upsert {
                collection: collection.to_string(),
                document_id: document_id.clone(),
                surrogate: surrogate.as_u32(),
                fields_json: fields_json.clone(),
                partial: *partial,
            };
            let bytes = encode_crdt_doc_op_payload(payload)?;
            Some((CrdtRecordKind::DocOp, bytes))
        }
        CrdtOp::DocDelete {
            collection,
            document_id,
            surrogate,
            returning: _,
            rls_filters: _,
        } => {
            let payload = crate::wal::CrdtDocOpWalRecord::Delete {
                collection: collection.to_string(),
                document_id: document_id.clone(),
                surrogate: surrogate.as_u32(),
            };
            let bytes = encode_crdt_doc_op_payload(payload)?;
            Some((CrdtRecordKind::DocOp, bytes))
        }
        // NotAWrite — reads / query ops / DDL that produces no engine mutation here
        CrdtOp::Read { .. }
        | CrdtOp::PreviewApply { .. }
        | CrdtOp::ReadConstraints { .. }
        | CrdtOp::GetPolicy { .. }
        | CrdtOp::SetPolicy { .. }
        | CrdtOp::ReadAtVersion { .. }
        | CrdtOp::GetVersionVector { .. }
        | CrdtOp::ExportDelta { .. } => None,
        // DurableElsewhere — the op discards durable oplog entries, and it
        // replicates as a `CompactHistory` catalog entry that every node
        // dispatches from post-apply, never through this oracle
        CrdtOp::CompactAtVersion { .. } => None,
        // DurableElsewhere — installed set is Raft-log-replay durable in cluster
        // mode; applied via SPSC from the distributed applier, never through this
        // oracle
        CrdtOp::SetConstraints { .. } | CrdtOp::DropConstraints { .. } => None,
        // DurableElsewhere — dispatched only to compute a forward delta; the delta
        // is logged+replicated via a follow-up CrdtOp::Apply
        CrdtOp::RestoreToVersion { .. } => None,
    };
    Ok(appended)
}

/// Encode a `CrdtListOpWalRecord` for a `CrdtOp::ListInsert` / `ListDelete` /
/// `ListMove` append. Shared by all three arms above so the msgpack encode +
/// error-mapping logic is written once (mirrors `encode_graph_node_label_payload`).
fn encode_crdt_list_op_payload(payload: crate::wal::CrdtListOpWalRecord) -> crate::Result<Vec<u8>> {
    zerompk::to_msgpack_vec(&payload).map_err(|e| crate::Error::Serialization {
        format: "msgpack".into(),
        detail: format!("wal crdt list op: {e}"),
    })
}

/// Encode a `CrdtDocOpWalRecord` for a `CrdtOp::DocUpsert` / `DocDelete`
/// append. Shared by both arms above so the msgpack encode + error-mapping
/// logic is written once.
fn encode_crdt_doc_op_payload(payload: crate::wal::CrdtDocOpWalRecord) -> crate::Result<Vec<u8>> {
    zerompk::to_msgpack_vec(&payload).map_err(|e| crate::Error::Serialization {
        format: "msgpack".into(),
        detail: format!("wal crdt doc op: {e}"),
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::wal::manager::WalManager;
    use nodedb_physical::physical_plan::PhysicalPlan;
    use nodedb_types::{QualifiedCollection, Surrogate};

    fn open_wal(dir: &std::path::Path) -> WalManager {
        WalManager::open_for_testing(&dir.join("test.wal")).expect("open wal")
    }

    fn has_record_of_type(wal: &WalManager, record_type: nodedb_wal::record::RecordType) -> bool {
        wal.sync().expect("sync wal");
        wal.replay().expect("read wal").into_iter().any(|r| {
            nodedb_wal::record::RecordType::from_raw(r.logical_record_type()) == Some(record_type)
        })
    }

    #[test]
    fn apply_appends_crdt_delta_record() {
        let dir = tempfile::tempdir().expect("tempdir");
        let wal = open_wal(dir.path());
        let plan = PhysicalPlan::Crdt(CrdtOp::Apply {
            collection: QualifiedCollection::new(DatabaseId::DEFAULT, "docs"),
            document_id: "d1".to_string(),
            delta: vec![9, 9, 9],
            peer_id: 1,
            mutation_id: 1,
            surrogate: Surrogate::new(3),
            provenance: None,
            constraint_version_required: 0,
            expected_frontier_digest: None,
        });

        let outcome = super::super::wal_append_if_write(
            &wal,
            TenantId::new(1),
            VShardId::new(0),
            DatabaseId::DEFAULT,
            &plan,
        )
        .expect("append");
        assert!(outcome.lsn.is_some(), "Apply must produce a durable LSN");
        assert!(has_record_of_type(
            &wal,
            nodedb_wal::record::RecordType::CrdtDelta
        ));
    }

    #[test]
    fn list_insert_appends_crdt_list_op_record() {
        let dir = tempfile::tempdir().expect("tempdir");
        let wal = open_wal(dir.path());
        let plan = PhysicalPlan::Crdt(CrdtOp::ListInsert {
            collection: QualifiedCollection::new(DatabaseId::DEFAULT, "docs"),
            document_id: "d1".to_string(),
            list_path: "blocks".to_string(),
            index: 0,
            fields_json: "{}".to_string(),
            surrogate: Surrogate::new(3),
        });

        let outcome = super::super::wal_append_if_write(
            &wal,
            TenantId::new(1),
            VShardId::new(0),
            DatabaseId::DEFAULT,
            &plan,
        )
        .expect("append");
        assert!(
            outcome.lsn.is_some(),
            "ListInsert must produce a durable LSN"
        );
        assert!(has_record_of_type(
            &wal,
            nodedb_wal::record::RecordType::CrdtListOp
        ));
    }

    #[test]
    fn doc_upsert_appends_crdt_doc_op_record() {
        let dir = tempfile::tempdir().expect("tempdir");
        let wal = open_wal(dir.path());
        let plan = PhysicalPlan::Crdt(CrdtOp::DocUpsert {
            collection: QualifiedCollection::new(DatabaseId::DEFAULT, "docs"),
            document_id: "d1".to_string(),
            fields_json: r#"{"a":1}"#.to_string(),
            surrogate: Surrogate::new(3),
            partial: false,
            verb: nodedb_physical::physical_plan::CrdtWriteVerb::Insert,
            returning: None,
            rls_filters: Vec::new(),
        });

        let outcome = super::super::wal_append_if_write(
            &wal,
            TenantId::new(1),
            VShardId::new(0),
            DatabaseId::DEFAULT,
            &plan,
        )
        .expect("append");
        assert!(
            outcome.lsn.is_some(),
            "DocUpsert must produce a durable LSN"
        );
        assert!(has_record_of_type(
            &wal,
            nodedb_wal::record::RecordType::CrdtDocOp
        ));
    }

    #[test]
    fn read_op_appends_nothing() {
        let dir = tempfile::tempdir().expect("tempdir");
        let wal = open_wal(dir.path());
        let plan = PhysicalPlan::Crdt(CrdtOp::Read {
            collection: QualifiedCollection::new(DatabaseId::DEFAULT, "docs"),
            document_id: "d1".to_string(),
        });

        let outcome = super::super::wal_append_if_write(
            &wal,
            TenantId::new(1),
            VShardId::new(0),
            DatabaseId::DEFAULT,
            &plan,
        )
        .expect("append");
        assert!(outcome.lsn.is_none(), "read op must produce no durable LSN");
    }
}
