// SPDX-License-Identifier: BUSL-1.1

//! CRDT serializer for transaction resolve.
//!
//! **Plan-driven.** A CRDT write buffered inside a transaction is an intent —
//! a document-row upsert or delete, a block-list mutation, a snapshot import —
//! that the live handler re-executes against the collection's Loro state. The
//! sub-record is the exact record the autocommit path journals for the same op
//! (`wal_dispatch::encode_crdt_op_record`), so the CRDT replay arm re-executes
//! it on restart and on every replica applying the committed record.
//!
//! A raw `Apply` / `ApplyAuthenticated` delta is refused inside a transaction
//! before it is buffered; reaching resolve is a typed error, never a silent
//! drop. Reads, constraint installs, policy changes and history compaction
//! emit nothing.

use nodedb_physical::physical_plan::CrdtOp;

use crate::control::server::wal_dispatch::encode_crdt_op_record;
use crate::wal::RedoSubRecord;

/// Append the redo sub-record for a single CRDT plan op to `ops`.
pub(super) fn serialize_crdt_op(op: &CrdtOp, ops: &mut Vec<RedoSubRecord>) -> crate::Result<()> {
    if matches!(op, CrdtOp::Apply { .. } | CrdtOp::ApplyAuthenticated { .. }) {
        return Err(crate::Error::PlanError {
            detail: "CRDT Apply is not supported inside a transaction".to_string(),
        });
    }
    if let Some((kind, payload)) = encode_crdt_op_record(op)? {
        ops.push(RedoSubRecord {
            record_type: kind.record_type() as u32,
            payload,
        });
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use nodedb_physical::physical_plan::CrdtWriteVerb;
    use nodedb_types::{DatabaseId, QualifiedCollection, Surrogate};
    use nodedb_wal::record::RecordType;

    #[test]
    fn doc_upsert_resolves_to_a_crdt_doc_op_sub_record() {
        let op = CrdtOp::DocUpsert {
            collection: QualifiedCollection::new(DatabaseId::DEFAULT, "notes"),
            document_id: "n1".to_string(),
            fields_json: r#"{"title":"a"}"#.to_string(),
            surrogate: Surrogate::new(5),
            partial: false,
            verb: CrdtWriteVerb::Insert,
            returning: None,
            rls_filters: Vec::new(),
        };
        let mut ops = Vec::new();
        serialize_crdt_op(&op, &mut ops).expect("resolve crdt doc upsert");
        assert_eq!(ops.len(), 1);
        assert_eq!(ops[0].record_type, RecordType::CrdtDocOp as u32);
    }

    #[test]
    fn raw_delta_apply_is_refused() {
        let op = CrdtOp::Apply {
            collection: QualifiedCollection::new(DatabaseId::DEFAULT, "notes"),
            document_id: "n1".to_string(),
            delta: vec![1],
            peer_id: 1,
            mutation_id: 1,
            surrogate: Surrogate::new(5),
            provenance: None,
            constraint_version_required: 0,
            expected_frontier_digest: None,
        };
        let mut ops = Vec::new();
        assert!(serialize_crdt_op(&op, &mut ops).is_err());
        assert!(ops.is_empty());
    }
}
