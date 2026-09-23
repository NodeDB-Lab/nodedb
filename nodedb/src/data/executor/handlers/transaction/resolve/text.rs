// SPDX-License-Identifier: BUSL-1.1

//! Full-text serializer for transaction resolve.
//!
//! **Plan-driven.** An `FtsIndexDoc` / `FtsDeleteDoc` buffered inside a
//! transaction carries the complete posting input (collection, surrogate,
//! text), so it resolves to the exact `FtsIndex` / `FtsDelete` record its
//! autocommit form journals (`wal_dispatch::encode_text_op_record`). A row the
//! same transaction also writes as a document re-derives the same postings at
//! install; an index upsert for one document is idempotent, so the two agree.
//! Searches and analyzer configuration emit nothing.

use nodedb_physical::physical_plan::TextOp;

use crate::control::server::wal_dispatch::encode_text_op_record;
use crate::wal::RedoSubRecord;

/// Append the redo sub-record for a single text plan op to `ops`.
pub(super) fn serialize_text_op(op: &TextOp, ops: &mut Vec<RedoSubRecord>) -> crate::Result<()> {
    if let Some((record_type, payload)) = encode_text_op_record(op)? {
        ops.push(RedoSubRecord {
            record_type: record_type as u32,
            payload,
        });
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use nodedb_types::{DatabaseId, QualifiedCollection, Surrogate};
    use nodedb_wal::record::RecordType;

    #[test]
    fn fts_index_resolves_to_an_fts_index_sub_record() {
        let op = TextOp::FtsIndexDoc {
            collection: QualifiedCollection::new(DatabaseId::DEFAULT, "docs"),
            surrogate: Surrogate::new(7),
            text: "hello world".to_string(),
            provenance: None,
        };
        let mut ops = Vec::new();
        serialize_text_op(&op, &mut ops).expect("resolve fts index");
        assert_eq!(ops.len(), 1);
        assert_eq!(ops[0].record_type, RecordType::FtsIndex as u32);
    }
}
