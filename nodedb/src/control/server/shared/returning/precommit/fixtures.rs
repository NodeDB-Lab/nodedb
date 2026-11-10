// SPDX-License-Identifier: BUSL-1.1

//! Shared builders for the precommit tests.

use nodedb_query::expr::SqlExpr;
use nodedb_types::Value;

use crate::control::security::catalog::sequence_types::StoredSequence;
use crate::control::sequence::SequenceRegistry;
use crate::control::server::response_shape::types::ShapedRow;
use crate::control::server::shared::returning::ReturningItem;
use crate::control::server::shared::returning::transcript::AccessorTranscript;

use super::{
    FrozenReturningReply, PrecommitReturning, PreparedReturningRow, PreparedRowIdentity,
    ReturningLimits,
};

pub(super) fn prepared_rows(rows: Vec<ShapedRow>) -> Vec<PreparedReturningRow> {
    rows.into_iter()
        .enumerate()
        .map(|(index, fields)| PreparedReturningRow {
            identity: PreparedRowIdentity {
                collection: "users".into(),
                stable_row_key: index.to_be_bytes().to_vec(),
                operation_index: index as u64,
            },
            fields,
        })
        .collect()
}

pub(super) fn finalize(
    request: &PrecommitReturning<'_>,
    rows: Vec<ShapedRow>,
    transcript: &AccessorTranscript,
) -> crate::Result<FrozenReturningReply> {
    super::finalize_returning(request, prepared_rows(rows), transcript)
}

pub(super) fn limits() -> ReturningLimits {
    ReturningLimits {
        rows: 100,
        columns: 100,
        cells: 1000,
        payload_bytes: 100_000,
    }
}

pub(super) fn computed(name: &str, expression: SqlExpr) -> ReturningItem {
    ReturningItem::Computed {
        name: name.into(),
        expression: zerompk::to_msgpack_vec(&expression).expect("encode expression"),
    }
}

pub(super) fn call(name: &str, args: Vec<SqlExpr>) -> SqlExpr {
    SqlExpr::Function {
        name: name.into(),
        args,
    }
}

pub(super) fn literal(value: Value) -> SqlExpr {
    SqlExpr::Literal(value)
}

pub(super) fn registry() -> SequenceRegistry {
    let registry = SequenceRegistry::new();
    for name in ["s", "other"] {
        registry
            .create(StoredSequence::new(1, 7, name.into(), "alice".into()))
            .expect("create sequence");
    }
    registry
}
