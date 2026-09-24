// SPDX-License-Identifier: BUSL-1.1

//! Native sorted-index read opcodes.
//!
//! `KvSortedIndexRank`, `KvSortedIndexTopK`, `KvSortedIndexRange`,
//! `KvSortedIndexCount` and `KvSortedIndexScore` name only an index. They run
//! through the same gated read as the SQL functions `RANK`, `TOPK`, `RANGE`
//! and `SORTED_COUNT`:
//!
//! - The caller must hold `Read` on the collection the index covers. The
//!   index registry names that collection.
//! - The read goes to the core that holds the collection's rows.
//! - Inside an explicit transaction the read sees the transaction's own
//!   writes and an index the transaction created.
//!
//! The reply keeps the Data Plane shape each opcode has always answered with.

use nodedb_physical::physical_plan::SortedIndexRead;
use nodedb_types::protocol::{NativeResponse, OpCode, TextFields};

use crate::control::server::shared::ddl::neutral::kv_sorted_index::run_read;
use crate::control::server::shared::session::DmlTxnCtx;

use super::response::data_plane_response_to_native;
use super::{DispatchCtx, ddl_result_to_native, error_to_native, error_to_native_with_sqlstate};

/// Run the sorted-index read opcode `op`.
pub(crate) async fn handle_sorted_read_op(
    ctx: &DispatchCtx<'_>,
    seq: u64,
    op: OpCode,
    fields: &TextFields,
) -> NativeResponse {
    // Per-operation caps (top_k and the like), as every direct read has.
    if let Err(e) = super::limits::check_op_limits(ctx.state, fields) {
        return error_to_native_with_sqlstate(seq, "0A000", &e);
    }
    if let Err(e) = ctx.state.check_tenant_quota(ctx.tenant_id()) {
        return error_to_native(seq, &e);
    }
    let (index_name, read) = match sorted_read(op, fields) {
        Ok(parsed) => parsed,
        Err(e) => return error_to_native_with_sqlstate(seq, "42601", &e),
    };
    let txn_ctx = DmlTxnCtx {
        sessions: ctx.sessions,
        session_id: ctx.peer_addr.into(),
    };
    match run_read(
        ctx.state,
        ctx.identity,
        ctx.database_id(),
        &txn_ctx,
        &index_name,
        read,
    )
    .await
    {
        Ok((plan, response)) => data_plane_response_to_native(ctx, seq, &plan, &response),
        Err(error) => ddl_result_to_native(seq, Err(error)),
    }
}

/// The index an opcode names and the read it asks for.
fn sorted_read(op: OpCode, fields: &TextFields) -> crate::Result<(String, SortedIndexRead)> {
    let index_name = required(fields.index_name.as_deref(), "index_name")?.to_string();
    let key = || required(fields.key.as_deref(), "key").map(|key| key.as_bytes().to_vec());
    let read = match op {
        OpCode::KvSortedIndexRank => SortedIndexRead::Rank {
            primary_key: key()?,
        },
        OpCode::KvSortedIndexTopK => SortedIndexRead::TopK {
            k: fields.top_k_count.unwrap_or(10),
        },
        OpCode::KvSortedIndexRange => SortedIndexRead::Range {
            score_min: fields.score_min.clone(),
            score_max: fields.score_max.clone(),
        },
        OpCode::KvSortedIndexCount => SortedIndexRead::Count,
        OpCode::KvSortedIndexScore => SortedIndexRead::Score {
            primary_key: key()?,
        },
        other => {
            return Err(crate::Error::BadRequest {
                detail: format!("opcode {other:?} is not a sorted-index read"),
            });
        }
    };
    Ok((index_name, read))
}

fn required<'a>(value: Option<&'a str>, name: &str) -> crate::Result<&'a str> {
    value.ok_or_else(|| crate::Error::BadRequest {
        detail: format!("missing '{name}'"),
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn each_read_opcode_names_its_read() {
        let fields = TextFields {
            index_name: Some("lb".into()),
            key: Some("p1".into()),
            top_k_count: Some(3),
            ..TextFields::default()
        };
        assert_eq!(
            sorted_read(OpCode::KvSortedIndexTopK, &fields).expect("top k"),
            ("lb".to_string(), SortedIndexRead::TopK { k: 3 })
        );
        assert_eq!(
            sorted_read(OpCode::KvSortedIndexScore, &fields)
                .expect("score")
                .1,
            SortedIndexRead::Score {
                primary_key: b"p1".to_vec()
            }
        );
        let no_key = TextFields {
            index_name: Some("lb".into()),
            ..TextFields::default()
        };
        assert!(sorted_read(OpCode::KvSortedIndexRank, &no_key).is_err());
        assert!(sorted_read(OpCode::KvSortedIndexCount, &no_key).is_ok());
    }
}
