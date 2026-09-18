// SPDX-License-Identifier: BUSL-1.1

//! Protocol-neutral set-operation merging over msgpack row payloads:
//! `UNION DISTINCT`, `INTERSECT`, `EXCEPT`. Operates on raw msgpack bytes
//! with no decode/re-encode round-trip. Used by the pgwire per-task set-op
//! path and by the coordinator's `QueryOp::SetOp` resolver.

mod intersect_except;
mod row_key;
mod union;

pub(crate) use intersect_except::{SetMergeMode, merge_set_op_payloads};
pub(crate) use union::dedup_union_payloads;
