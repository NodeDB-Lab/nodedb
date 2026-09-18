// SPDX-License-Identifier: Apache-2.0

//! Set-operation kinds for [`crate::physical_plan::QueryOp::SetOp`].

/// Which SQL set operation a [`crate::physical_plan::QueryOp::SetOp`] node
/// applies over its materialized inputs. Coordinator-resolved, never reaches
/// a Data-Plane core.
#[derive(
    Debug,
    Clone,
    Copy,
    PartialEq,
    Eq,
    serde::Serialize,
    serde::Deserialize,
    zerompk::ToMessagePack,
    zerompk::FromMessagePack,
)]
#[msgpack(c_enum)]
pub enum SetOpKind {
    /// `UNION ALL`: concatenate every input in order.
    UnionAll,
    /// `UNION`: concatenate, then drop duplicate rows.
    UnionDistinct,
    /// `INTERSECT`: rows present in every input, deduplicated.
    Intersect,
    /// `INTERSECT ALL`: rows present in every input, bag semantics.
    IntersectAll,
    /// `EXCEPT`: rows of the first input absent from the rest, deduplicated.
    Except,
    /// `EXCEPT ALL`: rows of the first input absent from the rest, bag semantics.
    ExceptAll,
}
