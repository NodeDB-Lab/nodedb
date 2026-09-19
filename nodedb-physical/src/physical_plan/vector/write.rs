// SPDX-License-Identifier: Apache-2.0

//! Row-existence and row-targeting shapes of the vector-primary direct
//! writes.

use nodedb_types::Surrogate;

/// Row-existence semantics of a vector-primary direct write, as the three
/// insert-family ops carry them on the durable record.
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
pub enum VectorDirectWriteIntent {
    /// [`VectorOp::DirectInsert`](super::VectorOp::DirectInsert): an existing
    /// key is a `unique_violation`.
    Insert,
    /// [`VectorOp::DirectInsertIfAbsent`](super::VectorOp::DirectInsertIfAbsent):
    /// an existing key is left alone.
    InsertIfAbsent,
    /// [`VectorOp::DirectUpsert`](super::VectorOp::DirectUpsert): an existing
    /// key is replaced or patched.
    Upsert,
}

/// The rows a vector-primary `DELETE` / `UPDATE` targets.
#[derive(
    Debug,
    Clone,
    PartialEq,
    serde::Serialize,
    serde::Deserialize,
    zerompk::ToMessagePack,
    zerompk::FromMessagePack,
)]
pub enum VectorWriteTargets {
    /// Surrogates the Control Plane resolved from primary-key equalities.
    /// `Surrogate::ZERO` is a key with no binding; it matches no row.
    Surrogates(Vec<Surrogate>),
    /// Serialized `Vec<ScanFilter>` the Data Plane evaluates against every
    /// payload sidecar row. Empty bytes match every row.
    Predicate(Vec<u8>),
}
