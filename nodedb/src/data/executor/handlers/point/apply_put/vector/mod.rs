// SPDX-License-Identifier: BUSL-1.1

//! HNSW vector-index side-effects for `apply_point_put`: index declared
//! strict-schema `Vector(dim)` columns, schemaless `vector_params` fields and
//! declared schemaless `VECTOR(n)` columns, and soft-delete a document's prior
//! vector nodes.

mod fields;
mod put;
mod remove;
mod types;

pub(in crate::data::executor) use types::{VectorIndexDelta, VectorIndexPutParams};
