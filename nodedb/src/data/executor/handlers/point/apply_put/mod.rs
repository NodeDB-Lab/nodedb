// SPDX-License-Identifier: BUSL-1.1

//! Apply-a-PointPut family: core transaction helper, index side-effects
//! (spatial/vector/sparse).

mod config_witness;
pub(in crate::data::executor::handlers::point) mod core;
pub(in crate::data::executor) mod enforce;
mod frozen_body;
mod frozen_defaults;
pub(in crate::data::executor::handlers::point) mod history;
pub(in crate::data::executor::handlers::point) mod index;
mod installed;
mod installed_schema;
mod materialized;
mod prepared;
pub(in crate::data::executor::handlers::point) mod sparse;
pub(in crate::data::executor) mod stored_body;
pub(in crate::data::executor::handlers::point) mod types;
pub(in crate::data::executor::handlers::point) mod vector;

pub(in crate::data::executor) use enforce::PutEnforcement;
pub(in crate::data::executor) use frozen_body::{
    FreezeDocumentBodyInput, FrozenDocumentBody, materialize_native_write_body,
};
pub(in crate::data::executor) use index::SpatialEntryId;
pub(in crate::data::executor) use prepared::PreparedPointPut;
pub(in crate::data::executor) use sparse::SparseIndexPut;
pub(in crate::data::executor) use types::{
    InstalledDocumentRow, PointPutOutcome, PointPutParams, map_enforcement_error,
};
pub(in crate::data::executor) use vector::{VectorIndexDelta, VectorIndexPutParams};

#[cfg(test)]
mod installed_schema_tests;

#[cfg(test)]
mod installed_image_tests;

#[cfg(test)]
mod installed_head_tests;

#[cfg(test)]
mod installed_projection_tests;

#[cfg(test)]
mod prepared_tests;

#[cfg(test)]
mod bound_defaults_tests;
#[cfg(test)]
mod frozen_body_tests;
