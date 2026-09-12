// SPDX-License-Identifier: BUSL-1.1

//! Apply-a-PointPut family: core transaction helper, index side-effects
//! (spatial/vector/sparse), and UNIQUE-constraint check.

pub(in crate::data::executor::handlers::point) mod core;
pub(in crate::data::executor::handlers::point) mod enforce;
pub(in crate::data::executor::handlers::point) mod index;
pub(in crate::data::executor::handlers::point) mod sparse;
pub(in crate::data::executor) mod stored_body;
pub(in crate::data::executor::handlers::point) mod types;
pub(in crate::data::executor) mod unique;
pub(in crate::data::executor::handlers::point) mod vector;

pub(in crate::data::executor) use index::SpatialEntryId;
pub(in crate::data::executor) use types::{PointPutOutcome, PointPutParams, map_enforcement_error};
pub(in crate::data::executor) use vector::{VectorIndexDelta, VectorIndexPutParams};
