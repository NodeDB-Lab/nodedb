// SPDX-License-Identifier: BUSL-1.1

//! Array engine dispatch — wires `PhysicalPlan::Array(ArrayOp)` to the
//! Data-Plane `ArrayEngine` via the shared `ArrayCatalogHandle`.

pub mod aggregate;
mod aggregate_helpers;
pub mod convert;
pub mod elementwise;
pub mod encode;
pub mod entry;
pub mod mutate;
pub mod open;
pub mod read;
mod slice_row;
pub mod surrogate_scan;
pub mod version_stamp;
