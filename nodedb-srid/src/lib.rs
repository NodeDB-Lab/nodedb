// SPDX-License-Identifier: Apache-2.0

// Library code propagates errors; tests may unwrap.
#![cfg_attr(not(test), deny(clippy::unwrap_used))]

//! Spatial reference system identifiers for NodeDB.
//!
//! An [`Srid`] names a CRS by code. A [`SridResolver`] answers whether a
//! code or a CRS name is defined, and whether the CRS is geographic or
//! projected. The registry that defines CRSs, its EPSG table and the
//! coordinate transforms live in `nodedb-crs`, which implements the
//! resolver. Crates that only carry SRIDs depend on this crate alone and
//! never link the table.

pub mod error;
pub mod kind;
pub mod name;
pub mod resolver;
pub mod srid;

pub use error::{CrsParseError, UnknownSrid};
pub use kind::CrsKind;
pub use name::{NODEDB_AUTHORITY, code_from_name};
pub use resolver::SridResolver;
pub use srid::{InvalidSridCode, NotUserSrid, Srid, SridAuthority};
