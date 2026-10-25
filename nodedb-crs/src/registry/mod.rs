// SPDX-License-Identifier: Apache-2.0

//! Where CRS definitions come from: the built-in table and user-defined
//! sets, resolved through a per-statement context.

pub mod builtin;
pub mod context;
pub mod custom;

pub use builtin::{
    BuiltinEntry, builtin_codes, builtin_entries, builtin_lookup, geographic_crs_on_datum,
    lookup_in,
};
pub use context::CrsContext;
pub use custom::{CustomCrs, CustomCrsSet, CustomCrsState, DuplicateCustomSrid};
pub use nodedb_srid::code_from_name;
