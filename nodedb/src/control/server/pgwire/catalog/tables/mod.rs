// SPDX-License-Identifier: BUSL-1.1

//! Per-relation row producers for each catalog table.

pub mod catalog_compat;
pub mod catalog_misc;
pub mod collections;
pub mod pg_attribute;
pub mod pg_class;
pub mod pg_index;
pub mod pg_type;

pub use catalog_compat::{pg_attrdef, pg_collation, pg_range};
pub use catalog_misc::{pg_authid, pg_database, pg_namespace};
pub use pg_attribute::pg_attribute;
pub use pg_class::pg_class;
pub use pg_index::pg_index;
pub use pg_type::pg_type;
