//! Minimal descriptor identity + lease types used by the replicated
//! metadata group. All per-DDL-object descriptor structs (Collection,
//! Index, Sequence, ...) live on the host side inside
//! `nodedb::control::catalog_entry::CatalogEntry`; this crate is
//! deliberately opaque to them.

pub mod common;
pub mod lease;

pub use common::{DescriptorHeader, DescriptorId, DescriptorKind};
pub use lease::DescriptorLease;
