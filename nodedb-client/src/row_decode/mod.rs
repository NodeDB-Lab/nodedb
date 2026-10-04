// SPDX-License-Identifier: Apache-2.0

//! Feature-agnostic decoders that turn `QueryResult` rows back into typed
//! domain structs.
//!
//! Lives at the crate root so both the (feature-agnostic) `NodeDb` trait
//! default impls and the feature-gated `remote` / `native` clients share
//! one parser per row shape — there is exactly one place to fix when a
//! system-catalog column layout changes.

pub(crate) mod dropped_collection;
pub(crate) mod search_hit;
pub(crate) mod value;

pub(crate) use dropped_collection::parse_dropped_collection_rows;
pub(crate) use search_hit::{HitSource, decode_search_hits};
