// SPDX-License-Identifier: BUSL-1.1

//! Control-Plane response post-processors that decorate Data-Plane
//! payloads with catalog-resolved fields (e.g. surrogate → user PK).

pub mod dispatch;
pub mod hit_key;
pub mod text_hybrid;
pub mod vector;

pub use dispatch::translate_search_response;
pub use vector::translate_vector_search_payload;
