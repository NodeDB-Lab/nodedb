//! Control-Plane response post-processors that decorate Data-Plane
//! payloads with catalog-resolved fields (e.g. surrogate → user PK).

pub mod vector;

pub use vector::{translate_if_vector, translate_vector_search_payload};
