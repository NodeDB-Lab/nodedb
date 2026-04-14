//! Post-scan filter evaluation.
//!
//! `ScanFilter` represents a single filter predicate.
//!
//! Shared between Origin (Control Plane + Data Plane) and Lite.

pub mod like;
pub mod op;
pub mod parse;
pub mod types;

pub use like::sql_like_match;
pub use op::FilterOp;
pub use parse::parse_simple_predicates;
pub use types::ScanFilter;
