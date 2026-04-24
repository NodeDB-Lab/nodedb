//! Columnar base insert handler.
//!
//! Writes rows to `nodedb-columnar`'s `MutationEngine`. Accepts msgpack payload
//! (array of objects). Creates the engine on first insert with schema inferred
//! from the first row.

pub mod insert;
pub mod read_prior;
pub mod schema;
pub mod spatial;
