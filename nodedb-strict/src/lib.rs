//! Binary Tuple serialization for NodeDB strict document mode.
//!
//! Strict mode replaces MessagePack with a fixed-layout Binary Tuple for
//! collections with known schemas. This gives O(1) field extraction and
//! 3-4x better cache density than self-describing formats (BSON, MessagePack).
//!
//! # Tuple Layout
//!
//! ```text
//! [schema_version: u16 LE]
//! [null_bitmap: ceil(N/8) bytes]
//! [fixed_fields: concatenated fixed-size values, zeroed when null]
//! [offset_table: (N_var + 1) × u32 LE — start offsets into variable data]
//! [variable_data: concatenated variable-length bytes]
//! ```
//!
//! Fixed fields reserve space even when null (zeroed), so byte offsets are
//! constant and computable from the schema alone. Variable fields use an
//! offset table with N_var + 1 entries so the last field's length can be
//! derived as `offset[N_var] - offset[N_var - 1]`.

pub mod arrow_extract;
pub mod decode;
pub mod encode;
pub mod error;

pub use decode::TupleDecoder;
pub use encode::TupleEncoder;
pub use error::StrictError;
