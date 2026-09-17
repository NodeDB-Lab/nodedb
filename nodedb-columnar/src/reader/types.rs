// SPDX-License-Identifier: Apache-2.0

/// Decoded column data from a segment scan.
///
/// The variant set is the closed set of physical column encodings, so a
/// reader matches it exhaustively and the compiler names every site an added
/// encoding must handle.
#[derive(Debug)]
pub enum DecodedColumn {
    Int64 {
        values: Vec<i64>,
        valid: Vec<bool>,
    },
    Float64 {
        values: Vec<f64>,
        valid: Vec<bool>,
    },
    Timestamp {
        values: Vec<i64>,
        valid: Vec<bool>,
    },
    Bool {
        values: Vec<bool>,
        valid: Vec<bool>,
    },
    /// Variable-length or fixed-size binary (String, Bytes, Geometry, Decimal, Uuid, Vector).
    Binary {
        /// Raw decompressed bytes for the block.
        data: Vec<u8>,
        /// Per-row byte offsets into `data`. Length = row_count + 1.
        offsets: Vec<u32>,
        valid: Vec<bool>,
    },
    /// Dictionary-encoded string column.
    ///
    /// IDs index into `dictionary`. Use `dictionary[ids[i]]` to recover the string
    /// for row `i` when `valid[i]` is true.
    DictEncoded {
        /// Symbol IDs per row (index into `dictionary`).
        ids: Vec<u32>,
        /// Dictionary: ID → string value. Populated from `ColumnMeta.dictionary`.
        dictionary: Vec<String>,
        valid: Vec<bool>,
    },
}
