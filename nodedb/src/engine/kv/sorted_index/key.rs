//! Composite sort key encoding for sorted indexes.
//!
//! Encodes multi-column sort keys into a single `Vec<u8>` such that the
//! natural lexicographic byte ordering matches the desired sort order.
//!
//! For ascending columns: value bytes are stored as-is (big-endian).
//! For descending columns: every byte is bitwise-complemented (~byte),
//! which reverses the sort order while preserving lexicographic comparison.
//!
//! Columns are separated by a length prefix (4 bytes, big-endian) to avoid
//! ambiguity between field boundaries.

/// Sort direction for a column in a sorted index.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SortDirection {
    Asc,
    Desc,
}

/// Definition of one column in a composite sort key.
#[derive(Debug, Clone)]
pub struct SortColumn {
    /// Column name in the collection schema.
    pub name: String,
    /// Sort direction for this column.
    pub direction: SortDirection,
}

/// Encodes composite sort keys for a sorted index definition.
///
/// Given a list of `(column_name, direction)` pairs, produces a single
/// byte key from extracted field values such that `BTreeMap` ordering
/// matches the desired multi-column sort.
#[derive(Debug, Clone)]
pub struct SortKeyEncoder {
    columns: Vec<SortColumn>,
}

impl SortKeyEncoder {
    pub fn new(columns: Vec<SortColumn>) -> Self {
        Self { columns }
    }

    /// Number of columns in the composite key.
    pub fn column_count(&self) -> usize {
        self.columns.len()
    }

    /// Column definitions.
    pub fn columns(&self) -> &[SortColumn] {
        &self.columns
    }

    /// Encode field values into a composite sort key.
    ///
    /// `values` must have the same length as `columns`.
    /// Each value is the raw bytes of the field (big-endian for numerics,
    /// UTF-8 for strings).
    pub fn encode(&self, values: &[&[u8]]) -> Vec<u8> {
        debug_assert_eq!(values.len(), self.columns.len());

        let total_len: usize = values.iter().map(|v| 4 + v.len()).sum();
        let mut key = Vec::with_capacity(total_len);

        for (value, col) in values.iter().zip(&self.columns) {
            // Length prefix (4 bytes, big-endian) — always ascending so that
            // shorter values sort before longer values within the same column.
            let len = value.len() as u32;
            key.extend_from_slice(&len.to_be_bytes());

            match col.direction {
                SortDirection::Asc => {
                    key.extend_from_slice(value);
                }
                SortDirection::Desc => {
                    // Bitwise complement reverses sort order.
                    for &b in *value {
                        key.push(!b);
                    }
                }
            }
        }

        key
    }

    /// Encode an i64 score as big-endian bytes suitable for sorting.
    ///
    /// Maps i64 to u64 by flipping the sign bit, so negative values sort
    /// before positive values in unsigned byte comparison.
    pub fn encode_i64(value: i64) -> [u8; 8] {
        let unsigned = (value as u64) ^ (1u64 << 63);
        unsigned.to_be_bytes()
    }

    /// Decode big-endian bytes back to i64.
    pub fn decode_i64(bytes: &[u8; 8]) -> i64 {
        let unsigned = u64::from_be_bytes(*bytes);
        (unsigned ^ (1u64 << 63)) as i64
    }

    /// Encode an f64 score as big-endian bytes suitable for sorting.
    ///
    /// Uses the IEEE 754 total-order encoding trick:
    /// - Positive floats: flip the sign bit (0x80...00 XOR).
    /// - Negative floats: flip all bits (bitwise NOT).
    ///
    /// This produces a byte sequence where f64 ordering matches byte ordering.
    pub fn encode_f64(value: f64) -> [u8; 8] {
        let bits = value.to_bits();
        let encoded = if bits & (1u64 << 63) == 0 {
            // Positive (or +0): flip sign bit.
            bits ^ (1u64 << 63)
        } else {
            // Negative (or -0): flip all bits.
            !bits
        };
        encoded.to_be_bytes()
    }

    /// Decode big-endian bytes back to f64.
    pub fn decode_f64(bytes: &[u8; 8]) -> f64 {
        let encoded = u64::from_be_bytes(*bytes);
        let bits = if encoded & (1u64 << 63) != 0 {
            // Was positive: flip sign bit back.
            encoded ^ (1u64 << 63)
        } else {
            // Was negative: flip all bits back.
            !encoded
        };
        f64::from_bits(bits)
    }

    /// Encode a millisecond timestamp as big-endian bytes.
    ///
    /// Timestamps are u64, so they naturally sort ascending in big-endian.
    pub fn encode_timestamp_ms(ts_ms: u64) -> [u8; 8] {
        ts_ms.to_be_bytes()
    }

    /// Decode big-endian bytes back to a millisecond timestamp.
    pub fn decode_timestamp_ms(bytes: &[u8; 8]) -> u64 {
        u64::from_be_bytes(*bytes)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn i64_roundtrip() {
        for v in [i64::MIN, -1, 0, 1, i64::MAX] {
            let encoded = SortKeyEncoder::encode_i64(v);
            assert_eq!(SortKeyEncoder::decode_i64(&encoded), v);
        }
    }

    #[test]
    fn i64_ordering() {
        let values = [i64::MIN, -1000, -1, 0, 1, 1000, i64::MAX];
        let encoded: Vec<_> = values
            .iter()
            .map(|v| SortKeyEncoder::encode_i64(*v))
            .collect();
        for i in 0..encoded.len() - 1 {
            assert!(
                encoded[i] < encoded[i + 1],
                "encode_i64({}) should be < encode_i64({})",
                values[i],
                values[i + 1]
            );
        }
    }

    #[test]
    fn f64_roundtrip() {
        for v in [f64::NEG_INFINITY, -1.0, -0.0, 0.0, 1.0, f64::INFINITY] {
            let encoded = SortKeyEncoder::encode_f64(v);
            let decoded = SortKeyEncoder::decode_f64(&encoded);
            assert_eq!(v.to_bits(), decoded.to_bits(), "roundtrip failed for {v}");
        }
    }

    #[test]
    fn f64_ordering() {
        let values = [
            f64::NEG_INFINITY,
            -100.0,
            -0.001,
            0.0,
            0.001,
            100.0,
            f64::INFINITY,
        ];
        let encoded: Vec<_> = values
            .iter()
            .map(|v| SortKeyEncoder::encode_f64(*v))
            .collect();
        for i in 0..encoded.len() - 1 {
            assert!(
                encoded[i] < encoded[i + 1],
                "encode_f64({}) should be < encode_f64({})",
                values[i],
                values[i + 1]
            );
        }
    }

    #[test]
    fn composite_key_desc_score_asc_time() {
        let encoder = SortKeyEncoder::new(vec![
            SortColumn {
                name: "score".into(),
                direction: SortDirection::Desc,
            },
            SortColumn {
                name: "updated_at".into(),
                direction: SortDirection::Asc,
            },
        ]);

        // Higher score should sort first (lower bytes due to DESC complement).
        let score_100 = SortKeyEncoder::encode_i64(100);
        let score_200 = SortKeyEncoder::encode_i64(200);
        let time_early = SortKeyEncoder::encode_timestamp_ms(1000);
        let time_late = SortKeyEncoder::encode_timestamp_ms(2000);

        let key_200_early = encoder.encode(&[&score_200, &time_early]);
        let key_100_early = encoder.encode(&[&score_100, &time_early]);
        let key_200_late = encoder.encode(&[&score_200, &time_late]);

        // 200 DESC sorts before 100 DESC.
        assert!(key_200_early < key_100_early);
        // Same score, earlier time ASC sorts before later time.
        assert!(key_200_early < key_200_late);
    }

    #[test]
    fn composite_key_asc_score() {
        let encoder = SortKeyEncoder::new(vec![SortColumn {
            name: "score".into(),
            direction: SortDirection::Asc,
        }]);

        let score_50 = SortKeyEncoder::encode_i64(50);
        let score_150 = SortKeyEncoder::encode_i64(150);

        let key_50 = encoder.encode(&[&score_50]);
        let key_150 = encoder.encode(&[&score_150]);

        // ASC: lower score sorts first.
        assert!(key_50 < key_150);
    }

    #[test]
    fn timestamp_roundtrip_and_ordering() {
        let ts1 = 1714600000000u64;
        let ts2 = 1714600001000u64;
        let e1 = SortKeyEncoder::encode_timestamp_ms(ts1);
        let e2 = SortKeyEncoder::encode_timestamp_ms(ts2);
        assert_eq!(SortKeyEncoder::decode_timestamp_ms(&e1), ts1);
        assert_eq!(SortKeyEncoder::decode_timestamp_ms(&e2), ts2);
        assert!(e1 < e2);
    }
}
