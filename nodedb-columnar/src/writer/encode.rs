// SPDX-License-Identifier: Apache-2.0

//! Low-level encoding helpers: validity bitmaps, numeric pipelines, schema hash.

use nodedb_codec::{ResolvedColumnCodec, encode_f64_pipeline, encode_i64_pipeline};
use nodedb_types::columnar::ColumnarSchema;

use crate::error::ColumnarError;

/// Encode i64 values with a prepended validity bitmap.
pub(super) fn encode_i64_with_validity(
    values: &[i64],
    valid: &[bool],
    codec: ResolvedColumnCodec,
) -> Result<Vec<u8>, ColumnarError> {
    let compressed = encode_i64_pipeline(values, codec.into_column_codec())?;
    Ok(prepend_validity(valid, &compressed))
}

/// Encode f64 values with a prepended validity bitmap.
pub(super) fn encode_f64_with_validity(
    values: &[f64],
    valid: &[bool],
    codec: ResolvedColumnCodec,
) -> Result<Vec<u8>, ColumnarError> {
    let compressed = encode_f64_pipeline(values, codec.into_column_codec())?;
    Ok(prepend_validity(valid, &compressed))
}

/// Encode a validity bitmap: ceil(N/8) bytes, bit=0 means null, bit=1 means valid.
pub(super) fn encode_validity_bitmap(valid: &[bool]) -> Vec<u8> {
    let byte_count = valid.len().div_ceil(8);
    let mut bitmap = vec![0u8; byte_count];
    for (i, &v) in valid.iter().enumerate() {
        if v {
            bitmap[i / 8] |= 1 << (i % 8);
        }
    }
    bitmap
}

/// Prepend a validity bitmap to compressed data.
pub(super) fn prepend_validity(valid: &[bool], compressed: &[u8]) -> Vec<u8> {
    let bitmap = encode_validity_bitmap(valid);
    let mut result = Vec::with_capacity(bitmap.len() + compressed.len());
    result.extend_from_slice(&bitmap);
    result.extend_from_slice(compressed);
    result
}

/// Compute a simple schema hash for the footer.
pub fn compute_schema_hash(schema: &ColumnarSchema) -> u64 {
    use std::hash::{Hash, Hasher};
    // no-determinism: segment compaction footer hash, not Calvin write path; tracked for switch to xxhash with fixed seed
    let mut hasher = std::collections::hash_map::DefaultHasher::new();
    for col in &schema.columns {
        col.name.hash(&mut hasher);
        format!("{:?}", col.column_type).hash(&mut hasher);
    }
    hasher.finish()
}
