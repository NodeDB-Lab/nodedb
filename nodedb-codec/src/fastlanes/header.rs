// SPDX-License-Identifier: Apache-2.0

//! Wire-format constants and header parsing/validation for FastLanes frames.

use std::mem::size_of;

use crate::bounds::{checked_capacity, checked_mul, checked_range, u32_to_usize};
use crate::error::CodecError;

use super::block::skip_block;

/// Block size for FastLanes processing. 1024 values aligns with SIMD
/// register widths across all targets (16 × 64-bit lanes on AVX-512,
/// 8 × 128-bit WASM v128 operations to cover 1024 elements).
pub(super) const BLOCK_SIZE: usize = 1024;

/// Header: 4 bytes count + 2 bytes block_count.
pub(super) const GLOBAL_HEADER_SIZE: usize = 6;

pub(super) fn parse_header(data: &[u8]) -> Result<(usize, usize), CodecError> {
    let header = checked_range(data, 0, GLOBAL_HEADER_SIZE, "FastLanes header")?;
    let total_count = u32_to_usize(
        u32::from_le_bytes([header[0], header[1], header[2], header[3]]),
        "FastLanes value count",
    )?;
    // Rejects a hostile count before it is used for block math; see
    // `checked_capacity` for why the scaling is done in 64-bit arithmetic.
    checked_capacity(total_count, size_of::<i64>(), "FastLanes")?;
    let block_count = usize::from(u16::from_le_bytes([header[4], header[5]]));
    let expected_blocks = total_count.div_ceil(BLOCK_SIZE);
    if block_count != expected_blocks {
        return Err(CodecError::Corrupt {
            detail: format!(
                "FastLanes block count {block_count} does not match value count {total_count}"
            ),
        });
    }
    Ok((total_count, block_count))
}

pub(super) fn validate_frame(
    data: &[u8],
    total_count: usize,
    block_count: usize,
) -> Result<(), CodecError> {
    let mut offset = GLOBAL_HEADER_SIZE;
    for block_idx in 0..block_count {
        offset = skip_block(
            data,
            offset,
            block_idx,
            expected_block_count(total_count, block_idx)?,
        )?;
    }
    if offset != data.len() {
        return Err(CodecError::Corrupt {
            detail: "trailing bytes after FastLanes frame".into(),
        });
    }
    Ok(())
}

pub(super) fn expected_block_count(
    total_count: usize,
    block_idx: usize,
) -> Result<usize, CodecError> {
    let start = checked_mul(block_idx, BLOCK_SIZE, "FastLanes block start")?;
    let remaining = total_count
        .checked_sub(start)
        .ok_or_else(|| CodecError::Corrupt {
            detail: "FastLanes block index exceeds declared count".into(),
        })?;
    Ok(remaining.min(BLOCK_SIZE))
}
