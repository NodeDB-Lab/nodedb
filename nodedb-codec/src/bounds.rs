// SPDX-License-Identifier: Apache-2.0

//! Shared hard limits and checked framing helpers for codec decoders.

use crate::error::CodecError;

/// Maximum decoded payload accepted from one codec frame (64 MiB).
///
/// This is deliberately the same ceiling as a WAL payload: it permits normal
/// column blocks while preventing a small hostile frame from reserving process
/// memory proportional to an attacker-controlled u32 length.
pub(crate) const MAX_DECODED_BYTES: usize = 64 * 1024 * 1024;

/// Maximum declared expansion relative to compressed bytes for Zstd frames.
/// Zstd frames may legitimately compress repeated telemetry strongly; 4096:1
/// permits that workload while rejecting extreme decompression bombs.
pub(crate) const MAX_DECOMPRESSION_RATIO: usize = 4096;

pub(crate) fn u32_to_usize(value: u32, context: &str) -> Result<usize, CodecError> {
    usize::try_from(value).map_err(|_| CodecError::Corrupt {
        detail: format!("{context} does not fit platform usize"),
    })
}

pub(crate) fn checked_add(left: usize, right: usize, context: &str) -> Result<usize, CodecError> {
    left.checked_add(right).ok_or_else(|| CodecError::Corrupt {
        detail: format!("{context} overflows usize"),
    })
}

pub(crate) fn checked_mul(left: usize, right: usize, context: &str) -> Result<usize, CodecError> {
    left.checked_mul(right).ok_or_else(|| CodecError::Corrupt {
        detail: format!("{context} overflows usize"),
    })
}

pub(crate) fn checked_range<'a>(
    data: &'a [u8],
    start: usize,
    len: usize,
    context: &str,
) -> Result<&'a [u8], CodecError> {
    let end = checked_add(start, len, context)?;
    data.get(start..end).ok_or(CodecError::Truncated {
        expected: end,
        actual: data.len(),
    })
}

pub(crate) fn decoded_len(value: usize, codec: &str) -> Result<usize, CodecError> {
    if value > MAX_DECODED_BYTES {
        return Err(CodecError::ResourceLimit {
            resource: format!("{codec} decoded bytes"),
            requested: value,
            limit: MAX_DECODED_BYTES,
        });
    }
    Ok(value)
}

/// Validate a decoded container allocation before reserving its element count.
///
/// The returned value is the original element count, suitable as the only
/// argument to `Vec::with_capacity`. This centralizes overflow and resource
/// checks for counts read from untrusted frames.
///
/// The element-size multiplication is carried out in 64-bit arithmetic rather
/// than in `usize`. A 32-bit target cannot multiply a hostile `u32` count by a
/// wide element at all, so doing it in `usize` turns a frame that is merely too
/// large into a `Corrupt` "overflows usize" — the same frame is a clean
/// `ResourceLimit` where `usize` is 64 bits. The ceiling is 64 MiB, far inside
/// any `usize` the workspace builds for, so the 64-bit product is always within
/// range when the check succeeds; only the comparison needed the wider type.
pub(crate) fn checked_capacity(
    count: usize,
    element_size: usize,
    context: &str,
) -> Result<usize, CodecError> {
    let bytes = (count as u64).saturating_mul(element_size as u64);
    if bytes > MAX_DECODED_BYTES as u64 {
        return Err(CodecError::ResourceLimit {
            resource: format!("{context} decoded bytes"),
            requested: usize::try_from(bytes).unwrap_or(usize::MAX),
            limit: MAX_DECODED_BYTES,
        });
    }
    Ok(count)
}

pub(crate) fn encode_u32_len(value: usize, context: &str) -> Result<u32, CodecError> {
    u32::try_from(value).map_err(|_| CodecError::ResourceLimit {
        resource: format!("{context} encoded length"),
        requested: value,
        limit: u32::MAX as usize,
    })
}

pub(crate) fn encode_input_len(value: usize, codec: &str) -> Result<u32, CodecError> {
    decoded_len(value, codec)?;
    encode_u32_len(value, codec)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn checked_capacity_returns_count_after_validating_bytes() {
        assert!(matches!(checked_capacity(16, 8, "test"), Ok(16)));
    }

    #[test]
    fn checked_capacity_rejects_resource_limit() {
        let error = checked_capacity(MAX_DECODED_BYTES / 8 + 1, 8, "test");
        assert!(matches!(error, Err(CodecError::ResourceLimit { .. })));
    }

    #[test]
    fn checked_capacity_rejects_oversized_element_count() {
        // `usize::MAX` elements of eight bytes each. Multiplying those in
        // `usize` overflows, and on a 32-bit target even `u32::MAX` elements
        // do; the classification is the resource limit either way, on every
        // target the workspace builds for.
        let error = checked_capacity(usize::MAX, 8, "test");
        assert!(matches!(error, Err(CodecError::ResourceLimit { .. })));
    }

    #[test]
    fn checked_capacity_rejects_wide_element_count_on_every_target() {
        // The 32-bit case in the large: `u32::MAX` elements of eight bytes is
        // 32 GiB at an element size that cannot be expressed in a 32-bit
        // `usize`.
        let error = checked_capacity(u32::MAX as usize, 8, "test");
        assert!(matches!(error, Err(CodecError::ResourceLimit { .. })));
    }
}
