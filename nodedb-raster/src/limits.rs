// SPDX-License-Identifier: Apache-2.0

//! Memory caps the readers decode under.

/// Largest single tag value other than the block index: GeoKeys,
/// `GDAL_METADATA`, `JPEGTables`, per-sample arrays.
pub(crate) const MAX_TAG_VALUE_BYTES: usize = 1024 * 1024;

/// Default cap on the header bytes an open caches: IFDs, tag values and
/// the block offset and byte count arrays.
pub const DEFAULT_HEADER_BYTES: usize = 32 * 1024 * 1024;

/// Most elements one decode allocation reserves, whatever limits the
/// caller passes. A larger request comes from a hostile header or cap.
pub const MAX_DECODE_CAPACITY: usize = 1 << 30;

/// A decode allocation over [`MAX_DECODE_CAPACITY`].
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
#[error("a decode allocation of {requested} elements is over the {limit}-element cap")]
pub struct CapacityExceeded {
    pub requested: usize,
    pub limit: usize,
}

/// `len` as the capacity of one decode allocation, refused over
/// [`MAX_DECODE_CAPACITY`].
pub(crate) fn checked_raster_capacity(len: usize) -> Result<usize, CapacityExceeded> {
    if len > MAX_DECODE_CAPACITY {
        return Err(CapacityExceeded {
            requested: len,
            limit: MAX_DECODE_CAPACITY,
        });
    }
    Ok(len)
}

/// Caps on what one open or one block decode allocates.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RasterLimits {
    /// Most bytes one block decodes to, across all its bands. The same cap
    /// bounds one compressed chunk read from the source.
    pub block_bytes: usize,
    /// Most bytes an open caches from the source.
    pub header_bytes: usize,
}

impl RasterLimits {
    /// Limits with `block_bytes` as the per-block cap and the default
    /// header cap.
    pub const fn new(block_bytes: usize) -> Self {
        Self {
            block_bytes,
            header_bytes: DEFAULT_HEADER_BYTES,
        }
    }

    /// These limits with `header_bytes` as the header cap.
    pub const fn with_header_bytes(self, header_bytes: usize) -> Self {
        Self {
            block_bytes: self.block_bytes,
            header_bytes,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn new_keeps_the_block_cap_and_the_default_header_cap() {
        let limits = RasterLimits::new(4096);
        assert_eq!(limits.block_bytes, 4096);
        assert_eq!(limits.header_bytes, DEFAULT_HEADER_BYTES);
        assert_eq!(limits.with_header_bytes(10).header_bytes, 10);
    }

    #[test]
    fn capacity_over_the_ceiling_is_refused() {
        assert_eq!(
            checked_raster_capacity(MAX_DECODE_CAPACITY),
            Ok(MAX_DECODE_CAPACITY)
        );
        assert_eq!(
            checked_raster_capacity(MAX_DECODE_CAPACITY + 1),
            Err(CapacityExceeded {
                requested: MAX_DECODE_CAPACITY + 1,
                limit: MAX_DECODE_CAPACITY,
            })
        );
    }
}
