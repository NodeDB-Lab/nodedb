//! Unified Quantized Vector layout — the cache-aligned superset format that
//! absorbs binary / ternary (BitNet 1.58) / 4-bit scalar / residual codecs
//! without polymorphic indirection in the hot path.
//!
//! 128-byte alignment matches AVX-512 cache-line pair and avoids false
//! sharing under thread-per-core execution.
//!
//! ## Outlier bitmask limit
//!
//! The `outlier_bitmask` in [`QuantHeader`] supports up to 64 outlier
//! dimensions per vector. Callers that need to mark more than 64 outliers
//! must bucket by 64-dim windows; multi-window support is out of scope for
//! this module.

use crate::error::CodecError;

// ── QuantMode ──────────────────────────────────────────────────────────────

/// Quantization mode discriminator stored in the header.
///
/// Values are **stable on disk** — never reorder, only append.
#[repr(u16)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum QuantMode {
    Binary = 0,
    RaBitQ = 1,
    Bbq = 2,
    /// 5 trits/byte cold layout, 1.6 bpw.
    TernaryPacked = 3,
    /// 2 bpw expanded for SIMD hot path.
    TernarySimd = 4,
    TurboQuant4b = 5,
    Sq8 = 6,
    Pq = 7,
    /// Reserved for ITQ3_S (Tier 14).
    Itq3S = 8,
    /// Reserved for PolarQuant (Tier 14).
    PolarQuant = 9,
}

impl QuantMode {
    /// Bits per weight for each mode — used by [`target_size`] to compute the
    /// packed-bits byte count.
    fn bits_per_weight(self) -> u32 {
        match self {
            QuantMode::Binary => 1,
            QuantMode::RaBitQ => 1,
            QuantMode::TernaryPacked => 2, // 5 trits/byte ≈ 1.6 bpw; ceil to 2 for alignment
            QuantMode::TernarySimd => 2,
            QuantMode::Bbq => 1,
            QuantMode::TurboQuant4b => 4,
            QuantMode::Sq8 => 8,
            QuantMode::Pq => 8,
            QuantMode::Itq3S => 2,
            QuantMode::PolarQuant => 4,
        }
    }
}

// ── QuantHeader ────────────────────────────────────────────────────────────

/// 32-byte interleaved header preceding the packed bit array.
///
/// `#[repr(C)]` for stable layout; serializable via raw bytes.
#[repr(C)]
#[derive(Debug, Clone, Copy)]
pub struct QuantHeader {
    /// [`QuantMode`] discriminant.
    pub quant_mode: u16,
    pub dim: u16,
    /// BitNet absmean / SQ8 scale / RaBitQ rotation magnitude.
    pub global_scale: f32,
    /// RaBitQ ‖v−c‖ / TurboQuant ‖r‖ / BBQ centroid distance.
    pub residual_norm: f32,
    /// RaBitQ ⟨v, q(v)⟩ / BBQ corrective / TurboQuant QJL bias.
    pub dot_quantized: f32,
    /// Sparse outlier index — branchless gather via popcnt.
    /// Supports up to 64 outlier dimensions per vector.
    pub outlier_bitmask: u64,
    /// QJL projection seed / OSAQ Hessian coeff / future use.
    pub reserved: [u8; 8],
}

// Compile-time assertion: header is exactly 32 bytes.
const _: () = assert!(core::mem::size_of::<QuantHeader>() == 32);

// ── Layout constants ────────────────────────────────────────────────────────

/// Byte size of one outlier entry: (dim_index: u32, value: f32).
const OUTLIER_ENTRY_BYTES: usize = 8;

/// Cache-line pair alignment — also the minimum allocation unit.
const ALIGN: usize = 128;

// ── target_size ─────────────────────────────────────────────────────────────

/// Compute the buffer size (rounded up to a 128-byte multiple) required to
/// hold a [`UnifiedQuantizedVector`] with the given parameters.
///
/// This is the pre-sizing helper for callers that want to allocate before
/// constructing.
pub fn target_size(quant_mode: QuantMode, dim: u16, outlier_count: u32) -> usize {
    let packed_bits_bytes = packed_bits_len(quant_mode, dim);
    let outlier_bytes = outlier_count as usize * OUTLIER_ENTRY_BYTES;
    let raw = core::mem::size_of::<QuantHeader>() + packed_bits_bytes + outlier_bytes;
    round_up_128(raw)
}

// ── Internal helpers ────────────────────────────────────────────────────────

#[inline]
fn packed_bits_len(quant_mode: QuantMode, dim: u16) -> usize {
    let bpw = quant_mode.bits_per_weight() as usize;
    let total_bits = dim as usize * bpw;
    total_bits.div_ceil(8)
}

#[inline]
fn round_up_128(n: usize) -> usize {
    (n + ALIGN - 1) & !(ALIGN - 1)
}

// ── UnifiedQuantizedVector ──────────────────────────────────────────────────

/// Owned, 128-byte-aligned unified quantized vector buffer.
///
/// Layout (contiguous bytes):
/// ```text
/// [ QuantHeader (32 B) | packed_bits (variable) | outlier_payload (8 B × n) | tail_pad ]
/// ```
///
/// The total allocation is always a multiple of 128 bytes (one AVX-512
/// cache-line pair).
pub struct UnifiedQuantizedVector {
    /// Backing storage.  Always a multiple of 128 bytes.
    buf: Vec<u8>,
    /// Byte length of the packed-bits region (excludes header and outliers).
    packed_bits_len: usize,
}

impl UnifiedQuantizedVector {
    /// Construct from an explicit header, packed-bit slice, and sparse
    /// outlier list.
    ///
    /// `outliers` is a slice of `(dim_index, value)` pairs.  The
    /// `outlier_bitmask` in `header` must have exactly one bit set for each
    /// entry in `outliers`, and bits must correspond to entries in ascending
    /// `dim_index` order (i.e. popcnt-dense order).
    ///
    /// # Errors
    ///
    /// Returns [`CodecError::LayoutError`] if:
    /// - `outliers.len()` does not match `popcnt(header.outlier_bitmask)`.
    /// - Any `dim_index` in `outliers` is ≥ 64 (bitmask only covers 64 dims).
    pub fn new(
        header: QuantHeader,
        packed_bits: &[u8],
        outliers: &[(u32, f32)],
    ) -> Result<Self, CodecError> {
        let expected_outlier_count = header.outlier_bitmask.count_ones() as usize;
        if outliers.len() != expected_outlier_count {
            return Err(CodecError::LayoutError {
                detail: format!(
                    "outlier count mismatch: bitmask has {} bits set but {} outliers provided",
                    expected_outlier_count,
                    outliers.len()
                ),
            });
        }
        for &(dim_idx, _) in outliers {
            if dim_idx >= 64 {
                return Err(CodecError::LayoutError {
                    detail: format!("outlier dim_index {dim_idx} exceeds bitmask capacity of 64"),
                });
            }
        }

        let header_bytes = core::mem::size_of::<QuantHeader>();
        let outlier_bytes = outliers.len() * OUTLIER_ENTRY_BYTES;
        let raw = header_bytes + packed_bits.len() + outlier_bytes;
        let total = round_up_128(raw);

        let mut buf = vec![0u8; total];

        // Write header via raw copy (QuantHeader is repr(C), no padding issues).
        let header_src = unsafe {
            core::slice::from_raw_parts(&header as *const QuantHeader as *const u8, header_bytes)
        };
        buf[..header_bytes].copy_from_slice(header_src);

        // Write packed bits.
        let pb_start = header_bytes;
        let pb_end = pb_start + packed_bits.len();
        buf[pb_start..pb_end].copy_from_slice(packed_bits);

        // Write outlier payload: each entry is u32 dim_index || f32 value (LE).
        let mut off = pb_end;
        for &(dim_idx, value) in outliers {
            buf[off..off + 4].copy_from_slice(&dim_idx.to_le_bytes());
            buf[off + 4..off + 8].copy_from_slice(&value.to_le_bytes());
            off += OUTLIER_ENTRY_BYTES;
        }

        Ok(Self {
            buf,
            packed_bits_len: packed_bits.len(),
        })
    }

    // ── Accessors ────────────────────────────────────────────────────────────

    /// Zero-copy reference to the header (first 32 bytes).
    #[inline]
    pub fn header(&self) -> &QuantHeader {
        let ptr = self.buf.as_ptr() as *const QuantHeader;
        // SAFETY: buf is always at least 32 bytes and QuantHeader is repr(C).
        unsafe { &*ptr }
    }

    /// Slice of the packed-bit region.
    #[inline]
    pub fn packed_bits(&self) -> &[u8] {
        let start = core::mem::size_of::<QuantHeader>();
        &self.buf[start..start + self.packed_bits_len]
    }

    /// Number of outlier entries, computed via popcnt of `outlier_bitmask`.
    #[inline]
    pub fn outlier_count(&self) -> u32 {
        self.header().outlier_bitmask.count_ones()
    }

    /// Return the outlier `(dim_index, value)` for the dimension at position
    /// `slot` in the bitmask.
    ///
    /// `slot` is the dimension index (0–63).  Returns `None` if the bit for
    /// `slot` is not set in `outlier_bitmask`, or if `slot ≥ 64`.
    ///
    /// Uses a branchless popcnt to find the dense offset into the outlier
    /// payload.
    pub fn outlier_at(&self, slot: u32) -> Option<(u32, f32)> {
        if slot >= 64 {
            return None;
        }
        let bitmask = self.header().outlier_bitmask;
        if bitmask & (1u64 << slot) == 0 {
            return None;
        }
        // Number of set bits below `slot` gives the dense array index.
        let mask = bitmask & ((1u64 << slot).wrapping_sub(1));
        let offset = mask.count_ones() as usize;

        let header_bytes = core::mem::size_of::<QuantHeader>();
        let base = header_bytes + self.packed_bits_len + offset * OUTLIER_ENTRY_BYTES;

        let dim_idx = u32::from_le_bytes(self.buf[base..base + 4].try_into().ok()?);
        let value = f32::from_le_bytes(self.buf[base + 4..base + 8].try_into().ok()?);
        Some((dim_idx, value))
    }

    /// Full backing buffer suitable for direct I/O.
    #[inline]
    pub fn as_bytes(&self) -> &[u8] {
        &self.buf
    }
}

// ── UnifiedQuantizedVectorRef ───────────────────────────────────────────────

/// Zero-copy borrowed view into a `UnifiedQuantizedVector` buffer.
///
/// Suitable for reads from the io_uring page cache — no allocation required.
pub struct UnifiedQuantizedVectorRef<'a> {
    buf: &'a [u8],
    packed_bits_len: usize,
}

impl<'a> UnifiedQuantizedVectorRef<'a> {
    /// Borrow a raw byte slice as a quantized vector view.
    ///
    /// # Errors
    ///
    /// Returns [`CodecError::LayoutError`] if the slice is shorter than 32 bytes
    /// (minimum header size).
    pub fn from_bytes(buf: &'a [u8], packed_bits_len: usize) -> Result<Self, CodecError> {
        let header_bytes = core::mem::size_of::<QuantHeader>();
        if buf.len() < header_bytes + packed_bits_len {
            return Err(CodecError::LayoutError {
                detail: format!(
                    "buffer too short: need at least {} bytes, got {}",
                    header_bytes + packed_bits_len,
                    buf.len()
                ),
            });
        }
        Ok(Self {
            buf,
            packed_bits_len,
        })
    }

    /// Zero-copy header reference.
    #[inline]
    pub fn header(&self) -> &QuantHeader {
        let ptr = self.buf.as_ptr() as *const QuantHeader;
        // SAFETY: validated in from_bytes that buf.len() >= 32.
        unsafe { &*ptr }
    }

    /// Packed-bits slice.
    #[inline]
    pub fn packed_bits(&self) -> &[u8] {
        let start = core::mem::size_of::<QuantHeader>();
        &self.buf[start..start + self.packed_bits_len]
    }

    /// Number of outlier entries via popcnt.
    #[inline]
    pub fn outlier_count(&self) -> u32 {
        self.header().outlier_bitmask.count_ones()
    }

    /// Outlier lookup — same semantics as [`UnifiedQuantizedVector::outlier_at`].
    pub fn outlier_at(&self, slot: u32) -> Option<(u32, f32)> {
        if slot >= 64 {
            return None;
        }
        let bitmask = self.header().outlier_bitmask;
        if bitmask & (1u64 << slot) == 0 {
            return None;
        }
        let mask = bitmask & ((1u64 << slot).wrapping_sub(1));
        let offset = mask.count_ones() as usize;

        let header_bytes = core::mem::size_of::<QuantHeader>();
        let base = header_bytes + self.packed_bits_len + offset * OUTLIER_ENTRY_BYTES;

        let dim_idx = u32::from_le_bytes(self.buf[base..base + 4].try_into().ok()?);
        let value = f32::from_le_bytes(self.buf[base + 4..base + 8].try_into().ok()?);
        Some((dim_idx, value))
    }
}

// ── Tests ───────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    fn make_header(mode: QuantMode, dim: u16, bitmask: u64) -> QuantHeader {
        QuantHeader {
            quant_mode: mode as u16,
            dim,
            global_scale: 1.5,
            residual_norm: 0.25,
            dot_quantized: 2.5,
            outlier_bitmask: bitmask,
            reserved: [0xAB; 8],
        }
    }

    #[test]
    fn header_is_32_bytes() {
        // Also enforced by the const assert above; belt-and-suspenders.
        assert_eq!(core::mem::size_of::<QuantHeader>(), 32);
    }

    #[test]
    fn target_size_is_128_multiple() {
        for mode in [
            QuantMode::Binary,
            QuantMode::RaBitQ,
            QuantMode::TernarySimd,
            QuantMode::TurboQuant4b,
            QuantMode::Sq8,
        ] {
            for dim in [64u16, 128, 256, 512, 1536] {
                for outliers in [0u32, 1, 8, 64] {
                    let sz = target_size(mode, dim, outliers);
                    assert_eq!(
                        sz % 128,
                        0,
                        "target_size not 128-aligned for {mode:?}/{dim}/{outliers}"
                    );
                    assert!(
                        sz >= 128,
                        "target_size below minimum for {mode:?}/{dim}/{outliers}"
                    );
                }
            }
        }
    }

    #[test]
    fn no_outliers_roundtrip() {
        let header = make_header(QuantMode::Binary, 128, 0);
        let packed = vec![0xFFu8; 16]; // 128 dims / 8 = 16 bytes
        let vec = UnifiedQuantizedVector::new(header, &packed, &[]).unwrap();

        assert_eq!(vec.outlier_count(), 0);
        assert_eq!(vec.packed_bits(), packed.as_slice());
        assert_eq!(vec.as_bytes().len() % 128, 0);
    }

    #[test]
    fn one_outlier_roundtrip() {
        // Bit 5 set → dim_index 5 is an outlier.
        let bitmask: u64 = 1 << 5;
        let header = make_header(QuantMode::Sq8, 64, bitmask);
        let packed = vec![0u8; 64]; // 64 dims × 8bpw = 64 bytes
        let outliers = [(5u32, 42.0f32)];
        let vec = UnifiedQuantizedVector::new(header, &packed, &outliers).unwrap();

        assert_eq!(vec.outlier_count(), 1);
        let (dim, val) = vec.outlier_at(5).expect("bit 5 should be set");
        assert_eq!(dim, 5);
        assert!((val - 42.0).abs() < f32::EPSILON);
        assert!(vec.outlier_at(0).is_none());
        assert!(vec.outlier_at(6).is_none());
    }

    #[test]
    fn eight_outliers_roundtrip() {
        // Bits 0,3,7,12,20,33,50,63 set.
        let bits: &[u32] = &[0, 3, 7, 12, 20, 33, 50, 63];
        let mut bitmask: u64 = 0;
        for &b in bits {
            bitmask |= 1 << b;
        }
        let header = make_header(QuantMode::TurboQuant4b, 128, bitmask);
        let packed = vec![0xAAu8; 64]; // 128 dims × 4bpw = 64 bytes
        let outlier_list: Vec<(u32, f32)> = bits
            .iter()
            .enumerate()
            .map(|(i, &b)| (b, i as f32 * 1.1))
            .collect();
        let vec = UnifiedQuantizedVector::new(header, &packed, &outlier_list).unwrap();

        assert_eq!(vec.outlier_count(), 8);
        for (i, &b) in bits.iter().enumerate() {
            let (dim, val) = vec
                .outlier_at(b)
                .unwrap_or_else(|| panic!("outlier at {b} missing"));
            assert_eq!(dim, b);
            assert!(
                (val - i as f32 * 1.1f32).abs() < 1e-5,
                "value mismatch at dim {b}"
            );
        }
    }

    #[test]
    fn as_bytes_reborrow_via_ref() {
        let bitmask: u64 = 1 << 10;
        let header = make_header(QuantMode::RaBitQ, 64, bitmask);
        let packed = vec![0u8; 8]; // 64 dims / 8 = 8 bytes (1 bpw)
        let outliers = [(10u32, 7.77f32)];
        let vec = UnifiedQuantizedVector::new(header, &packed, &outliers).unwrap();

        let bytes = vec.as_bytes();
        let packed_bits_len = vec.packed_bits_len;
        let vref = UnifiedQuantizedVectorRef::from_bytes(bytes, packed_bits_len).unwrap();

        assert_eq!(vref.outlier_count(), 1);
        let (dim, val) = vref.outlier_at(10).unwrap();
        assert_eq!(dim, 10);
        assert!((val - 7.77).abs() < 1e-5);
    }

    #[test]
    fn header_field_roundtrip() {
        let header = QuantHeader {
            quant_mode: QuantMode::Bbq as u16,
            dim: 512,
            global_scale: 4.5,
            residual_norm: 0.99,
            dot_quantized: -1.23,
            outlier_bitmask: 0xDEAD_BEEF_0000_0001,
            reserved: [0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08],
        };
        let packed = vec![0u8; packed_bits_len(QuantMode::Bbq, 512)];

        // Build bitmask consistent with header.
        let bitmask = header.outlier_bitmask;
        let count = bitmask.count_ones() as usize;
        // Generate dummy outlier entries for each set bit (lowest bits first).
        let mut outliers: Vec<(u32, f32)> = Vec::with_capacity(count);
        for bit in 0u32..64 {
            if bitmask & (1u64 << bit) != 0 {
                outliers.push((bit, bit as f32));
            }
        }

        let vec = UnifiedQuantizedVector::new(header, &packed, &outliers).unwrap();
        let h = vec.header();

        assert_eq!(h.quant_mode, QuantMode::Bbq as u16);
        assert_eq!(h.dim, 512);
        assert!((h.global_scale - 4.5).abs() < 1e-5);
        assert!((h.residual_norm - 0.99).abs() < 1e-5);
        assert!((h.dot_quantized - (-1.23)).abs() < 1e-5);
        assert_eq!(h.outlier_bitmask, 0xDEAD_BEEF_0000_0001);
        assert_eq!(h.reserved, [0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08]);
    }

    #[test]
    fn outlier_ordering_popcnt() {
        // Outliers at dims [3, 17, 40] — bit 17 should be the second entry.
        let bitmask: u64 = (1 << 3) | (1 << 17) | (1 << 40);
        let header = make_header(QuantMode::Sq8, 64, bitmask);
        let packed = vec![0u8; 64];
        let outliers = [(3u32, 100.0f32), (17u32, 200.0f32), (40u32, 300.0f32)];
        let vec = UnifiedQuantizedVector::new(header, &packed, &outliers).unwrap();

        // outlier_at(17) should return the second entry (value 200.0).
        let (dim, val) = vec.outlier_at(17).expect("dim 17 should be an outlier");
        assert_eq!(dim, 17);
        assert!((val - 200.0).abs() < f32::EPSILON);

        let (dim0, val0) = vec.outlier_at(3).expect("dim 3 should be an outlier");
        assert_eq!(dim0, 3);
        assert!((val0 - 100.0).abs() < f32::EPSILON);

        let (dim2, val2) = vec.outlier_at(40).expect("dim 40 should be an outlier");
        assert_eq!(dim2, 40);
        assert!((val2 - 300.0).abs() < f32::EPSILON);
    }

    #[test]
    fn out_of_range_slot_returns_none() {
        let header = make_header(QuantMode::Binary, 64, 0);
        let packed = vec![0u8; 8];
        let vec = UnifiedQuantizedVector::new(header, &packed, &[]).unwrap();

        assert!(vec.outlier_at(64).is_none(), "slot 64 is out of range");
        assert!(vec.outlier_at(80).is_none(), "slot 80 is out of range");
        assert!(
            vec.outlier_at(u32::MAX).is_none(),
            "slot u32::MAX is out of range"
        );
    }

    #[test]
    fn outlier_count_mismatch_is_error() {
        // Bitmask says 1 outlier but we provide 0.
        let bitmask: u64 = 1 << 2;
        let header = make_header(QuantMode::Binary, 64, bitmask);
        let packed = vec![0u8; 8];
        let err = UnifiedQuantizedVector::new(header, &packed, &[]);
        assert!(
            err.is_err(),
            "should fail when outlier count mismatches bitmask"
        );
    }
}
