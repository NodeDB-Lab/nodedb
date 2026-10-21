// SPDX-License-Identifier: Apache-2.0

//! TIFF byte order and fixed-width reads.

/// The byte order a TIFF stores its numbers in.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ByteOrder {
    /// `II`.
    Little,
    /// `MM`.
    Big,
}

impl ByteOrder {
    pub const fn u16(self, raw: [u8; 2]) -> u16 {
        match self {
            ByteOrder::Little => u16::from_le_bytes(raw),
            ByteOrder::Big => u16::from_be_bytes(raw),
        }
    }

    pub const fn u32(self, raw: [u8; 4]) -> u32 {
        match self {
            ByteOrder::Little => u32::from_le_bytes(raw),
            ByteOrder::Big => u32::from_be_bytes(raw),
        }
    }

    pub const fn u64(self, raw: [u8; 8]) -> u64 {
        match self {
            ByteOrder::Little => u64::from_le_bytes(raw),
            ByteOrder::Big => u64::from_be_bytes(raw),
        }
    }

    /// `N` bytes of `bytes` at `at`, read as an `N`-byte unsigned integer.
    pub fn uint_at(self, bytes: &[u8], at: usize, width: usize) -> Option<u64> {
        match width {
            1 => bytes.get(at).map(|b| u64::from(*b)),
            2 => array::<2>(bytes, at).map(|raw| u64::from(self.u16(raw))),
            4 => array::<4>(bytes, at).map(|raw| u64::from(self.u32(raw))),
            8 => array::<8>(bytes, at).map(|raw| self.u64(raw)),
            _ => None,
        }
    }

    /// Write the low `width` bytes of `value` at `at` in this order.
    pub fn put_uint(self, bytes: &mut [u8], at: usize, width: usize, value: u64) {
        let raw = match self {
            ByteOrder::Little => value.to_le_bytes(),
            ByteOrder::Big => value.to_be_bytes(),
        };
        let field = match self {
            ByteOrder::Little => raw.get(..width),
            ByteOrder::Big => raw.get(8usize.saturating_sub(width)..),
        };
        if let (Some(field), Some(out)) = (field, bytes.get_mut(at..at.saturating_add(width))) {
            out.copy_from_slice(field);
        }
    }
}

/// The `N` bytes of `bytes` at `at`, or `None` past the end.
pub fn array<const N: usize>(bytes: &[u8], at: usize) -> Option<[u8; N]> {
    bytes.get(at..at.checked_add(N)?)?.try_into().ok()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn both_orders_read_and_write() {
        let bytes = [0x12, 0x34, 0x56, 0x78];
        assert_eq!(ByteOrder::Little.uint_at(&bytes, 0, 2), Some(0x3412));
        assert_eq!(ByteOrder::Big.uint_at(&bytes, 0, 4), Some(0x1234_5678));
        assert_eq!(ByteOrder::Big.uint_at(&bytes, 2, 4), None);
        let mut out = [0u8; 4];
        ByteOrder::Big.put_uint(&mut out, 1, 2, 0xABCD);
        assert_eq!(out, [0, 0xAB, 0xCD, 0]);
        ByteOrder::Little.put_uint(&mut out, 0, 2, 0xABCD);
        assert_eq!(out, [0xCD, 0xAB, 0xCD, 0]);
    }
}
