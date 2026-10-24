// SPDX-License-Identifier: Apache-2.0

//! WKB type words, flags and decoder limits shared by the reader and
//! writers.

use nodedb_types::geometry::{Dimension, GeometryKind};

pub(super) const BYTE_ORDER_BE: u8 = 0;
pub(super) const BYTE_ORDER_LE: u8 = 1;

/// PostGIS EWKB flag: Z ordinates follow X and Y.
pub const EWKB_Z_FLAG: u32 = 0x8000_0000;
/// PostGIS EWKB flag: M ordinates follow X, Y and Z.
pub const EWKB_M_FLAG: u32 = 0x4000_0000;
/// PostGIS EWKB flag: a 4-byte SRID follows the type word.
pub const EWKB_SRID_FLAG: u32 = 0x2000_0000;

/// The bits of a type word the EWKB flags do not use.
pub(super) const TYPE_CODE_MASK: u32 = 0x0FFF_FFFF;

/// ISO 13249 adds 1000 for Z, 2000 for M, 3000 for ZM to the base code.
pub(super) const ISO_DIMENSION_STEP: u32 = 1000;

// WKB count fields are attacker-controlled. The bound caps allocation and
// parsing work even when the encoded input itself is very large.
pub(super) const MAX_WKB_ELEMENTS: usize = 1_000_000;
pub(super) const MAX_WKB_BYTES: usize = 64 * 1024 * 1024;
// Nested geometry collections are recursive on the stack. Keep the budget
// explicit so hostile nesting cannot exhaust the decoder thread's stack.
pub(super) const MAX_WKB_DEPTH: usize = 64;

/// The ISO type word of `kind` in `dimension`.
pub(super) fn iso_type_word(kind: GeometryKind, dimension: Dimension) -> u32 {
    let step = match dimension {
        Dimension::Xy => 0,
        Dimension::Xyz => 1,
        Dimension::Xym => 2,
        Dimension::Xyzm => 3,
    };
    kind.wkb_code() + step * ISO_DIMENSION_STEP
}

/// The EWKB type word of `kind` in `dimension`, SRID flag excluded.
pub(super) fn ewkb_type_word(kind: GeometryKind, dimension: Dimension) -> u32 {
    let mut word = kind.wkb_code();
    if dimension.has_z() {
        word |= EWKB_Z_FLAG;
    }
    if dimension.has_m() {
        word |= EWKB_M_FLAG;
    }
    word
}

/// A decoded type word.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) struct TypeWord {
    pub kind: GeometryKind,
    pub dimension: Dimension,
    pub has_srid: bool,
}

/// Why a type word does not decode.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum TypeWordError {
    UnknownCode,
    /// EWKB flags and the ISO thousands name different dimensions.
    ConflictingDimension,
}

/// Decode a type word in either the ISO or the EWKB convention.
pub(super) fn decode_type_word(raw: u32) -> Result<TypeWord, TypeWordError> {
    let code = raw & TYPE_CODE_MASK;
    let iso_step = code / ISO_DIMENSION_STEP;
    let kind =
        GeometryKind::from_wkb_code(code % ISO_DIMENSION_STEP).ok_or(TypeWordError::UnknownCode)?;
    let iso = match iso_step {
        0 => None,
        1 => Some(Dimension::Xyz),
        2 => Some(Dimension::Xym),
        3 => Some(Dimension::Xyzm),
        _ => return Err(TypeWordError::UnknownCode),
    };
    let flags = Dimension::from_flags(raw & EWKB_Z_FLAG != 0, raw & EWKB_M_FLAG != 0);
    let dimension = match (iso, flags) {
        (None, flags) => flags,
        (Some(iso), Dimension::Xy) => iso,
        (Some(iso), flags) if iso == flags => iso,
        (Some(_), _) => return Err(TypeWordError::ConflictingDimension),
    };
    Ok(TypeWord {
        kind,
        dimension,
        has_srid: raw & EWKB_SRID_FLAG != 0,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn iso_and_ewkb_words_decode_alike() {
        for dimension in [
            Dimension::Xy,
            Dimension::Xyz,
            Dimension::Xym,
            Dimension::Xyzm,
        ] {
            for kind in [GeometryKind::Point, GeometryKind::GeometryCollection] {
                let iso = decode_type_word(iso_type_word(kind, dimension)).unwrap();
                let ewkb = decode_type_word(ewkb_type_word(kind, dimension)).unwrap();
                assert_eq!(iso, ewkb);
                assert_eq!(iso.dimension, dimension);
                assert_eq!(iso.kind, kind);
            }
        }
        assert_eq!(iso_type_word(GeometryKind::Point, Dimension::Xyzm), 3001);
    }

    #[test]
    fn conflicting_and_unknown_words_fail() {
        assert_eq!(
            decode_type_word(1001 | EWKB_M_FLAG),
            Err(TypeWordError::ConflictingDimension)
        );
        assert_eq!(decode_type_word(8), Err(TypeWordError::UnknownCode));
        assert_eq!(decode_type_word(4001), Err(TypeWordError::UnknownCode));
    }
}
