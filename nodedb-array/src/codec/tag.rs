// Tag byte identifying the tile codec on the wire. Sits as the very first
// byte of every new-format tile payload (after BlockFraming unwraps).
//
// Legacy v3 segments had no tag — the payload began with a msgpack map
// header (0x80..=0x8f for fixmap, 0xde for map16, 0xdf for map32).
// The reader peeks this byte and uses None to signal "fall through to
// zerompk legacy path".

/// One-byte codec tag at the front of a new-format (v4+) tile payload.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum CodecTag {
    /// Raw msgpack fallback: cell_count < 8 or sentinel-only tiles where
    /// codec overhead exceeds potential savings.
    Raw = 0,
    /// Structural codec: coord delta + fastlanes surrogates + gorilla timestamps.
    Structural = 1,
}

impl CodecTag {
    pub fn from_byte(b: u8) -> Option<Self> {
        match b {
            0 => Some(CodecTag::Raw),
            1 => Some(CodecTag::Structural),
            _ => None,
        }
    }

    pub fn as_byte(self) -> u8 {
        self as u8
    }
}

/// Peek at the first byte of a tile payload and determine whether it is a
/// legacy (v3) msgpack tile or a new-format tile.
///
/// Returns `None` when the byte is a msgpack map-start (legacy path).
/// Returns `Some(tag)` for a recognized new-format tag byte.
/// Returns `None` for any other byte that cannot be decoded as a known tag
/// (treated as corrupt — callers should surface an error after checking).
pub fn peek_tag(payload: &[u8]) -> Option<CodecTag> {
    let first = *payload.first()?;
    // msgpack fixmap: 0x80..=0x8f
    // msgpack map16: 0xde
    // msgpack map32: 0xdf
    if matches!(first, 0x80..=0x8f | 0xde | 0xdf) {
        return None;
    }
    CodecTag::from_byte(first)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn raw_tag_roundtrips() {
        assert_eq!(CodecTag::Raw.as_byte(), 0);
        assert_eq!(CodecTag::from_byte(0), Some(CodecTag::Raw));
    }

    #[test]
    fn structural_tag_roundtrips() {
        assert_eq!(CodecTag::Structural.as_byte(), 1);
        assert_eq!(CodecTag::from_byte(1), Some(CodecTag::Structural));
    }

    #[test]
    fn unknown_byte_returns_none() {
        assert_eq!(CodecTag::from_byte(42), None);
        assert_eq!(CodecTag::from_byte(255), None);
    }

    #[test]
    fn peek_tag_detects_msgpack_fixmap() {
        // fixmap range: 0x80..=0x8f
        for b in 0x80u8..=0x8fu8 {
            let payload = [b, 0x00];
            assert_eq!(
                peek_tag(&payload),
                None,
                "fixmap byte {b:#04x} should be None"
            );
        }
    }

    #[test]
    fn peek_tag_detects_msgpack_map16_map32() {
        assert_eq!(peek_tag(&[0xde, 0x00]), None);
        assert_eq!(peek_tag(&[0xdf, 0x00]), None);
    }

    #[test]
    fn peek_tag_detects_raw_tag() {
        assert_eq!(peek_tag(&[0x00, 0x01, 0x02]), Some(CodecTag::Raw));
    }

    #[test]
    fn peek_tag_detects_structural_tag() {
        assert_eq!(peek_tag(&[0x01, 0x00]), Some(CodecTag::Structural));
    }

    #[test]
    fn peek_tag_empty_returns_none() {
        assert_eq!(peek_tag(&[]), None);
    }

    #[test]
    fn peek_tag_unknown_byte_returns_none() {
        // Byte 42 is not a valid tag and not a msgpack map header.
        assert_eq!(peek_tag(&[42]), None);
    }
}
