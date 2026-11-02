// SPDX-License-Identifier: Apache-2.0

//! `parse` — decode and fully validate a plaintext backup envelope.

use std::mem::size_of;

use crate::decode_bounds::checked_decode_capacity;

use super::types::{Envelope, EnvelopeError, EnvelopeMeta, Section, read2, read4, read8};
use super::types::{HEADER_LEN, MAGIC, SECTION_OVERHEAD, TRAILER_LEN, VERSION};

/// Parse and fully validate a plaintext backup envelope.
///
/// Rejects bytes that do not carry [`VERSION`] in the header.
/// Use [`crate::backup_envelope::parse_encrypted`] for encrypted envelopes.
pub fn parse(bytes: &[u8], max_total: u64) -> Result<Envelope, EnvelopeError> {
    if bytes.len() as u64 > max_total {
        return Err(EnvelopeError::OverSizeTotal { cap: max_total });
    }
    if bytes.len() < HEADER_LEN + TRAILER_LEN {
        return Err(EnvelopeError::Truncated);
    }

    // Header.
    let header_bytes = &bytes[..HEADER_LEN];
    if &header_bytes[0..4] != MAGIC {
        return Err(EnvelopeError::BadMagic);
    }
    let version = header_bytes[4];
    if version != VERSION {
        return Err(EnvelopeError::UnsupportedVersion(version));
    }

    parse_validated_body(bytes, header_bytes)
}

/// Inner parse shared by the plain and encrypted paths.
/// Caller has already verified magic, version, and (for encrypted path)
/// the crypto header. `bytes` is the full envelope; `header_bytes` is `&bytes[..HEADER_LEN]`.
pub(super) fn parse_validated_body(
    bytes: &[u8],
    header_bytes: &[u8],
) -> Result<Envelope, EnvelopeError> {
    // Validate header CRC.
    let claimed_header_crc = u32::from_le_bytes(read4(&header_bytes[48..52]));
    let actual_header_crc = crc32c::crc32c(&header_bytes[..48]);
    if claimed_header_crc != actual_header_crc {
        return Err(EnvelopeError::HeaderCrcMismatch);
    }

    let meta = EnvelopeMeta {
        tenant_id: u64::from_le_bytes(read8(&header_bytes[8..16])),
        source_vshard_count: u16::from_le_bytes(read2(&header_bytes[16..18])),
        hash_seed: u64::from_le_bytes(read8(&header_bytes[24..32])),
        snapshot_watermark: u64::from_le_bytes(read8(&header_bytes[32..40])),
    };
    let section_count = u16::from_le_bytes(read2(&header_bytes[40..42]));

    // Trailer position: tail 4 bytes.
    let trailer_start = bytes.len() - TRAILER_LEN;
    let claimed_trailer_crc = u32::from_le_bytes(read4(&bytes[trailer_start..]));
    let actual_trailer_crc = crc32c::crc32c(&bytes[..trailer_start]);
    if claimed_trailer_crc != actual_trailer_crc {
        return Err(EnvelopeError::TrailerCrcMismatch);
    }

    parse_sections(bytes, section_count, HEADER_LEN, trailer_start, meta)
}

pub(super) fn parse_sections(
    bytes: &[u8],
    section_count: u16,
    body_start: usize,
    trailer_start: usize,
    meta: EnvelopeMeta,
) -> Result<Envelope, EnvelopeError> {
    let section_capacity = checked_section_capacity(
        section_count,
        trailer_start.saturating_sub(body_start),
        SECTION_OVERHEAD,
    )?;
    let mut cursor = body_start;
    let mut sections = Vec::with_capacity(section_capacity);
    for _ in 0..section_count {
        if cursor + SECTION_OVERHEAD > trailer_start {
            return Err(EnvelopeError::Truncated);
        }
        let origin_node_id = u64::from_le_bytes(read8(&bytes[cursor..cursor + 8]));
        let body_len = u32::from_le_bytes(read4(&bytes[cursor + 8..cursor + 12])) as usize;
        let body_start_inner = cursor + 12;
        let body_end = body_start_inner + body_len;
        let crc_end = body_end + 4;
        if crc_end > trailer_start {
            return Err(EnvelopeError::Truncated);
        }
        let body = bytes[body_start_inner..body_end].to_vec();
        let claimed_body_crc = u32::from_le_bytes(read4(&bytes[body_end..crc_end]));
        if crc32c::crc32c(&body) != claimed_body_crc {
            return Err(EnvelopeError::BodyCrcMismatch);
        }
        sections.push(Section {
            origin_node_id,
            body,
        });
        cursor = crc_end;
    }
    if cursor != trailer_start {
        return Err(EnvelopeError::Truncated);
    }

    Ok(Envelope { meta, sections })
}

/// Bounds a section container by the minimum encoded size of each section.
/// This runs before allocation so an untrusted header count cannot reserve
/// more entries than the bytes remaining in the frame can describe.
pub(super) fn checked_section_capacity(
    section_count: u16,
    remaining_bytes: usize,
    minimum_section_bytes: usize,
) -> Result<usize, EnvelopeError> {
    if minimum_section_bytes == 0 {
        return Err(EnvelopeError::Truncated);
    }
    let count = usize::from(section_count);
    checked_decode_capacity(
        count,
        size_of::<Section>(),
        remaining_bytes,
        minimum_section_bytes,
        usize::from(u16::MAX),
        usize::MAX,
    )
    .ok_or(EnvelopeError::Truncated)
}

// ── shared tests for plaintext path ─────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use crate::backup_envelope::types::{DEFAULT_MAX_SECTION_BYTES, DEFAULT_MAX_TOTAL_BYTES};
    use crate::backup_envelope::write::EnvelopeWriter;

    fn meta() -> EnvelopeMeta {
        EnvelopeMeta {
            tenant_id: 42_u64,
            source_vshard_count: 1024,
            hash_seed: 0,
            snapshot_watermark: 12345,
        }
    }

    #[test]
    fn rejects_huge_section_count_with_tiny_body_before_allocation() {
        let mut bytes = EnvelopeWriter::new(meta()).finalize();
        bytes[40..42].copy_from_slice(&u16::MAX.to_le_bytes());
        let header_crc = crc32c::crc32c(&bytes[..48]);
        bytes[48..52].copy_from_slice(&header_crc.to_le_bytes());
        let trailer_start = bytes.len() - TRAILER_LEN;
        let trailer_crc = crc32c::crc32c(&bytes[..trailer_start]);
        bytes[trailer_start..].copy_from_slice(&trailer_crc.to_le_bytes());
        assert_eq!(
            parse(&bytes, DEFAULT_MAX_TOTAL_BYTES),
            Err(EnvelopeError::Truncated)
        );
    }

    #[test]
    fn empty_envelope_roundtrips() {
        let bytes = EnvelopeWriter::new(meta()).finalize();
        let env = parse(&bytes, DEFAULT_MAX_TOTAL_BYTES).unwrap();
        assert_eq!(env.meta, meta());
        assert!(env.sections.is_empty());
    }

    #[test]
    fn multi_section_roundtrips() {
        let mut w = EnvelopeWriter::new(meta());
        w.push_section(1, b"one".to_vec()).unwrap();
        w.push_section(2, b"two-payload".to_vec()).unwrap();
        w.push_section(3, vec![]).unwrap();
        let bytes = w.finalize();

        let env = parse(&bytes, DEFAULT_MAX_TOTAL_BYTES).unwrap();
        assert_eq!(env.sections.len(), 3);
        assert_eq!(env.sections[0].origin_node_id, 1);
        assert_eq!(env.sections[0].body, b"one");
        assert_eq!(env.sections[1].origin_node_id, 2);
        assert_eq!(env.sections[1].body, b"two-payload");
        assert_eq!(env.sections[2].body, b"");
    }

    #[test]
    fn rejects_short_input() {
        assert_eq!(
            parse(b"NDBB", DEFAULT_MAX_TOTAL_BYTES),
            Err(EnvelopeError::Truncated)
        );
    }

    #[test]
    fn rejects_bad_magic() {
        let mut bytes = EnvelopeWriter::new(meta()).finalize();
        bytes[0] = b'X';
        match parse(&bytes, DEFAULT_MAX_TOTAL_BYTES).unwrap_err() {
            EnvelopeError::BadMagic => {}
            other => panic!("expected BadMagic, got {other:?}"),
        }
    }

    #[test]
    fn rejects_unsupported_version() {
        let mut bytes = EnvelopeWriter::new(meta()).finalize();
        bytes[4] = VERSION + 1;
        match parse(&bytes, DEFAULT_MAX_TOTAL_BYTES).unwrap_err() {
            EnvelopeError::UnsupportedVersion(v) if v == VERSION + 1 => {}
            other => panic!(
                "expected UnsupportedVersion({}), got {other:?}",
                VERSION + 1
            ),
        }
    }

    #[test]
    fn u64_tenant_id_roundtrips() {
        let large_meta = EnvelopeMeta {
            tenant_id: u32::MAX as u64 + 1,
            source_vshard_count: 512,
            hash_seed: 0xDEAD_BEEF_CAFE_1234,
            snapshot_watermark: 9_999_999_999,
        };
        let mut w = EnvelopeWriter::new(large_meta);
        w.push_section(42, b"payload".to_vec()).unwrap();
        let bytes = w.finalize();
        let env = parse(&bytes, DEFAULT_MAX_TOTAL_BYTES).unwrap();
        assert_eq!(env.meta, large_meta);
        assert_eq!(env.sections.len(), 1);
        assert_eq!(env.sections[0].body, b"payload");
    }

    #[test]
    fn rejects_header_crc_corruption() {
        let mut bytes = EnvelopeWriter::new(meta()).finalize();
        bytes[8] ^= 0xFF;
        assert_eq!(
            parse(&bytes, DEFAULT_MAX_TOTAL_BYTES),
            Err(EnvelopeError::HeaderCrcMismatch)
        );
    }

    #[test]
    fn rejects_body_crc_corruption() {
        let mut w = EnvelopeWriter::new(meta());
        w.push_section(7, b"hello".to_vec()).unwrap();
        let mut bytes = w.finalize();
        let body_off = HEADER_LEN + 12;
        bytes[body_off] ^= 0xFF;
        // Recompute trailer to isolate body-CRC enforcement.
        let trailer_off = bytes.len() - TRAILER_LEN;
        let new_trailer = crc32c::crc32c(&bytes[..trailer_off]);
        bytes[trailer_off..].copy_from_slice(&new_trailer.to_le_bytes());
        assert_eq!(
            parse(&bytes, DEFAULT_MAX_TOTAL_BYTES),
            Err(EnvelopeError::BodyCrcMismatch)
        );
    }

    #[test]
    fn rejects_trailer_crc_corruption() {
        let mut w = EnvelopeWriter::new(meta());
        w.push_section(7, b"x".to_vec()).unwrap();
        let mut bytes = w.finalize();
        let last = bytes.len() - 1;
        bytes[last] ^= 0xFF;
        assert_eq!(
            parse(&bytes, DEFAULT_MAX_TOTAL_BYTES),
            Err(EnvelopeError::TrailerCrcMismatch)
        );
    }

    #[test]
    fn rejects_oversized_total() {
        let mut w = EnvelopeWriter::with_caps(meta(), 64, DEFAULT_MAX_SECTION_BYTES);
        let err = w.push_section(1, vec![0u8; 1024]).unwrap_err();
        assert!(matches!(err, EnvelopeError::OverSizeTotal { .. }));
    }

    #[test]
    fn rejects_oversized_section_at_write() {
        let mut w = EnvelopeWriter::with_caps(meta(), DEFAULT_MAX_TOTAL_BYTES, 8);
        let err = w.push_section(1, vec![0u8; 9]).unwrap_err();
        assert!(matches!(err, EnvelopeError::OverSizeSection { .. }));
    }

    #[test]
    fn rejects_oversized_total_at_parse() {
        let bytes = EnvelopeWriter::new(meta()).finalize();
        assert!(matches!(
            parse(&bytes, 4),
            Err(EnvelopeError::OverSizeTotal { .. })
        ));
    }

    #[test]
    fn truncated_section_body() {
        let mut w = EnvelopeWriter::new(meta());
        w.push_section(1, b"hello world".to_vec()).unwrap();
        let bytes = w.finalize();
        let truncated = &bytes[..bytes.len() - 8];
        assert!(parse(truncated, DEFAULT_MAX_TOTAL_BYTES).is_err());
    }

    /// Asserts `NDBB` magic at [0..4], VERSION == 1 at [4], and that the
    /// header CRC at [48..52] covers header bytes [0..48].
    #[test]
    fn golden_backup_envelope_format() {
        use crate::backup_envelope::types::{HEADER_LEN, MAGIC, VERSION};

        let bytes = EnvelopeWriter::new(meta()).finalize();

        // Magic at [0..4].
        assert_eq!(&bytes[0..4], MAGIC.as_slice(), "magic mismatch");

        // VERSION == 1 at [4].
        assert_eq!(bytes[4], VERSION, "version mismatch");
        assert_eq!(bytes[4], 1u8, "expected VERSION == 1");

        // Header CRC at [48..52] covers [0..48].
        assert!(bytes.len() >= HEADER_LEN, "envelope too short for header");
        let stored_crc = u32::from_le_bytes([bytes[48], bytes[49], bytes[50], bytes[51]]);
        let recomputed = crc32c::crc32c(&bytes[..48]);
        assert_eq!(stored_crc, recomputed, "header CRC mismatch");
    }
}
