// SPDX-License-Identifier: Apache-2.0

//! Judging a record header whose format version this build does not read.
//!
//! A segment written by another build holds intact records in another format.
//! The open must refuse that segment and name both versions. A damaged header
//! in this build's format is not a format gap: the reader stops there, as at
//! any other damaged header.
//!
//! One writer never mixes formats inside one segment, so the format is judged
//! at the segment's first record only. A mismatch later in the segment is
//! damage. No build writes format 0, so a zeroed version is a torn write.
//!
//! A flipped bit in the version field of this build's record leaves every
//! other field and the checksum intact. The checksum covers the version, so
//! recomputing it with this build's version restores a match. Another build's
//! header puts its fields at other offsets, so its checksum does not match.

use super::header::{RecordHeader, WAL_FORMAT_VERSION};

/// Whether `header`, whose version this build does not read, can be another
/// build's format. Only a non-zero version at the segment's first record
/// can. Every other mismatch is damage.
pub fn may_be_format_gap(header: &RecordHeader, at_first_record: bool) -> bool {
    at_first_record && header.format_version != 0
}

/// Whether a header that [`may_be_format_gap`] accepted is another build's
/// format.
///
/// `payload` is the bytes the header declares, or `None` when the segment
/// does not hold that many. Without them the header is judged a format gap:
/// nothing proves it is this build's record.
pub fn is_format_gap(header: &RecordHeader, payload: Option<&[u8]>) -> bool {
    let Some(payload) = payload else {
        return true;
    };
    let restored = RecordHeader {
        format_version: WAL_FORMAT_VERSION,
        ..*header
    };
    restored.compute_checksum(payload) != header.crc32c
}

/// Read the payload `header` declares from `file`, whose cursor sits at
/// `payload_start`. Returns `None` when the declared length is over the
/// payload limit or past the end of the file.
#[cfg(not(target_arch = "wasm32"))]
pub(crate) fn read_declared_payload(
    file: &mut std::fs::File,
    header: &RecordHeader,
    payload_start: u64,
) -> crate::error::Result<Option<Vec<u8>>> {
    use std::io::Read;

    use super::header::MAX_WAL_PAYLOAD_SIZE;

    let Ok(len) = usize::try_from(header.payload_len) else {
        return Ok(None);
    };
    if len > MAX_WAL_PAYLOAD_SIZE {
        return Ok(None);
    }
    let file_len = file.metadata()?.len();
    match payload_start.checked_add(u64::from(header.payload_len)) {
        Some(end) if end <= file_len => {}
        _ => return Ok(None),
    }
    let mut payload = vec![0u8; len];
    file.read_exact(&mut payload)?;
    Ok(Some(payload))
}

/// Rewrite the record at `start` of `bytes` the way another build seals one:
/// declare `version`, then recompute the checksum.
#[cfg(test)]
pub(crate) fn reseal_with_version(bytes: &mut [u8], start: usize, version: u16) {
    use super::header::HEADER_SIZE;

    let head: [u8; HEADER_SIZE] = bytes[start..start + HEADER_SIZE]
        .try_into()
        .expect("a whole header");
    let mut header = RecordHeader::from_bytes(&head);
    header.format_version = version;
    let payload_start = start + HEADER_SIZE;
    let payload_end = payload_start + header.payload_len as usize;
    header.crc32c = header.compute_checksum(&bytes[payload_start..payload_end]);
    bytes[start..payload_start].copy_from_slice(&header.to_bytes());
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::record::{NO_EVENT_SOURCE, WAL_MAGIC};

    fn sealed(payload: &[u8]) -> RecordHeader {
        let mut header = RecordHeader {
            magic: WAL_MAGIC,
            format_version: WAL_FORMAT_VERSION,
            record_type: 1,
            lsn: 7,
            tenant_id: 1,
            vshard_id: 0,
            payload_len: u32::try_from(payload.len()).unwrap(),
            database_id: 0,
            apply_key: 0,
            event_source: NO_EVENT_SOURCE,
            commit_hlc: 0,
            system_ordinal: crate::record::NO_SYSTEM_ORDINAL,
            crc32c: 0,
        };
        header.crc32c = header.compute_checksum(payload);
        header
    }

    #[test]
    fn only_a_non_zero_version_at_the_first_record_may_be_a_gap() {
        let mut header = sealed(b"x");
        header.format_version = WAL_FORMAT_VERSION + 1;
        assert!(may_be_format_gap(&header, true));
        assert!(!may_be_format_gap(&header, false));
        header.format_version = 0;
        assert!(!may_be_format_gap(&header, true));
    }

    #[test]
    fn a_flipped_version_bit_in_this_builds_record_is_damage() {
        let payload = b"payload";
        let mut header = sealed(payload);
        header.format_version = WAL_FORMAT_VERSION ^ 0x0100;
        assert!(!is_format_gap(&header, Some(payload)));
    }

    #[test]
    fn a_record_sealed_in_another_version_is_a_gap() {
        let payload = b"payload";
        let mut header = sealed(payload);
        header.format_version = WAL_FORMAT_VERSION + 1;
        header.crc32c = header.compute_checksum(payload);
        assert!(is_format_gap(&header, Some(payload)));
    }

    #[test]
    fn a_header_without_its_payload_is_a_gap() {
        let mut header = sealed(b"payload");
        header.format_version = WAL_FORMAT_VERSION + 1;
        assert!(is_format_gap(&header, None));
    }
}
