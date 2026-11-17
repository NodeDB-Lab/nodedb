// SPDX-License-Identifier: Apache-2.0

//! Alignment padding records.
//!
//! O_DIRECT requires every write to be a multiple of the device block size, so
//! the tail of a flush batch is padded out to the next boundary and the file
//! offset advances by the padded length. The bytes in that gap are not part of
//! any record.
//!
//! Leaving the gap unframed breaks replay. A reader walks records back to back,
//! so it lands on the padding and tries to parse it as a record header — the
//! bytes are stale content from a previous batch (the write buffer is reused
//! without being re-zeroed), and even all-zero padding fails the magic check.
//! The reader treats that as the end of the committed prefix, so a segment
//! holding more than one flush batch replays only its first batch.
//!
//! Padding is therefore framed as a record: the gap is filled with a `Noop`
//! whose payload is zeroes, keeping the record chain continuous across batch
//! boundaries so every reader walks the whole segment. Padding records carry
//! LSN 0, are never mirrored into the double-write buffer, and are skipped by
//! all readers rather than surfaced to replay.

use super::header::HEADER_SIZE;
use super::types::RecordType;
use super::wal_record::{WalRecord, WalRecordArgs};
use crate::align::{AlignedBuf, round_up};
use crate::error::{Result, WalError};

/// Smallest on-disk footprint a padding record can occupy: a bare header with
/// an empty payload. A gap narrower than this cannot be framed at all.
pub const MIN_PADDING_RECORD_SIZE: usize = HEADER_SIZE;

/// On-disk size of the padding record needed to bring `len` up to an
/// `alignment` boundary, or `None` when `len` is already aligned.
///
/// A gap narrower than a record header cannot hold one, so the span is
/// extended by a whole extra block. The result is always either `0` or at
/// least `MIN_PADDING_RECORD_SIZE`, and `len + span` is always aligned.
pub fn padding_span(len: usize, alignment: usize) -> Option<usize> {
    let gap = round_up(len, alignment) - len;
    if gap == 0 {
        return None;
    }
    if gap < MIN_PADDING_RECORD_SIZE {
        Some(gap + alignment)
    } else {
        Some(gap)
    }
}

/// Build a padding record occupying exactly `total_size` bytes on disk.
///
/// Padding is never encrypted: it carries no data, and the AES-GCM tag would
/// change its on-disk length, which is the one property that has to be exact.
pub fn padding_record(total_size: usize) -> Result<WalRecord> {
    let payload_len = total_size
        .checked_sub(HEADER_SIZE)
        .ok_or(WalError::AlignmentViolation {
            context: "alignment padding is narrower than a WAL record header",
            required: HEADER_SIZE,
            actual: total_size,
        })?;

    WalRecord::new(WalRecordArgs {
        record_type: RecordType::Noop as u32,
        lsn: 0,
        tenant_id: 0,
        vshard_id: 0,
        database_id: 0,
        payload: vec![0u8; payload_len],
        encryption_key: None,
        preamble_bytes: None,
    })
}

/// Fill the tail of `buffer` with a framed padding record so it ends on an
/// `alignment` boundary. Shared by `WalWriter::flush_buffer` and
/// `UringWriter::submit_and_wait_write`, whose O_DIRECT batches both need the
/// same framing before submission — see the module doc for why the gap can't
/// be left unframed.
///
/// `context` is threaded into [`WalError::AlignmentViolation`] so a caller
/// can identify which writer's buffer ran out of room.
pub(crate) fn pad_buffer_to_alignment(
    buffer: &mut AlignedBuf,
    alignment: usize,
    context: &'static str,
) -> Result<()> {
    let Some(span) = padding_span(buffer.len(), alignment) else {
        return Ok(());
    };
    if buffer.remaining() < span {
        return Err(WalError::AlignmentViolation {
            context,
            required: span,
            actual: buffer.remaining(),
        });
    }
    let padding = padding_record(span)?;
    buffer.write(&padding.header.to_bytes());
    buffer.write(&padding.payload);
    Ok(())
}

/// Frame one record alone in an aligned buffer sized to it.
///
/// A record larger than a writer's batch buffer goes to the file by itself.
/// Under O_DIRECT it closes with a padding record, as a batch does, so the
/// written length and the next file offset stay on a boundary. The written
/// bytes are `as_slice()` of the result in both modes.
pub(crate) fn one_shot_record_buffer(
    header: &[u8],
    payload: &[u8],
    alignment: usize,
    direct_io: bool,
) -> Result<AlignedBuf> {
    let len = header.len().saturating_add(payload.len());
    let reserve = if direct_io {
        alignment.saturating_add(MIN_PADDING_RECORD_SIZE)
    } else {
        0
    };
    let mut buffer = AlignedBuf::new(len.saturating_add(reserve), alignment)?;
    buffer.write(header);
    buffer.write(payload);
    if direct_io {
        pad_buffer_to_alignment(
            &mut buffer,
            alignment,
            "one-shot record buffer has no room for its alignment padding record",
        )?;
    }
    Ok(buffer)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::align::DEFAULT_ALIGNMENT;
    use crate::record::RecordHeader;

    #[test]
    fn aligned_length_needs_no_padding() {
        assert_eq!(padding_span(0, DEFAULT_ALIGNMENT), None);
        assert_eq!(padding_span(DEFAULT_ALIGNMENT, DEFAULT_ALIGNMENT), None);
        assert_eq!(padding_span(2 * DEFAULT_ALIGNMENT, DEFAULT_ALIGNMENT), None);
    }

    #[test]
    fn span_closes_the_gap_to_the_next_boundary() {
        let span = padding_span(100, DEFAULT_ALIGNMENT).unwrap();
        assert_eq!(span, DEFAULT_ALIGNMENT - 100);
        assert_eq!((100 + span) % DEFAULT_ALIGNMENT, 0);
    }

    #[test]
    fn narrow_gap_is_extended_by_a_whole_block() {
        // One byte short of the boundary: a header does not fit, so the span
        // must borrow an entire extra block and still land on a boundary.
        let len = DEFAULT_ALIGNMENT - 1;
        let span = padding_span(len, DEFAULT_ALIGNMENT).unwrap();
        assert!(span >= MIN_PADDING_RECORD_SIZE);
        assert_eq!((len + span) % DEFAULT_ALIGNMENT, 0);
    }

    #[test]
    fn every_unaligned_length_yields_a_framable_aligned_span() {
        for len in 0..(2 * DEFAULT_ALIGNMENT) {
            match padding_span(len, DEFAULT_ALIGNMENT) {
                None => assert_eq!(len % DEFAULT_ALIGNMENT, 0),
                Some(span) => {
                    assert!(
                        span >= MIN_PADDING_RECORD_SIZE,
                        "span {span} cannot be framed"
                    );
                    assert_eq!((len + span) % DEFAULT_ALIGNMENT, 0);
                    assert!(padding_record(span).is_ok());
                }
            }
        }
    }

    #[test]
    fn padding_record_occupies_exactly_its_span() {
        let record = padding_record(512).unwrap();
        assert_eq!(HEADER_SIZE + record.payload.len(), 512);
        assert_eq!(record.header.lsn, 0);
        assert_eq!(record.header.record_type, RecordType::Noop as u32);
        assert!(record.verify_checksum().is_ok());
    }

    #[test]
    fn one_shot_buffer_ends_on_a_boundary_under_direct_io() {
        let header = [1u8; HEADER_SIZE];
        let payload = vec![2u8; 3 * DEFAULT_ALIGNMENT + 7];
        let direct = one_shot_record_buffer(&header, &payload, DEFAULT_ALIGNMENT, true).unwrap();
        assert_eq!(direct.len() % DEFAULT_ALIGNMENT, 0);
        assert_eq!(&direct.as_slice()[..HEADER_SIZE], &header);
        let tail = &direct.as_slice()[HEADER_SIZE + payload.len()..];
        let padding = RecordHeader::from_bytes(tail[..HEADER_SIZE].try_into().unwrap());
        assert_eq!(padding.record_type, RecordType::Noop as u32);
        assert_eq!(HEADER_SIZE + padding.payload_len as usize, tail.len());

        let buffered = one_shot_record_buffer(&header, &payload, DEFAULT_ALIGNMENT, false).unwrap();
        assert_eq!(buffered.len(), HEADER_SIZE + payload.len());
    }

    #[test]
    fn span_below_a_header_is_rejected() {
        assert!(matches!(
            padding_record(HEADER_SIZE - 1),
            Err(WalError::AlignmentViolation { .. })
        ));
    }
}
