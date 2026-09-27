// SPDX-License-Identifier: Apache-2.0

//! Reading records back out of the double-write buffer.
//!
//! Recovery scans **all** slots rather than trusting the file header's
//! `count` / `write_pos`, which a crash can leave stale or corrupt. Each slot
//! is self-describing: a checksummed prefix carries its sequence number and
//! length, and the record's own CRC says whether the bytes are usable.

use crate::error::Result;
use crate::record::WalRecord;

use super::buffer::DoubleWriteBuffer;
use super::layout::SlotPrefix;

#[cfg(not(target_arch = "wasm32"))]
use super::layout::{DWB_CAPACITY, DWB_SLOT_STRIDE, SLOT_PREFIX_SIZE, slot_offset};

impl DoubleWriteBuffer {
    /// Try to recover a WAL record by LSN from the double-write buffer.
    ///
    /// When two slots hold the same LSN — a crash between the DWB fsync and
    /// the WAL fsync leaves an LSN durable here but absent from the WAL, so
    /// recovery hands that LSN to a *different* record — the copy with the
    /// highest slot sequence number wins. Returning the older one would
    /// resurrect a payload that was never acknowledged to any client.
    pub fn recover_record(&mut self, target_lsn: u64) -> Result<Option<WalRecord>> {
        // Tail expressions, not early returns: exactly one arm compiles per
        // target, so a `return` here is redundant and `-D warnings` rejects it
        // on the wasm build.
        #[cfg(target_arch = "wasm32")]
        {
            let _ = target_lsn;
            Ok(None)
        }

        #[cfg(not(target_arch = "wasm32"))]
        {
            let mut best: Option<(u64, WalRecord)> = None;
            for_each_slot(self, |prefix, record_bytes| {
                let Some(record) = decode_record(record_bytes) else {
                    return;
                };
                if record.header.lsn != target_lsn || record.verify_checksum().is_err() {
                    return;
                }
                if best.as_ref().is_none_or(|(seq, _)| prefix.seq > *seq) {
                    best = Some((prefix.seq, record));
                }
            })?;
            Ok(best.map(|(_, record)| record))
        }
    }
}

/// Highest slot sequence number readable in the ring, or 0 when it holds none.
///
/// The write path resumes above this so a reused sequence number can never let
/// a stale copy tie with the record that replaced it.
pub(super) fn scan_max_seq(dwb: &mut DoubleWriteBuffer) -> Result<u64> {
    #[cfg(not(target_arch = "wasm32"))]
    {
        let mut max = 0u64;
        for_each_slot(dwb, |prefix, _| max = max.max(prefix.seq))?;
        Ok(max)
    }

    #[cfg(target_arch = "wasm32")]
    {
        use std::io::{Read as _, Seek as _, SeekFrom};

        use super::layout::{DWB_CAPACITY, SLOT_PREFIX_SIZE, slot_offset};
        use crate::error::WalError;

        let mut max = 0u64;
        let mut prefix = [0u8; SLOT_PREFIX_SIZE];
        for i in 0..DWB_CAPACITY as u32 {
            if dwb
                .file
                .seek(SeekFrom::Start(slot_offset(i)))
                .map_err(WalError::Io)
                .is_err()
            {
                continue;
            }
            if dwb.file.read_exact(&mut prefix).is_err() {
                continue;
            }
            if let Some(decoded) = SlotPrefix::decode(&prefix) {
                max = max.max(decoded.seq);
            }
        }
        Ok(max)
    }
}

/// Visit every slot that carries a well-formed prefix, handing the callback
/// the prefix and the record bytes (WAL header + payload) it frames.
#[cfg(not(target_arch = "wasm32"))]
fn for_each_slot<F>(dwb: &DoubleWriteBuffer, mut visit: F) -> Result<()>
where
    F: FnMut(&SlotPrefix, &[u8]),
{
    use std::os::unix::io::AsRawFd as _;

    use crate::align::{AlignedBuf, DEFAULT_ALIGNMENT};

    // Under O_DIRECT, reads must also use aligned buffers and aligned
    // lengths. Read one full aligned slot at a time, then parse.
    let mut slot = AlignedBuf::new(DWB_SLOT_STRIDE, DEFAULT_ALIGNMENT)?;

    for i in 0..DWB_CAPACITY as u32 {
        let offset = slot_offset(i);
        // SAFETY: slot.as_mut_ptr is valid for `capacity()` bytes.
        let read = unsafe {
            libc::pread(
                dwb.file.as_raw_fd(),
                slot.as_mut_ptr() as *mut libc::c_void,
                DWB_SLOT_STRIDE,
                offset as libc::off_t,
            )
        };
        if read <= 0 {
            continue;
        }
        // SAFETY: the kernel populated `read` bytes starting at the buffer.
        let bytes: &[u8] = unsafe { std::slice::from_raw_parts(slot.as_ptr(), read as usize) };

        let Some(prefix) = SlotPrefix::decode(bytes) else {
            continue;
        };
        let end = SLOT_PREFIX_SIZE + prefix.total_size;
        if bytes.len() < end {
            continue;
        }
        visit(&prefix, &bytes[SLOT_PREFIX_SIZE..end]);
    }

    Ok(())
}

/// Rebuild a record from the bytes a slot frames. `None` when the header is
/// not a WAL header at all.
#[cfg(not(target_arch = "wasm32"))]
fn decode_record(bytes: &[u8]) -> Option<WalRecord> {
    use crate::record::{HEADER_SIZE, RecordHeader, WAL_MAGIC};

    if bytes.len() < HEADER_SIZE {
        return None;
    }
    let mut header_buf = [0u8; HEADER_SIZE];
    header_buf.copy_from_slice(&bytes[..HEADER_SIZE]);
    let header = RecordHeader::from_bytes(&header_buf);
    if header.magic != WAL_MAGIC {
        return None;
    }
    Some(WalRecord {
        header,
        payload: bytes[HEADER_SIZE..].to_vec(),
    })
}

#[cfg(test)]
mod tests {
    use std::path::Path;

    use super::*;
    use crate::double_write::layout::DWB_CAPACITY;
    use crate::double_write::mode::DwbMode;
    use crate::double_write::status::DwbMirror;
    use crate::record::{RecordType, WalRecordArgs};

    // `recover_record` itself is a no-op on wasm32 — the only arm that compiles
    // there is `Ok(None)`, because slot recovery needs `pread`/O_DIRECT. Every
    // test in this module asserts what recovery returns, so they are all
    // native-only; `scan_max_seq`'s wasm arm is still exercised indirectly by
    // the write path on wasm.

    fn open_buffered(path: &Path) -> DoubleWriteBuffer {
        DoubleWriteBuffer::open(path, DwbMode::Buffered).unwrap()
    }

    fn record(lsn: u64, payload: &[u8]) -> WalRecord {
        WalRecord::new(WalRecordArgs {
            record_type: RecordType::Put as u32,
            lsn,
            tenant_id: 1,
            vshard_id: 0,
            database_id: 0,
            payload: payload.to_vec(),
            encryption_key: None,
            preamble_bytes: None,
        })
        .unwrap()
    }

    fn mirror(dwb: &mut DoubleWriteBuffer, rec: &WalRecord) {
        assert_eq!(dwb.write_record(rec).unwrap(), DwbMirror::Mirrored);
    }

    fn mirror_deferred(dwb: &mut DoubleWriteBuffer, rec: &WalRecord) {
        assert_eq!(dwb.write_record_deferred(rec).unwrap(), DwbMirror::Mirrored);
    }

    #[cfg(not(target_arch = "wasm32"))]
    #[test]
    fn recover_after_wraparound() {
        let dir = crate::test_tempdir().unwrap();
        let mut dwb = open_buffered(&dir.path().join("wrap.dwb"));

        let total = DWB_CAPACITY as u64 + 5;
        for lsn in 1..=total {
            mirror_deferred(&mut dwb, &record(lsn, format!("wrap-{lsn}").as_bytes()));
        }
        dwb.flush().unwrap();

        for lsn in (total - 4)..=total {
            let recovered = dwb.recover_record(lsn).unwrap().expect("recoverable");
            assert_eq!(recovered.payload, format!("wrap-{lsn}").into_bytes());
        }

        for lsn in 1..=5u64 {
            assert!(
                dwb.recover_record(lsn).unwrap().is_none(),
                "LSN {lsn} should have been overwritten by wrap-around"
            );
        }
    }

    /// A crash in the window between the DWB fsync and the WAL fsync leaves an
    /// LSN durable here but missing from the WAL. Recovery resumes at that LSN
    /// and a *different* record is written under it, so two CRC-valid slots
    /// claim it. The one written last is the one the WAL committed.
    #[cfg(not(target_arch = "wasm32"))]
    #[test]
    fn reused_lsn_recovers_the_newer_copy() {
        let dir = crate::test_tempdir().unwrap();
        let mut dwb = open_buffered(&dir.path().join("reuse.dwb"));

        mirror(&mut dwb, &record(9, b"never-acknowledged"));
        mirror(&mut dwb, &record(9, b"after-recovery"));

        let recovered = dwb.recover_record(9).unwrap().expect("recoverable");
        assert_eq!(
            recovered.payload, b"after-recovery",
            "recovery resurrected the unacknowledged copy"
        );
    }

    /// The same reuse, but with the ring wrapping between the two copies, so
    /// the newer one sits at a *lower* slot index than the older one. Pins the
    /// selection to the sequence number: neither ascending nor descending slot
    /// order is correct on its own, and this case is the one a descending scan
    /// would get wrong.
    #[cfg(not(target_arch = "wasm32"))]
    #[test]
    fn reused_lsn_across_a_wrap_recovers_the_newer_copy() {
        let dir = crate::test_tempdir().unwrap();
        let mut dwb = open_buffered(&dir.path().join("reuse_wrap.dwb"));

        // Older copy lands in the last slot of the ring.
        for lsn in 1..DWB_CAPACITY as u64 {
            mirror_deferred(&mut dwb, &record(lsn, b"filler"));
        }
        mirror_deferred(&mut dwb, &record(500, b"never-acknowledged"));
        // The next write wraps to slot 0.
        mirror_deferred(&mut dwb, &record(500, b"after-recovery"));
        dwb.flush().unwrap();

        let recovered = dwb.recover_record(500).unwrap().expect("recoverable");
        assert_eq!(recovered.payload, b"after-recovery");
    }

    /// Reopening must not restart the sequence: a fresh copy written after a
    /// restart still has to outrank the stale one already on disk.
    #[cfg(not(target_arch = "wasm32"))]
    #[test]
    fn reused_lsn_across_a_reopen_recovers_the_newer_copy() {
        let dir = crate::test_tempdir().unwrap();
        let path = dir.path().join("reuse_reopen.dwb");

        {
            let mut dwb = open_buffered(&path);
            mirror(&mut dwb, &record(3, b"never-acknowledged"));
        }

        let mut dwb = open_buffered(&path);
        mirror(&mut dwb, &record(3, b"after-recovery"));

        let recovered = dwb.recover_record(3).unwrap().expect("recoverable");
        assert_eq!(recovered.payload, b"after-recovery");
    }
}
