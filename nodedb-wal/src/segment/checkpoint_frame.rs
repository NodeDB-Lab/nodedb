// SPDX-License-Identifier: Apache-2.0

//! CRC-framed checkpoint envelope.
//!
//! Plaintext checkpoint files written via [`crate::segment::atomic_write_fsync`]
//! carry no integrity check of their own — silent bit-rot or a torn write past
//! the atomic-rename boundary is invisible until deserialization fails (or,
//! worse, succeeds on corrupted data). This module wraps the payload in a
//! fixed 17-byte header carrying a CRC32C so corruption is caught on read.
//!
//! Frame layout:
//!
//! ```text
//! [ magic "NCKF" (4) ] [ version: u8 (1) ] [ crc32c: u32 LE (4) ] [ payload_len: u64 LE (8) ] [ payload... ]
//! ```
//!
//! [`write_checkpoint_framed`] / [`read_checkpoint_framed`] are drop-in
//! replacements for [`crate::segment::atomic_write_fsync`] /
//! [`crate::segment::read_checkpoint_dontneed`] at checkpoint call sites.

use std::path::Path;

use crate::error::{Result, WalError};
use crate::segment::atomic_io::{atomic_write_fsync, read_checkpoint_dontneed};

const MAGIC: [u8; 4] = *b"NCKF";
const VERSION: u8 = 1;
const HEADER_LEN: usize = 17;

/// Frame `payload` with a magic + version + CRC32C + length header and
/// atomically write it to `dir/name` (see [`atomic_write_fsync`], which owns
/// the name check and the staging-file naming).
pub fn write_checkpoint_framed(dir: &Path, name: &str, payload: &[u8]) -> Result<()> {
    let crc = crc32c::crc32c(payload);
    let payload_len = payload.len() as u64;

    let mut framed = Vec::with_capacity(HEADER_LEN + payload.len());
    framed.extend_from_slice(&MAGIC);
    framed.push(VERSION);
    framed.extend_from_slice(&crc.to_le_bytes());
    framed.extend_from_slice(&payload_len.to_le_bytes());
    framed.extend_from_slice(payload);

    atomic_write_fsync(dir, name, &framed)
}

/// Read a checkpoint file written by [`write_checkpoint_framed`], verify its
/// CRC32C, and return the unframed payload.
///
/// Transitional migration path: files shorter than the 17-byte header, or
/// whose first 4 bytes are not the `NCKF` magic, are treated as pre-framing
/// legacy checkpoints and returned unchanged (best-effort, no CRC). This
/// branch exists only to bridge the migration — once every checkpoint on
/// disk is known-framed (each self-heals to framed on its next write), it
/// should be removed.
pub fn read_checkpoint_framed(path: &Path) -> Result<Vec<u8>> {
    let bytes = read_checkpoint_dontneed(path)?;

    if bytes.len() < HEADER_LEN || bytes[0..4] != MAGIC {
        // Legacy transitional path — see doc comment above.
        return Ok(bytes);
    }

    // Safe: length-checked >= HEADER_LEN (17) above, so all fixed offsets
    // up to 17 are in-bounds.
    let version = bytes[4];
    if version != VERSION {
        return Err(WalError::CheckpointCorrupt {
            path: path.display().to_string(),
            detail: format!("unsupported checkpoint frame version {version}"),
        });
    }

    let stored_crc = u32::from_le_bytes([bytes[5], bytes[6], bytes[7], bytes[8]]);
    let payload_len = u64::from_le_bytes([
        bytes[9], bytes[10], bytes[11], bytes[12], bytes[13], bytes[14], bytes[15], bytes[16],
    ]);

    let payload = &bytes[HEADER_LEN..];
    let actual_len = payload.len() as u64;
    if actual_len != payload_len {
        return Err(WalError::CheckpointCorrupt {
            path: path.display().to_string(),
            detail: format!("truncated: header len {payload_len} != actual {actual_len}"),
        });
    }

    let computed = crc32c::crc32c(payload);
    if computed != stored_crc {
        return Err(WalError::CheckpointCorrupt {
            path: path.display().to_string(),
            detail: format!("crc mismatch: header {stored_crc:#x} != computed {computed:#x}"),
        });
    }

    Ok(payload.to_vec())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;

    #[test]
    fn roundtrip() {
        let dir = crate::test_tempdir().unwrap();
        let dst = dir.path().join("payload.ckpt");

        write_checkpoint_framed(dir.path(), "payload.ckpt", b"hello framed world").unwrap();
        let read_back = read_checkpoint_framed(&dst).unwrap();
        assert_eq!(read_back, b"hello framed world");
    }

    #[test]
    fn corruption_detected() {
        let dir = crate::test_tempdir().unwrap();
        let dst = dir.path().join("payload.ckpt");

        write_checkpoint_framed(dir.path(), "payload.ckpt", b"integrity matters").unwrap();

        // Flip a byte in the payload region (after the 17-byte header).
        let mut bytes = fs::read(&dst).unwrap();
        let flip_idx = HEADER_LEN + 2;
        bytes[flip_idx] ^= 0xFF;
        fs::write(&dst, &bytes).unwrap();

        let err = read_checkpoint_framed(&dst).unwrap_err();
        match err {
            WalError::CheckpointCorrupt { detail, .. } => {
                assert!(detail.contains("crc mismatch"), "detail was: {detail}");
            }
            other => panic!("expected CheckpointCorrupt, got {other:?}"),
        }
    }

    #[test]
    fn truncation_detected() {
        let dir = crate::test_tempdir().unwrap();
        let dst = dir.path().join("payload.ckpt");

        write_checkpoint_framed(dir.path(), "payload.ckpt", b"a longer payload body here").unwrap();

        let mut bytes = fs::read(&dst).unwrap();
        bytes.truncate(bytes.len() - 5);
        fs::write(&dst, &bytes).unwrap();

        let err = read_checkpoint_framed(&dst).unwrap_err();
        match err {
            WalError::CheckpointCorrupt { detail, .. } => {
                assert!(detail.contains("truncated"), "detail was: {detail}");
            }
            other => panic!("expected CheckpointCorrupt, got {other:?}"),
        }
    }

    #[test]
    fn legacy_unframed_accepted() {
        let dir = crate::test_tempdir().unwrap();
        let dst = dir.path().join("payload.ckpt");

        // Raw pre-framing bytes: first 4 bytes are NOT the NCKF magic.
        atomic_write_fsync(dir.path(), "payload.ckpt", b"legacy plaintext checkpoint").unwrap();

        let read_back = read_checkpoint_framed(&dst).unwrap();
        assert_eq!(read_back, b"legacy plaintext checkpoint");
    }
}
