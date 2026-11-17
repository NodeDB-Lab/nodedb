// SPDX-License-Identifier: Apache-2.0

//! CRC-framed checkpoint envelope that carries the file's own key.
//!
//! A checkpoint named by a hash of its key needs the full key inside the file:
//! a loader reads the key back instead of parsing the name, and a reclaim scan
//! matches files by key without decoding their payloads. The key sits in the
//! header with its own CRC32C, so [`read_checkpoint_key`] reads and checks the
//! key without reading the payload.
//!
//! Frame layout:
//!
//! ```text
//! [ magic "NCKK" (4) ] [ version: u8 (1) ] [ key_len: u32 LE (4) ] [ key_crc32c: u32 LE (4) ]
//! [ payload_crc32c: u32 LE (4) ] [ payload_len: u64 LE (8) ] [ key... ] [ payload... ]
//! ```

use std::io::Read;
use std::path::Path;

use crate::error::{Result, WalError};
use crate::segment::atomic_io::{atomic_write_fsync, read_checkpoint_dontneed};

const MAGIC: [u8; 4] = *b"NCKK";
const VERSION: u8 = 1;
const HEADER_LEN: usize = 25;

/// Largest key a keyed checkpoint frame accepts, on write and on read.
pub const MAX_CHECKPOINT_KEY_LEN: usize = 64 * 1024;

/// A keyed checkpoint file read back whole.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KeyedCheckpoint {
    /// The key bytes stored in the header.
    pub key: Vec<u8>,
    /// The unframed payload.
    pub payload: Vec<u8>,
}

/// Parsed fixed header.
struct Header {
    key_len: usize,
    key_crc: u32,
    payload_crc: u32,
    payload_len: u64,
}

fn corrupt(path: &Path, detail: String) -> WalError {
    WalError::CheckpointCorrupt {
        path: path.display().to_string(),
        detail,
    }
}

/// Frame `key` and `payload` and atomically write them to `dir/name` (see
/// [`atomic_write_fsync`], which owns the name check and the staging file).
pub fn write_checkpoint_keyed(dir: &Path, name: &str, key: &[u8], payload: &[u8]) -> Result<()> {
    if key.len() > MAX_CHECKPOINT_KEY_LEN {
        return Err(corrupt(
            &dir.join(name),
            format!(
                "checkpoint key is {} bytes, limit {MAX_CHECKPOINT_KEY_LEN}",
                key.len()
            ),
        ));
    }
    let key_len = key.len() as u32;
    let payload_len = payload.len() as u64;

    let mut framed = Vec::with_capacity(HEADER_LEN + key.len() + payload.len());
    framed.extend_from_slice(&MAGIC);
    framed.push(VERSION);
    framed.extend_from_slice(&key_len.to_le_bytes());
    framed.extend_from_slice(&crc32c::crc32c(key).to_le_bytes());
    framed.extend_from_slice(&crc32c::crc32c(payload).to_le_bytes());
    framed.extend_from_slice(&payload_len.to_le_bytes());
    framed.extend_from_slice(key);
    framed.extend_from_slice(payload);

    atomic_write_fsync(dir, name, &framed)
}

fn parse_header(path: &Path, bytes: &[u8]) -> Result<Header> {
    if bytes.len() < HEADER_LEN {
        return Err(corrupt(
            path,
            format!(
                "too short for a keyed checkpoint frame: {} bytes, need {HEADER_LEN}",
                bytes.len()
            ),
        ));
    }
    if bytes[0..4] != MAGIC {
        return Err(corrupt(path, "missing keyed checkpoint frame magic".into()));
    }
    let version = bytes[4];
    if version != VERSION {
        return Err(corrupt(
            path,
            format!("unsupported keyed checkpoint frame version {version}"),
        ));
    }
    let u32_at =
        |at: usize| u32::from_le_bytes([bytes[at], bytes[at + 1], bytes[at + 2], bytes[at + 3]]);
    let key_len = u32_at(5) as usize;
    if key_len > MAX_CHECKPOINT_KEY_LEN {
        return Err(corrupt(
            path,
            format!("key length {key_len} exceeds limit {MAX_CHECKPOINT_KEY_LEN}"),
        ));
    }
    let mut len_bytes = [0u8; 8];
    len_bytes.copy_from_slice(&bytes[17..25]);
    Ok(Header {
        key_len,
        key_crc: u32_at(9),
        payload_crc: u32_at(13),
        payload_len: u64::from_le_bytes(len_bytes),
    })
}

fn check_key(path: &Path, header: &Header, key: &[u8]) -> Result<()> {
    let computed = crc32c::crc32c(key);
    if computed != header.key_crc {
        return Err(corrupt(
            path,
            format!(
                "key crc mismatch: header {:#x} != computed {computed:#x}",
                header.key_crc
            ),
        ));
    }
    Ok(())
}

/// Read a file written by [`write_checkpoint_keyed`], check both CRCs, and
/// return its key and payload.
pub fn read_checkpoint_keyed(path: &Path) -> Result<KeyedCheckpoint> {
    let bytes = read_checkpoint_dontneed(path)?;
    let header = parse_header(path, &bytes)?;
    let body = &bytes[HEADER_LEN..];
    if body.len() < header.key_len {
        return Err(corrupt(
            path,
            format!(
                "truncated: key len {} exceeds {} body bytes",
                header.key_len,
                body.len()
            ),
        ));
    }
    let (key, payload) = body.split_at(header.key_len);
    check_key(path, &header, key)?;

    let actual_len = payload.len() as u64;
    if actual_len != header.payload_len {
        return Err(corrupt(
            path,
            format!(
                "truncated: header len {} != actual {actual_len}",
                header.payload_len
            ),
        ));
    }
    let computed = crc32c::crc32c(payload);
    if computed != header.payload_crc {
        return Err(corrupt(
            path,
            format!(
                "crc mismatch: header {:#x} != computed {computed:#x}",
                header.payload_crc
            ),
        ));
    }
    Ok(KeyedCheckpoint {
        key: key.to_vec(),
        payload: payload.to_vec(),
    })
}

/// Read and check only the key of a file written by
/// [`write_checkpoint_keyed`]. The payload is neither read nor checked.
pub fn read_checkpoint_key(path: &Path) -> Result<Vec<u8>> {
    let mut file = std::fs::File::open(path)?;
    let mut head = [0u8; HEADER_LEN];
    let mut filled = 0usize;
    while filled < HEADER_LEN {
        let n = file.read(&mut head[filled..])?;
        if n == 0 {
            break;
        }
        filled += n;
    }
    let header = parse_header(path, &head[..filled])?;
    let mut key = vec![0u8; header.key_len];
    file.read_exact(&mut key).map_err(|e| {
        if e.kind() == std::io::ErrorKind::UnexpectedEof {
            corrupt(path, format!("truncated: key len {}", header.key_len))
        } else {
            WalError::Io(e)
        }
    })?;
    check_key(path, &header, &key)?;
    Ok(key)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;

    fn write(dir: &Path, key: &[u8], payload: &[u8]) -> std::path::PathBuf {
        write_checkpoint_keyed(dir, "a.ckpt", key, payload).unwrap();
        dir.join("a.ckpt")
    }

    #[test]
    fn roundtrip() {
        let dir = tempfile::tempdir().unwrap();
        let path = write(dir.path(), b"the key", b"the payload");
        let read = read_checkpoint_keyed(&path).unwrap();
        assert_eq!(read.key, b"the key");
        assert_eq!(read.payload, b"the payload");
        assert_eq!(read_checkpoint_key(&path).unwrap(), b"the key");
    }

    #[test]
    fn empty_key_and_payload_roundtrip() {
        let dir = tempfile::tempdir().unwrap();
        let path = write(dir.path(), b"", b"");
        let read = read_checkpoint_keyed(&path).unwrap();
        assert!(read.key.is_empty() && read.payload.is_empty());
    }

    #[test]
    fn key_corruption_detected_by_both_readers() {
        let dir = tempfile::tempdir().unwrap();
        let path = write(dir.path(), b"collection", b"payload");
        let mut bytes = fs::read(&path).unwrap();
        bytes[HEADER_LEN] ^= 0xFF;
        fs::write(&path, &bytes).unwrap();
        for err in [
            read_checkpoint_keyed(&path).unwrap_err(),
            read_checkpoint_key(&path).unwrap_err(),
        ] {
            match err {
                WalError::CheckpointCorrupt { detail, .. } => {
                    assert!(detail.contains("key crc mismatch"), "detail was: {detail}");
                }
                other => panic!("expected CheckpointCorrupt, got {other:?}"),
            }
        }
    }

    #[test]
    fn payload_corruption_detected() {
        let dir = tempfile::tempdir().unwrap();
        let path = write(dir.path(), b"k", b"payload bytes");
        let mut bytes = fs::read(&path).unwrap();
        let last = bytes.len() - 1;
        bytes[last] ^= 0xFF;
        fs::write(&path, &bytes).unwrap();
        let err = read_checkpoint_keyed(&path).unwrap_err();
        assert!(
            matches!(err, WalError::CheckpointCorrupt { detail, .. } if detail.contains("crc mismatch"))
        );
        // The key alone is still intact.
        assert_eq!(read_checkpoint_key(&path).unwrap(), b"k");
    }

    #[test]
    fn truncation_detected() {
        let dir = tempfile::tempdir().unwrap();
        let path = write(dir.path(), b"key", b"a longer payload body");
        let mut bytes = fs::read(&path).unwrap();
        bytes.truncate(bytes.len() - 3);
        fs::write(&path, &bytes).unwrap();
        let err = read_checkpoint_keyed(&path).unwrap_err();
        assert!(
            matches!(err, WalError::CheckpointCorrupt { detail, .. } if detail.contains("truncated"))
        );

        bytes.truncate(HEADER_LEN + 1);
        fs::write(&path, &bytes).unwrap();
        let err = read_checkpoint_key(&path).unwrap_err();
        assert!(
            matches!(err, WalError::CheckpointCorrupt { detail, .. } if detail.contains("truncated"))
        );
    }

    #[test]
    fn unkeyed_frame_refused() {
        let dir = tempfile::tempdir().unwrap();
        crate::segment::write_checkpoint_framed(dir.path(), "a.ckpt", b"plain").unwrap();
        let path = dir.path().join("a.ckpt");
        assert!(matches!(
            read_checkpoint_keyed(&path).unwrap_err(),
            WalError::CheckpointCorrupt { .. }
        ));
        assert!(matches!(
            read_checkpoint_key(&path).unwrap_err(),
            WalError::CheckpointCorrupt { .. }
        ));
    }

    #[test]
    fn oversized_key_refused_on_write() {
        let dir = tempfile::tempdir().unwrap();
        let key = vec![b'k'; MAX_CHECKPOINT_KEY_LEN + 1];
        assert!(write_checkpoint_keyed(dir.path(), "a.ckpt", &key, b"").is_err());
        assert!(!dir.path().join("a.ckpt").exists());
    }
}
