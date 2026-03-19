use std::io::{Read, Write};
use std::path::Path;

use crate::types::Lsn;

/// Magic bytes identifying a NodeDB segment file.
const SEGMENT_MAGIC: [u8; 4] = *b"SYNS";

/// Current segment format version.
const FORMAT_VERSION: u16 = 1;

/// Footer size in bytes: magic(4) + version(2) + created_by(32) + checksum(4) + min_lsn(8) + max_lsn(8) = 58.
const FOOTER_SIZE: usize = 58;

/// Segment file footer.
///
/// All persistent files embed this footer for crash-safe validation.
/// Footer is written at the end of the file; readers seek to `file_len - FOOTER_SIZE`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SegmentFooter {
    /// Format version for forward compatibility.
    pub format_version: u16,
    /// Identifier of the process/node that created this segment.
    pub created_by: [u8; 32],
    /// CRC32C checksum of the segment data (excluding footer).
    pub checksum: u32,
    /// Minimum LSN of records in this segment.
    pub min_lsn: Lsn,
    /// Maximum LSN of records in this segment.
    pub max_lsn: Lsn,
}

impl SegmentFooter {
    /// Create a new footer for a segment.
    pub fn new(created_by: &str, checksum: u32, min_lsn: Lsn, max_lsn: Lsn) -> Self {
        let mut cb = [0u8; 32];
        let bytes = created_by.as_bytes();
        let len = bytes.len().min(32);
        cb[..len].copy_from_slice(&bytes[..len]);

        Self {
            format_version: FORMAT_VERSION,
            created_by: cb,
            checksum,
            min_lsn,
            max_lsn,
        }
    }

    /// Serialize the footer to bytes.
    pub fn to_bytes(&self) -> [u8; FOOTER_SIZE] {
        let mut buf = [0u8; FOOTER_SIZE];
        buf[0..4].copy_from_slice(&SEGMENT_MAGIC);
        buf[4..6].copy_from_slice(&self.format_version.to_le_bytes());
        buf[6..38].copy_from_slice(&self.created_by);
        buf[38..42].copy_from_slice(&self.checksum.to_le_bytes());
        buf[42..50].copy_from_slice(&self.min_lsn.as_u64().to_le_bytes());
        buf[50..58].copy_from_slice(&self.max_lsn.as_u64().to_le_bytes());
        buf
    }

    /// Deserialize a footer from bytes.
    pub fn from_bytes(buf: &[u8; FOOTER_SIZE]) -> crate::Result<Self> {
        if buf[0..4] != SEGMENT_MAGIC {
            return Err(crate::Error::SegmentCorrupted {
                detail: "invalid segment magic".into(),
            });
        }

        let format_version = u16::from_le_bytes([buf[4], buf[5]]);
        let mut created_by = [0u8; 32];
        created_by.copy_from_slice(&buf[6..38]);
        let checksum = u32::from_le_bytes([buf[38], buf[39], buf[40], buf[41]]);
        let min_lsn = Lsn::new(u64::from_le_bytes(buf[42..50].try_into().unwrap()));
        let max_lsn = Lsn::new(u64::from_le_bytes(buf[50..58].try_into().unwrap()));

        Ok(Self {
            format_version,
            created_by,
            checksum,
            min_lsn,
            max_lsn,
        })
    }

    /// Write the footer to a file.
    pub fn write_to(&self, path: &Path) -> crate::Result<()> {
        let mut file = std::fs::OpenOptions::new().append(true).open(path)?;
        file.write_all(&self.to_bytes())?;
        file.flush()?;
        Ok(())
    }

    /// Read the footer from the end of a file.
    pub fn read_from(path: &Path) -> crate::Result<Self> {
        let mut file = std::fs::File::open(path)?;
        let file_len = file.metadata()?.len() as usize;
        if file_len < FOOTER_SIZE {
            return Err(crate::Error::SegmentCorrupted {
                detail: "file too small for segment footer".into(),
            });
        }

        use std::io::Seek;
        file.seek(std::io::SeekFrom::End(-(FOOTER_SIZE as i64)))?;
        let mut buf = [0u8; FOOTER_SIZE];
        file.read_exact(&mut buf)?;

        Self::from_bytes(&buf)
    }

    /// Footer size in bytes.
    pub const fn size() -> usize {
        FOOTER_SIZE
    }
}

/// Write a segment file with optional encryption.
///
/// Layout: [encrypted_data | footer(58B plaintext)]
/// If `key` is provided, data is AES-256-GCM encrypted with the footer's
/// min_lsn as the nonce (deterministic, unique per segment).
pub fn write_encrypted_segment(
    path: &Path,
    data: &[u8],
    footer: &SegmentFooter,
    key: Option<&nodedb_wal::crypto::WalEncryptionKey>,
) -> crate::Result<()> {
    let final_data = if let Some(key) = key {
        let mut aad = [0u8; nodedb_wal::record::HEADER_SIZE];
        aad[..4].copy_from_slice(b"SEGM");
        key.encrypt(footer.min_lsn.as_u64(), &aad, data)
            .map_err(|e| crate::Error::Storage {
                engine: "segment".into(),
                detail: format!("segment encryption failed: {e}"),
            })?
    } else {
        data.to_vec()
    };

    let mut file = std::fs::File::create(path)?;
    file.write_all(&final_data)?;
    file.write_all(&footer.to_bytes())?;
    file.flush()?;
    Ok(())
}

/// Read and decrypt a segment file's data portion.
///
/// Returns the plaintext data (footer is stripped and validated separately).
pub fn read_encrypted_segment(
    path: &Path,
    key: Option<&nodedb_wal::crypto::WalEncryptionKey>,
) -> crate::Result<Vec<u8>> {
    let raw = std::fs::read(path)?;
    if raw.len() < FOOTER_SIZE {
        return Err(crate::Error::SegmentCorrupted {
            detail: "file too small".into(),
        });
    }

    let data = &raw[..raw.len() - FOOTER_SIZE];
    let footer_bytes: [u8; FOOTER_SIZE] =
        raw[raw.len() - FOOTER_SIZE..]
            .try_into()
            .map_err(|_| crate::Error::SegmentCorrupted {
                detail: "footer size mismatch".into(),
            })?;

    let footer = SegmentFooter::from_bytes(&footer_bytes)?;

    if let Some(key) = key {
        let mut aad = [0u8; nodedb_wal::record::HEADER_SIZE];
        aad[..4].copy_from_slice(b"SEGM");
        key.decrypt(footer.min_lsn.as_u64(), &aad, data)
            .map_err(|e| crate::Error::Storage {
                engine: "segment".into(),
                detail: format!("segment decryption failed: {e}"),
            })
    } else {
        Ok(data.to_vec())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn roundtrip_bytes() {
        let footer = SegmentFooter::new("node-1", 0xDEADBEEF, Lsn::new(10), Lsn::new(99));
        let bytes = footer.to_bytes();
        let parsed = SegmentFooter::from_bytes(&bytes).unwrap();
        assert_eq!(footer, parsed);
    }

    #[test]
    fn invalid_magic_rejected() {
        let mut bytes = [0u8; FOOTER_SIZE];
        bytes[0..4].copy_from_slice(b"NOPE");
        assert!(SegmentFooter::from_bytes(&bytes).is_err());
    }

    #[test]
    fn write_and_read_file() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("test.seg");

        // Write some data + footer.
        std::fs::write(&path, b"segment data here").unwrap();
        let footer = SegmentFooter::new("test", 42, Lsn::new(1), Lsn::new(50));
        footer.write_to(&path).unwrap();

        // Read back.
        let read_footer = SegmentFooter::read_from(&path).unwrap();
        assert_eq!(read_footer.checksum, 42);
        assert_eq!(read_footer.min_lsn, Lsn::new(1));
        assert_eq!(read_footer.max_lsn, Lsn::new(50));
    }

    #[test]
    fn created_by_truncates_long_names() {
        let footer = SegmentFooter::new(
            "this-is-a-very-long-node-name-that-exceeds-32-bytes",
            0,
            Lsn::ZERO,
            Lsn::ZERO,
        );
        // Should not panic, truncates to 32 bytes.
        let bytes = footer.to_bytes();
        let parsed = SegmentFooter::from_bytes(&bytes).unwrap();
        assert_eq!(parsed.created_by[..4], *b"this");
    }

    #[test]
    fn lsn_ordering_preserved() {
        let footer = SegmentFooter::new("n", 0, Lsn::new(100), Lsn::new(200));
        assert!(footer.min_lsn < footer.max_lsn);
    }
}
