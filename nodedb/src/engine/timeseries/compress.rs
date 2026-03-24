//! Zstd dictionary compression for structured log ingestion.
//!
//! NodeDB supports pre-trained static dictionaries for common logging
//! formats (NGINX, JSON structured logs). This yields 3-5x compression
//! advantage during high-throughput ingest while avoiding the distributed
//! synchronization overhead of dynamic dictionaries.

use std::collections::HashMap;
use std::sync::Arc;

/// Pre-trained compression dictionary.
#[derive(Debug, Clone)]
pub struct LogDictionary {
    /// Human-readable name (e.g., "nginx", "json-structured").
    pub name: String,
    /// Raw dictionary bytes (trained via `zstd --train`).
    pub data: Arc<[u8]>,
    /// Dictionary ID for envelope tagging.
    pub id: u32,
}

/// Registry of pre-trained log dictionaries.
///
/// Each dictionary is identified by a u32 ID embedded in the compressed
/// block header. Decompressors look up the dictionary by ID.
#[derive(Debug, Clone)]
pub struct DictionaryRegistry {
    by_id: HashMap<u32, LogDictionary>,
    by_name: HashMap<String, u32>,
}

impl DictionaryRegistry {
    pub fn new() -> Self {
        Self {
            by_id: HashMap::new(),
            by_name: HashMap::new(),
        }
    }

    /// Register a dictionary. Returns error if ID is already taken.
    pub fn register(&mut self, dict: LogDictionary) -> crate::Result<()> {
        if self.by_id.contains_key(&dict.id) {
            return Err(crate::Error::BadRequest {
                detail: format!("dictionary ID {} already registered", dict.id),
            });
        }
        self.by_name.insert(dict.name.clone(), dict.id);
        self.by_id.insert(dict.id, dict);
        Ok(())
    }

    pub fn get_by_id(&self, id: u32) -> Option<&LogDictionary> {
        self.by_id.get(&id)
    }

    pub fn get_by_name(&self, name: &str) -> Option<&LogDictionary> {
        self.by_name.get(name).and_then(|id| self.by_id.get(id))
    }

    pub fn len(&self) -> usize {
        self.by_id.len()
    }

    pub fn is_empty(&self) -> bool {
        self.by_id.is_empty()
    }
}

impl Default for DictionaryRegistry {
    fn default() -> Self {
        Self::new()
    }
}

/// Compressed log block header.
///
/// ```text
/// ┌──────────┬────────────┬───────────────┬─────────────────┐
/// │ magic(2) │ dict_id(4) │ raw_len(4)    │ compressed data │
/// └──────────┴────────────┴───────────────┴─────────────────┘
/// ```
const BLOCK_MAGIC: [u8; 2] = *b"ZL";
const BLOCK_HEADER_SIZE: usize = 10;

/// Compress a log entry using a dictionary.
///
/// Returns `BLOCK_HEADER + zstd_compressed_data`.
/// If no dictionary is provided, uses plain zstd compression.
pub fn compress_log(data: &[u8], dict: Option<&LogDictionary>, level: i32) -> Vec<u8> {
    let compressed = match dict {
        Some(d) => zstd_compress_with_dict(data, &d.data, level),
        None => zstd_compress(data, level),
    };

    let dict_id = dict.map_or(0, |d| d.id);
    let mut buf = Vec::with_capacity(BLOCK_HEADER_SIZE + compressed.len());
    buf.extend_from_slice(&BLOCK_MAGIC);
    buf.extend_from_slice(&dict_id.to_le_bytes());
    buf.extend_from_slice(&(data.len() as u32).to_le_bytes());
    buf.extend_from_slice(&compressed);
    buf
}

/// Decompress a log block.
///
/// Looks up the dictionary by ID from the registry.
pub fn decompress_log(
    block: &[u8],
    registry: &DictionaryRegistry,
) -> Result<Vec<u8>, LogCompressError> {
    if block.len() < BLOCK_HEADER_SIZE {
        return Err(LogCompressError::BlockTooSmall);
    }
    if block[0..2] != BLOCK_MAGIC {
        return Err(LogCompressError::InvalidMagic);
    }

    let dict_id = u32::from_le_bytes(
        block[2..6]
            .try_into()
            .map_err(|_| LogCompressError::BlockTooSmall)?,
    );
    let raw_len = u32::from_le_bytes(
        block[6..10]
            .try_into()
            .map_err(|_| LogCompressError::BlockTooSmall)?,
    ) as usize;
    let compressed = &block[BLOCK_HEADER_SIZE..];

    let decompressed = if dict_id == 0 {
        zstd_decompress(compressed, raw_len)?
    } else {
        let dict = registry
            .get_by_id(dict_id)
            .ok_or(LogCompressError::DictionaryNotFound { id: dict_id })?;
        zstd_decompress_with_dict(compressed, &dict.data, raw_len)?
    };

    if decompressed.len() != raw_len {
        return Err(LogCompressError::SizeMismatch {
            expected: raw_len,
            actual: decompressed.len(),
        });
    }

    Ok(decompressed)
}

/// Errors from log compression/decompression.
#[derive(Debug, thiserror::Error)]
pub enum LogCompressError {
    #[error("compressed block too small")]
    BlockTooSmall,
    #[error("invalid block magic")]
    InvalidMagic,
    #[error("dictionary {id} not found in registry")]
    DictionaryNotFound { id: u32 },
    #[error("decompressed size mismatch: expected {expected}, got {actual}")]
    SizeMismatch { expected: usize, actual: usize },
    #[error("zstd error: {0}")]
    Zstd(String),
}

// --- Zstd wrappers ---
// These use the `zstd` crate's streaming API.

fn zstd_compress(data: &[u8], level: i32) -> Vec<u8> {
    zstd::bulk::compress(data, level).unwrap_or_else(|_| data.to_vec())
}

fn zstd_compress_with_dict(data: &[u8], dict: &[u8], level: i32) -> Vec<u8> {
    let dict = zstd::dict::EncoderDictionary::copy(dict, level);
    let mut out = Vec::new();
    let Ok(mut encoder) = zstd::stream::Encoder::with_prepared_dictionary(&mut out, &dict) else {
        return data.to_vec();
    };
    if std::io::Write::write_all(&mut encoder, data).is_err() {
        return data.to_vec();
    }
    if encoder.finish().is_err() {
        return data.to_vec();
    }
    out
}

fn zstd_decompress(data: &[u8], capacity: usize) -> Result<Vec<u8>, LogCompressError> {
    zstd::bulk::decompress(data, capacity).map_err(|e| LogCompressError::Zstd(e.to_string()))
}

fn zstd_decompress_with_dict(
    data: &[u8],
    dict: &[u8],
    capacity: usize,
) -> Result<Vec<u8>, LogCompressError> {
    let dict = zstd::dict::DecoderDictionary::copy(dict);
    let mut decoder =
        zstd::stream::Decoder::with_prepared_dictionary(std::io::Cursor::new(data), &dict)
            .map_err(|e| LogCompressError::Zstd(e.to_string()))?;
    let mut out = Vec::with_capacity(capacity);
    std::io::Read::read_to_end(&mut decoder, &mut out)
        .map_err(|e| LogCompressError::Zstd(e.to_string()))?;
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn registry_lifecycle() {
        let mut reg = DictionaryRegistry::new();
        assert!(reg.is_empty());

        let dict = LogDictionary {
            name: "nginx".into(),
            data: Arc::from(vec![0u8; 64].as_slice()),
            id: 1,
        };
        reg.register(dict).unwrap();
        assert_eq!(reg.len(), 1);
        assert!(reg.get_by_id(1).is_some());
        assert!(reg.get_by_name("nginx").is_some());
        assert!(reg.get_by_name("unknown").is_none());
    }

    #[test]
    fn duplicate_id_rejected() {
        let mut reg = DictionaryRegistry::new();
        let dict1 = LogDictionary {
            name: "a".into(),
            data: Arc::from(vec![0u8; 32].as_slice()),
            id: 1,
        };
        let dict2 = LogDictionary {
            name: "b".into(),
            data: Arc::from(vec![0u8; 32].as_slice()),
            id: 1,
        };
        reg.register(dict1).unwrap();
        assert!(reg.register(dict2).is_err());
    }

    #[test]
    fn compress_decompress_no_dict() {
        let reg = DictionaryRegistry::new();
        let data = b"2024-01-15 12:00:00 INFO request completed in 42ms";
        let block = compress_log(data, None, 3);
        let decompressed = decompress_log(&block, &reg).unwrap();
        assert_eq!(&decompressed, data);
    }

    #[test]
    fn compress_decompress_with_dict() {
        // Train a trivial dictionary from repeated log patterns.
        let training_data: Vec<u8> = (0..100)
            .flat_map(|i| format!("2024-01-15 12:{i:02}:00 INFO request completed\n").into_bytes())
            .collect();
        // Use training data as "dictionary" — in production this would be zstd-trained.
        let dict = LogDictionary {
            name: "test".into(),
            data: Arc::from(training_data.as_slice()),
            id: 42,
        };

        let mut reg = DictionaryRegistry::new();
        reg.register(dict.clone()).unwrap();

        let log_line = b"2024-01-15 12:30:00 INFO request completed";
        let block = compress_log(log_line, Some(&dict), 3);
        let decompressed = decompress_log(&block, &reg).unwrap();
        assert_eq!(&decompressed, log_line);
    }

    #[test]
    fn repeated_logs_compress_well() {
        let log_lines: Vec<u8> = (0..1000)
            .flat_map(|i| {
                format!(
                    "{{\"ts\":\"2024-01-15T12:{:02}:{:02}Z\",\"level\":\"INFO\",\"msg\":\"request\",\"latency_ms\":{}}}\n",
                    i / 60 % 60,
                    i % 60,
                    i % 100
                )
                .into_bytes()
            })
            .collect();

        let block = compress_log(&log_lines, None, 3);
        let ratio = log_lines.len() as f64 / (block.len() - BLOCK_HEADER_SIZE) as f64;
        assert!(
            ratio > 3.0,
            "expected >3x compression for repetitive logs, got {ratio:.1}x"
        );
    }

    #[test]
    fn invalid_block_errors() {
        let reg = DictionaryRegistry::new();

        // Too small.
        assert!(matches!(
            decompress_log(&[0; 5], &reg),
            Err(LogCompressError::BlockTooSmall)
        ));

        // Invalid magic.
        let mut block = [0u8; 10];
        block[0..2].copy_from_slice(b"XX");
        assert!(matches!(
            decompress_log(&block, &reg),
            Err(LogCompressError::InvalidMagic)
        ));
    }

    #[test]
    fn missing_dictionary_error() {
        let reg = DictionaryRegistry::new();
        // Block claiming dict_id=99 which doesn't exist.
        let mut block = Vec::new();
        block.extend_from_slice(&BLOCK_MAGIC);
        block.extend_from_slice(&99u32.to_le_bytes());
        block.extend_from_slice(&0u32.to_le_bytes());
        assert!(matches!(
            decompress_log(&block, &reg),
            Err(LogCompressError::DictionaryNotFound { id: 99 })
        ));
    }
}
