// SPDX-License-Identifier: BUSL-1.1

//! The full key of one per-collection checkpoint file, its byte encoding, and
//! the fixed-length file name derived from it.
//!
//! A file name built from a user name has no length bound, and a name past the
//! filesystem limit fails the write. The name is therefore a hash of the key:
//! the first 128 bits of SHA-256 over the encoded key, hex-encoded. The key
//! itself is stored in the file, so a loader never parses names back into keys.

/// Extension every keyed checkpoint file carries.
pub(crate) const KEYED_CKPT_EXTENSION: &str = "ckpt";

/// Which engine a checkpoint key belongs to. Part of the hashed key, so two
/// engines never share a file name for the same collection.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub(crate) enum CheckpointEngine {
    Vector,
    SparseVector,
    Spatial,
    Crdt,
    Columnar,
    Kv,
}

impl CheckpointEngine {
    fn tag(self) -> u8 {
        match self {
            Self::Vector => 1,
            Self::SparseVector => 2,
            Self::Spatial => 3,
            Self::Crdt => 4,
            Self::Columnar => 5,
            Self::Kv => 6,
        }
    }

    fn from_tag(tag: u8) -> Option<Self> {
        Some(match tag {
            1 => Self::Vector,
            2 => Self::SparseVector,
            3 => Self::Spatial,
            4 => Self::Crdt,
            5 => Self::Columnar,
            6 => Self::Kv,
            _ => return None,
        })
    }

    /// Lower-case engine name for error text.
    pub(crate) fn name(self) -> &'static str {
        match self {
            Self::Vector => "vector",
            Self::SparseVector => "sparse-vector",
            Self::Spatial => "spatial",
            Self::Crdt => "crdt",
            Self::Columnar => "columnar",
            Self::Kv => "kv",
        }
    }
}

/// The full key of one checkpoint file: engine, database, tenant, and the
/// engine's name parts (collection, then field where the engine has one).
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub(crate) struct CheckpointKey {
    pub engine: CheckpointEngine,
    pub database_id: u64,
    pub tenant_id: u64,
    pub parts: Vec<String>,
}

impl std::fmt::Display for CheckpointKey {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{} db {} tenant {} {:?}",
            self.engine.name(),
            self.database_id,
            self.tenant_id,
            self.parts
        )
    }
}

impl CheckpointKey {
    pub(crate) fn new(
        engine: CheckpointEngine,
        database_id: u64,
        tenant_id: u64,
        parts: Vec<String>,
    ) -> Self {
        Self {
            engine,
            database_id,
            tenant_id,
            parts,
        }
    }

    /// Deterministic byte encoding:
    /// `[tag u8][db u64 LE][tenant u64 LE][count u32 LE]` then per part
    /// `[len u32 LE][utf8 bytes]`.
    pub(crate) fn encode(&self) -> Vec<u8> {
        let mut out =
            Vec::with_capacity(21 + self.parts.iter().map(|p| 4 + p.len()).sum::<usize>());
        out.push(self.engine.tag());
        out.extend_from_slice(&self.database_id.to_le_bytes());
        out.extend_from_slice(&self.tenant_id.to_le_bytes());
        out.extend_from_slice(&(self.parts.len() as u32).to_le_bytes());
        for part in &self.parts {
            out.extend_from_slice(&(part.len() as u32).to_le_bytes());
            out.extend_from_slice(part.as_bytes());
        }
        out
    }

    /// Inverse of [`Self::encode`]. Trailing bytes are an error.
    pub(crate) fn decode(bytes: &[u8]) -> Result<Self, String> {
        let mut cursor = Cursor { rest: bytes };
        let tag = cursor.take(1, "engine tag")?[0];
        let engine =
            CheckpointEngine::from_tag(tag).ok_or_else(|| format!("unknown engine tag {tag}"))?;
        let database_id = u64_le(cursor.take(8, "database id")?);
        let tenant_id = u64_le(cursor.take(8, "tenant id")?);
        let count = u32_le(cursor.take(4, "part count")?) as usize;
        let mut parts = Vec::new();
        for index in 0..count {
            let len = u32_le(cursor.take(4, "part length")?) as usize;
            let raw = cursor.take(len, "part bytes")?;
            let part = std::str::from_utf8(raw)
                .map_err(|e| format!("key part {index} is not utf-8: {e}"))?;
            parts.push(part.to_string());
        }
        if !cursor.rest.is_empty() {
            return Err(format!("{} trailing bytes after key", cursor.rest.len()));
        }
        Ok(Self {
            engine,
            database_id,
            tenant_id,
            parts,
        })
    }

    /// The fixed-length file stem for this key: 32 hex digits, the first 128
    /// bits of SHA-256 over [`Self::encode`].
    pub(crate) fn file_stem(&self) -> String {
        nodedb_types::hashed_file_stem(&self.encode())
    }

    /// The checkpoint file name for this key: [`Self::file_stem`] plus `.ckpt`.
    pub(crate) fn file_name(&self) -> String {
        format!("{}.{KEYED_CKPT_EXTENSION}", self.file_stem())
    }
}

/// Reads fixed-size fields off the front of an encoded key.
struct Cursor<'a> {
    rest: &'a [u8],
}

impl<'a> Cursor<'a> {
    fn take(&mut self, n: usize, what: &str) -> Result<&'a [u8], String> {
        if self.rest.len() < n {
            return Err(format!("key truncated in {what}"));
        }
        let (head, tail) = self.rest.split_at(n);
        self.rest = tail;
        Ok(head)
    }
}

fn u64_le(bytes: &[u8]) -> u64 {
    let mut buf = [0u8; 8];
    buf.copy_from_slice(bytes);
    u64::from_le_bytes(buf)
}

fn u32_le(bytes: &[u8]) -> u32 {
    let mut buf = [0u8; 4];
    buf.copy_from_slice(bytes);
    u32::from_le_bytes(buf)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn key(parts: &[&str]) -> CheckpointKey {
        CheckpointKey::new(
            CheckpointEngine::Vector,
            3,
            9,
            parts.iter().map(|p| p.to_string()).collect(),
        )
    }

    #[test]
    fn encode_roundtrips() {
        let cases: [&[&str]; 5] = [&[], &["docs"], &["docs:emb"], &["a/b", "../c"], &[""]];
        for parts in cases {
            let k = key(parts);
            assert_eq!(CheckpointKey::decode(&k.encode()).expect("decode"), k);
        }
    }

    #[test]
    fn decode_refuses_truncation_and_trailing_bytes() {
        let bytes = key(&["docs"]).encode();
        assert!(CheckpointKey::decode(&bytes[..bytes.len() - 1]).is_err());
        let mut long = bytes.clone();
        long.push(0);
        assert!(CheckpointKey::decode(&long).is_err());
        let mut bad_tag = bytes;
        bad_tag[0] = 0xEE;
        assert!(CheckpointKey::decode(&bad_tag).is_err());
    }

    /// The name has one length whatever the key holds, and stays a plain
    /// path component.
    #[test]
    fn file_name_is_fixed_length_and_plain() {
        let long = "c".repeat(255);
        let field = "f".repeat(200);
        for k in [
            key(&["docs"]),
            key(&[long.as_str(), field.as_str()]),
            key(&["../../etc", "a/b"]),
        ] {
            let name = k.file_name();
            assert_eq!(name.len(), 32 + 1 + KEYED_CKPT_EXTENSION.len());
            assert!(nodedb_types::is_plain_path_component(&name), "{name}");
        }
    }

    /// Every component of the key changes the name.
    #[test]
    fn file_name_depends_on_every_component() {
        let base = key(&["docs", "emb"]);
        let mut other_engine = base.clone();
        other_engine.engine = CheckpointEngine::Spatial;
        let mut other_db = base.clone();
        other_db.database_id = 4;
        let mut other_tenant = base.clone();
        other_tenant.tenant_id = 10;
        // Part boundaries are length-prefixed, so moving bytes between parts
        // is a different key.
        let shifted = key(&["docse", "mb"]);
        for other in [other_engine, other_db, other_tenant, shifted] {
            assert_ne!(base.file_name(), other.file_name(), "{other}");
        }
    }
}
