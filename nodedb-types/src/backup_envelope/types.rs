// SPDX-License-Identifier: Apache-2.0

//! Shared types, constants, and error definitions for the backup envelope.

use thiserror::Error;

pub const MAGIC: &[u8; 4] = b"NDBB";

/// Backup envelope version stamped in byte 4 of every envelope header.
/// Plaintext and encrypted envelopes carry the same version. The crypto
/// block (68 bytes after the header) distinguishes them.
///
/// Every section body is scoped to a database: data sections carry a
/// [`DatabaseDataSection`], and the metadata sections name the database of
/// each entry. Every envelope carries one `SECTION_ORIGIN_VERIFICATION`
/// section. An envelope of any other version is refused.
pub const VERSION: u8 = 1;

/// Header is fixed-size — 52 bytes (48 framed + 4 crc).
///
/// `tenant_id` in the header is a u64.
pub const HEADER_LEN: usize = 52;
/// Per-section framing overhead: origin(8) + len(4) + crc(4).
pub const SECTION_OVERHEAD: usize = 16;
/// Trailing crc.
pub const TRAILER_LEN: usize = 4;

/// Default cap on total envelope size: 16 GiB. Tunable per call.
pub const DEFAULT_MAX_TOTAL_BYTES: u64 = 16 * 1024 * 1024 * 1024;
/// Default cap on a single section body: 16 GiB.
pub const DEFAULT_MAX_SECTION_BYTES: u64 = 16 * 1024 * 1024 * 1024;

/// Sentinel `origin_node_id` values that mark sections carrying
/// metadata rather than per-node engine data. Restore handlers
/// recognize the sentinel and route the body to the correct
/// catalog writer. Section CRCs validate independently of whether
/// the reader acts on the section body.
pub const SECTION_ORIGIN_CATALOG_ROWS: u64 = 0xFFFF_FFFF_FFFF_FFF0;
pub const SECTION_ORIGIN_SOURCE_TOMBSTONES: u64 = 0xFFFF_FFFF_FFFF_FFF1;
/// Section carrying the tenant's PK→surrogate identity bindings. The surrogate
/// map is DATA-derived per-node state that the per-node data sections do NOT
/// carry (the Data-Plane snapshot handler has no catalog access), so without
/// this section a restored node has documents but cannot resolve PK
/// point-lookups (`WHERE id=<pk>`). The body is a msgpack-encoded
/// `Vec<SurrogateBindBlob>`.
pub const SECTION_ORIGIN_SURROGATE_PK: u64 = 0xFFFF_FFFF_FFFF_FFF2;
/// Section carrying every database the tenant has collections in. The body
/// is a msgpack-encoded `Vec<DatabaseBlob>`. Restore reads it first: every
/// other section names its database by the id recorded here.
pub const SECTION_ORIGIN_DATABASES: u64 = 0xFFFF_FFFF_FFFF_FFF3;
/// Section carrying the per-collection row counts and digests restore checks.
/// The body is a msgpack-encoded `Vec<CollectionVerification>`. Every
/// envelope carries exactly one.
pub const SECTION_ORIGIN_VERIFICATION: u64 = 0xFFFF_FFFF_FFFF_FFF4;
/// Section carrying the catalog row of each of the tenant's arrays. The body
/// is a msgpack-encoded `Vec<ArrayCatalogBlob>`. The cells travel in the data
/// sections.
pub const SECTION_ORIGIN_ARRAY_CATALOG: u64 = 0xFFFF_FFFF_FFFF_FFF5;

/// One database of the backed-up tenant, carried in a
/// `SECTION_ORIGIN_DATABASES` section.
///
/// `database_id` is the id on the source cluster. Restore maps it to the
/// destination database of the same `name`, and creates that database when
/// the destination has none.
#[derive(Debug, Clone, PartialEq, Eq, zerompk::ToMessagePack, zerompk::FromMessagePack)]
pub struct DatabaseBlob {
    pub database_id: u64,
    pub name: String,
    /// zerompk-encoded `DatabaseDescriptor` from the `nodedb` crate: the
    /// database's settings.
    pub descriptor: Vec<u8>,
    /// The database's own quota, when one is set.
    pub database_quota: Option<crate::QuotaRecord>,
    /// The tenant's quota inside this database, when one is set.
    pub tenant_quota: Option<crate::QuotaRecord>,
}

/// Body of a per-node data section: one database's slice of the tenant
/// snapshot a source node took.
#[derive(Debug, Clone, PartialEq, Eq, zerompk::ToMessagePack, zerompk::FromMessagePack)]
pub struct DatabaseDataSection {
    /// Source id of the database the snapshot covers.
    pub database_id: u64,
    /// zerompk-encoded `TenantDataSnapshot` from the `nodedb` crate.
    pub snapshot: Vec<u8>,
}

/// Single catalog-row entry in a catalog-rows section. The outer
/// container is `Vec<StoredCollectionBlob>` msgpack-encoded into the
/// section body. Bytes are the zerompk-encoded `StoredCollection`
/// from the `nodedb` crate — `nodedb-types` intentionally doesn't
/// depend on the `nodedb` catalog types, so the blob is opaque here.
#[derive(Debug, Clone, PartialEq, Eq, zerompk::ToMessagePack, zerompk::FromMessagePack)]
pub struct StoredCollectionBlob {
    /// Source id of the database the collection lives in.
    pub database_id: u64,
    pub name: String,
    /// zerompk-encoded `StoredCollection`.
    pub bytes: Vec<u8>,
}

/// One array's catalog row in a `SECTION_ORIGIN_ARRAY_CATALOG` section.
#[derive(Debug, Clone, PartialEq, Eq, zerompk::ToMessagePack, zerompk::FromMessagePack)]
pub struct ArrayCatalogBlob {
    /// Source id of the database the array lives in.
    pub database_id: u64,
    pub name: String,
    /// zerompk-encoded `ArrayCatalogEntry` from the `nodedb` crate.
    pub bytes: Vec<u8>,
}

/// Single source-side tombstone entry. `purge_lsn` is the Origin WAL
/// LSN at which the hard-delete committed — restore uses it as a
/// per-collection replay barrier so rows older than the purge don't
/// resurrect.
#[derive(Debug, Clone, PartialEq, Eq, zerompk::ToMessagePack, zerompk::FromMessagePack)]
pub struct SourceTombstoneEntry {
    /// Source id of the database the collection lived in.
    pub database_id: u64,
    pub collection: String,
    pub purge_lsn: u64,
}

/// Single PK→surrogate binding carried in a `SECTION_ORIGIN_SURROGATE_PK`
/// section. The outer container is `Vec<SurrogateBindBlob>` msgpack-encoded into
/// the section body. Mirrors one row of the source catalog's `surrogate_pk`
/// table for one `(database_id, tenant_id, collection)`; rebound on the restore
/// side so PK point-lookups resolve.
#[derive(Debug, Clone, PartialEq, Eq, zerompk::ToMessagePack, zerompk::FromMessagePack)]
pub struct SurrogateBindBlob {
    /// Source id of the database the collection lives in.
    pub database_id: u64,
    pub tenant_id: u64,
    /// Bare catalog name of the collection.
    pub collection: String,
    pub pk: Vec<u8>,
    pub surrogate: u32,
}

#[derive(Debug, Error, PartialEq, Eq)]
#[non_exhaustive]
pub enum EnvelopeError {
    #[error("invalid backup format")]
    BadMagic,
    #[error("unsupported backup version: {0}")]
    UnsupportedVersion(u8),
    #[error("invalid backup format")]
    HeaderCrcMismatch,
    #[error("invalid backup format")]
    BodyCrcMismatch,
    #[error("invalid backup format")]
    TrailerCrcMismatch,
    #[error("backup truncated")]
    Truncated,
    #[error("backup tenant mismatch: expected {expected}, got {actual}")]
    TenantMismatch { expected: u64, actual: u64 },
    #[error("backup exceeds size cap of {cap} bytes")]
    OverSizeTotal { cap: u64 },
    #[error("backup section exceeds size cap of {cap} bytes")]
    OverSizeSection { cap: u64 },
    #[error("too many sections: {0}")]
    TooManySections(u16),
    /// The KEK presented at restore time does not match the KEK fingerprint
    /// embedded in the envelope. Surfaces before any decryption attempt so
    /// the caller receives a clear, actionable error rather than an opaque
    /// authentication failure.
    #[error("wrong backup KEK: presented key fingerprint does not match envelope")]
    WrongBackupKek,
    /// AES-256-GCM authentication tag verification failed. Either the
    /// ciphertext or the key is corrupt.
    #[error("backup decryption failed: authentication tag mismatch")]
    DecryptionFailed,
    /// AES-256-GCM encryption failed (e.g. plaintext exceeds the per-message
    /// limit of 2^36 - 31 bytes). Distinct from `DecryptionFailed` so callers
    /// can tell which side of the crypto pipeline produced the error.
    #[error("backup encryption failed")]
    EncryptionFailed,
    /// `getrandom` returned an error when generating nonces or the DEK.
    #[error("backup encryption failed: {0}")]
    RandomFailure(String),
}

/// Header metadata captured at backup time.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct EnvelopeMeta {
    pub tenant_id: u64,
    pub source_vshard_count: u16,
    pub hash_seed: u64,
    pub snapshot_watermark: u64,
}

/// One contiguous body produced by one origin node.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Section {
    pub origin_node_id: u64,
    pub body: Vec<u8>,
}

/// Decoded envelope.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Envelope {
    pub meta: EnvelopeMeta,
    pub sections: Vec<Section>,
}

// ── byte helpers ─────────────────────────────────────────────────────────────

pub fn read2(s: &[u8]) -> [u8; 2] {
    [s[0], s[1]]
}
pub fn read4(s: &[u8]) -> [u8; 4] {
    [s[0], s[1], s[2], s[3]]
}
pub fn read8(s: &[u8]) -> [u8; 8] {
    [s[0], s[1], s[2], s[3], s[4], s[5], s[6], s[7]]
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::backup_envelope::{parse_encrypted, write::EnvelopeWriter};

    const KEK: [u8; 32] = [0x5Au8; 32];

    fn meta() -> EnvelopeMeta {
        EnvelopeMeta {
            tenant_id: 3,
            source_vshard_count: 1024,
            hash_seed: 0,
            snapshot_watermark: 17,
        }
    }

    /// A database-scoped data section and the database list survive the
    /// encrypted envelope intact, each database under its own id.
    #[test]
    fn database_sections_round_trip_through_an_encrypted_envelope() {
        let databases = vec![
            DatabaseBlob {
                database_id: 0,
                name: "default".into(),
                descriptor: vec![1],
                database_quota: None,
                tenant_quota: None,
            },
            DatabaseBlob {
                database_id: 1025,
                name: "sales".into(),
                descriptor: vec![2],
                database_quota: Some(crate::QuotaRecord::DEFAULT),
                tenant_quota: None,
            },
        ];
        let data = DatabaseDataSection {
            database_id: 1025,
            snapshot: vec![9, 8, 7],
        };
        let mut writer = EnvelopeWriter::new(meta());
        writer
            .push_section(
                SECTION_ORIGIN_DATABASES,
                zerompk::to_msgpack_vec(&databases).expect("encode databases"),
            )
            .expect("push databases");
        writer
            .push_section(7, zerompk::to_msgpack_vec(&data).expect("encode data"))
            .expect("push data");
        let bytes = writer.finalize_encrypted(&KEK).expect("encrypt");

        let env = parse_encrypted(&bytes, DEFAULT_MAX_TOTAL_BYTES, &KEK).expect("parse");
        let decoded: Vec<DatabaseBlob> =
            zerompk::from_msgpack(&env.sections[0].body).expect("decode databases");
        assert_eq!(decoded, databases);
        let decoded: DatabaseDataSection =
            zerompk::from_msgpack(&env.sections[1].body).expect("decode data");
        assert_eq!(decoded, data);
    }

    /// The database id sits inside the encrypted body, so the AEAD tag
    /// covers it: flipping a ciphertext byte fails the parse.
    #[test]
    fn a_tampered_database_section_fails_authentication() {
        let data = DatabaseDataSection {
            database_id: 1025,
            snapshot: vec![1, 2, 3, 4],
        };
        let mut writer = EnvelopeWriter::new(meta());
        writer
            .push_section(7, zerompk::to_msgpack_vec(&data).expect("encode data"))
            .expect("push data");
        let mut bytes = writer.finalize_encrypted(&KEK).expect("encrypt");
        // Header, crypto block, origin, length and nonce precede the body.
        let body_start = HEADER_LEN + 68 + 8 + 4 + 12;
        bytes[body_start] ^= 0xFF;
        // Recompute the body and trailer CRCs so only the AEAD tag can refuse.
        let body_len = u32::from_le_bytes(read4(&bytes[HEADER_LEN + 68 + 8..])) as usize;
        let body_end = body_start + body_len;
        let body_crc = crc32c::crc32c(&bytes[body_start..body_end]);
        bytes[body_end..body_end + 4].copy_from_slice(&body_crc.to_le_bytes());
        let trailer_start = bytes.len() - TRAILER_LEN;
        let trailer_crc = crc32c::crc32c(&bytes[..trailer_start]);
        bytes[trailer_start..].copy_from_slice(&trailer_crc.to_le_bytes());
        assert_eq!(
            parse_encrypted(&bytes, DEFAULT_MAX_TOTAL_BYTES, &KEK),
            Err(EnvelopeError::DecryptionFailed)
        );
    }
}
