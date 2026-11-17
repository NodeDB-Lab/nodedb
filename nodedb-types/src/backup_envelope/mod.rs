// SPDX-License-Identifier: Apache-2.0

pub mod crypto;
pub mod database;
pub mod read;
pub mod types;
pub mod verification;
pub mod write;

pub use crypto::parse_encrypted;
pub use database::{
    DATABASE_BACKUP_TENANT, DatabaseBackupManifest, SECTION_ORIGIN_DATABASE_MANIFEST,
};
pub use read::parse;
pub use types::{
    ArrayCatalogBlob, DatabaseBlob, DatabaseDataSection, Envelope, EnvelopeError, EnvelopeMeta,
    IndexRecordBlob, Section, SourceTombstoneEntry, StoredCollectionBlob, SurrogateBindBlob,
};
pub use types::{
    DEFAULT_MAX_SECTION_BYTES, DEFAULT_MAX_TOTAL_BYTES, HEADER_LEN, MAGIC,
    SECTION_ORIGIN_ARRAY_CATALOG, SECTION_ORIGIN_CATALOG_ROWS, SECTION_ORIGIN_DATABASES,
    SECTION_ORIGIN_INDEX_RECORDS, SECTION_ORIGIN_SOURCE_TOMBSTONES, SECTION_ORIGIN_SURROGATE_PK,
    SECTION_ORIGIN_VERIFICATION, SECTION_OVERHEAD, TRAILER_LEN, VERSION,
};
pub use verification::{
    CollectionVerification, VerificationMismatch, VerificationPhase, VerifiedPart, VerifiedTally,
    verification_failure_message,
};
pub use write::EnvelopeWriter;
