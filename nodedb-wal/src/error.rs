/// Errors produced by the WAL subsystem.
#[derive(Debug, thiserror::Error)]
pub enum WalError {
    /// I/O error from the underlying file operations.
    #[error("WAL I/O error: {0}")]
    Io(#[from] std::io::Error),

    /// CRC32C checksum mismatch during read/replay.
    #[error("WAL checksum mismatch at LSN {lsn}: expected {expected:#010x}, got {actual:#010x}")]
    ChecksumMismatch {
        lsn: u64,
        expected: u32,
        actual: u32,
    },

    /// Record header has an invalid magic number — file is corrupted or not a WAL.
    #[error("invalid WAL magic at offset {offset}: expected {expected:#010x}, got {actual:#010x}")]
    InvalidMagic {
        offset: u64,
        expected: u32,
        actual: u32,
    },

    /// WAL format version is not supported by this binary.
    #[error("unsupported WAL format version {version} (supported: {supported})")]
    UnsupportedVersion { version: u16, supported: u16 },

    /// Unknown required record type encountered during replay.
    /// Optional unknown record types are safely skipped.
    #[error("unknown required record type {record_type} at LSN {lsn}")]
    UnknownRequiredRecordType { record_type: u16, lsn: u64 },

    /// Write payload exceeds maximum record size.
    #[error("payload too large: {size} bytes (max: {max})")]
    PayloadTooLarge { size: usize, max: usize },

    /// Attempted to write to a WAL that has been closed or is in error state.
    #[error("WAL is sealed and no longer accepting writes")]
    Sealed,

    /// Alignment violation — O_DIRECT requires aligned buffers and offsets.
    #[error("alignment violation: {context} (required: {required}, actual: {actual})")]
    AlignmentViolation {
        context: &'static str,
        required: usize,
        actual: usize,
    },

    /// A mutex was poisoned (another thread panicked while holding the lock).
    #[error("WAL lock poisoned: {context}")]
    LockPoisoned { context: &'static str },

    /// Encryption or decryption failed.
    #[error("WAL encryption error: {detail}")]
    EncryptionError { detail: String },
}

pub type Result<T> = std::result::Result<T, WalError>;
