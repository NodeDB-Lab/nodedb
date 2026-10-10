// SPDX-License-Identifier: Apache-2.0

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

    /// Checkpoint frame is corrupt: bad version, truncated payload, or CRC mismatch.
    #[error("checkpoint frame corrupt at {path}: {detail}")]
    CheckpointCorrupt { path: String, detail: String },

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

    /// A segment holds records in a WAL format version this build cannot read.
    ///
    /// The same finding as [`WalError::UnsupportedVersion`], reported by the
    /// callers that know which file carries it. A reader sees one header at a
    /// time and holds no path, and a version alone does not tell an operator
    /// which segment to act on.
    #[error(
        "WAL segment '{path}' holds records in WAL format version {version}; this build reads \
         version {supported}"
    )]
    SegmentFormatVersion {
        path: String,
        version: u16,
        supported: u16,
    },

    /// Unknown required record type encountered during replay.
    /// Optional unknown record types are safely skipped.
    #[error("unknown required record type {record_type} at LSN {lsn}")]
    UnknownRequiredRecordType { record_type: u32, lsn: u64 },

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

    /// A record still carries `ENCRYPTED_FLAG` at a point where plaintext is
    /// required, and no key ring is available to turn it back into plaintext.
    ///
    /// Passing the ciphertext through would hand every downstream decoder
    /// bytes it cannot parse: the lucky ones fail with a confusing framing
    /// error, the unlucky ones decode garbage into engine state. Skipping the
    /// record would silently drop a committed write. Neither is recoverable at
    /// this layer, so the only honest outcome is to refuse to replay.
    #[error("WAL record at LSN {lsn} is encrypted but no decryption key is available ({context})")]
    EncryptedRecordWithoutKey { lsn: u64, context: &'static str },

    /// `DoubleWriteBuffer::open` was called with `DwbMode::Off`. Callers
    /// that want the DWB disabled must not call `open` at all.
    #[error("DoubleWriteBuffer::open called with DwbMode::Off")]
    DwbOffNotOpenable,

    /// Record payload failed structural validation (truncation, bad length
    /// prefix, invalid UTF-8, etc.). Distinct from [`WalError::ChecksumMismatch`]
    /// — the bytes passed CRC but the payload's own framing is wrong.
    #[error("corrupt WAL record at LSN {lsn}: {detail}")]
    CorruptRecord { lsn: u64, detail: String },

    /// Record payload is structurally invalid at parse time, before the
    /// surrounding LSN context is known (e.g., anchor payload decoded from
    /// a byte slice during unit-level use).
    #[error("invalid WAL payload: {detail}")]
    InvalidPayload { detail: String },

    /// The filesystem holding the WAL cannot open files with `O_DIRECT`.
    ///
    /// Most overlayfs configurations, many network filesystems, and tmpfs
    /// before Linux 6.1 reject `O_DIRECT` at `open(2)` with `EINVAL`. Kept
    /// distinct from a generic [`WalError::Io`] because the operator action
    /// is specific — relocate the data directory or opt out of direct I/O
    /// deliberately — and because silently continuing with buffered I/O
    /// would downgrade the WAL's durability guarantee without anyone being
    /// told.
    #[error("filesystem holding {path} does not support O_DIRECT")]
    DirectIoUnsupported { path: String },

    /// The filesystem ran out of space while appending to the WAL (ENOSPC).
    ///
    /// Distinct from a generic [`WalError::Io`] so callers can stop
    /// acknowledging writes and surface a specific operator action instead of
    /// retrying a write that cannot succeed.
    #[error("WAL write failed: no space left on device ({context})")]
    OutOfSpace { context: &'static str },

    /// An fsync of the WAL segment failed, so the writer is poisoned and
    /// permanently refuses further work.
    ///
    /// A failed fsync is terminal, not transient. Linux reports a writeback
    /// error exactly once (`errseq_t`) and drops the dirty pages that failed,
    /// so the bytes the writer already handed to the page cache are gone and
    /// no retry can put them back. Continuing to serve would let a later
    /// `sync` see an empty buffer, report success, and acknowledge records
    /// that no longer exist anywhere. The only correct move at this layer is
    /// to stop: the segment must be re-opened from what is actually on disk.
    #[error("WAL durability lost, writer poisoned: {detail}")]
    DurabilityLost { detail: String },

    /// A WAL segment is damaged in the middle: a valid record was found
    /// *after* the damaged region, so it cannot be the unfsynced tail of the
    /// last write.
    ///
    /// Stopping at the damage point and calling everything before it "the
    /// committed prefix" would silently discard the committed records that
    /// follow, so recovery refuses to proceed instead.
    #[error(
        "mid-file WAL corruption in {path} at offset {offset}: a valid record (LSN {resync_lsn}) \
         follows the damaged region at offset {resync_offset}"
    )]
    MidFileCorruption {
        path: String,
        offset: u64,
        resync_offset: u64,
        resync_lsn: u64,
    },

    /// Two surviving WAL segments are not contiguous: the later segment's
    /// declared first LSN is above the LSN that follows the end of the earlier
    /// one, so a whole segment is missing from the middle of the log.
    ///
    /// Concatenating the two would hand recovery a record stream with a silent
    /// hole in it, so replay refuses instead. A missing *prefix* is legal
    /// (checkpoint truncation deletes whole segments below the checkpoint) and
    /// does not produce this error.
    #[error(
        "WAL segment LSN gap: {path} starts at LSN {found_lsn}, but {previous_path} ends at \
         LSN {previous_last_lsn} — LSNs {expected_lsn}..{found_lsn} are missing"
    )]
    SegmentLsnGap {
        path: String,
        previous_path: String,
        previous_last_lsn: u64,
        expected_lsn: u64,
        found_lsn: u64,
    },

    /// The last segment starts at or below an LSN an earlier segment already
    /// holds. An interrupted roll created it while the earlier segment stayed
    /// active. Resuming it would reissue LSNs, so the WAL refuses to open.
    #[error(
        "WAL segment {path} starts at LSN {first_lsn}, but {previous_path} already holds \
         records through LSN {previous_last_lsn}; resuming would reissue those LSNs. \
         {path} was created by an interrupted segment roll: check that it holds no \
         records, remove it, and restart"
    )]
    SegmentOverlapsPrevious {
        path: String,
        first_lsn: u64,
        previous_path: String,
        previous_last_lsn: u64,
    },

    /// A segment roll failed, and removing the segment file it created also
    /// failed. The next open refuses that file with `SegmentOverlapsPrevious`.
    #[error(
        "WAL segment roll failed: {roll}; removing the unused segment {path} also failed: \
         {cleanup}"
    )]
    RollCleanupFailed {
        path: String,
        roll: Box<WalError>,
        cleanup: Box<WalError>,
    },

    /// A replay was asked for the suffix starting at `from_lsn`, but the WAL
    /// no longer retains it: checkpoint truncation has already deleted every
    /// segment below `retained_floor_lsn`.
    ///
    /// Filtering the surviving records by `lsn >= from_lsn` would hand the
    /// caller a *shorter* suffix that is indistinguishable from a complete
    /// one — no hole, no gap between surviving segments, just fewer records
    /// than the caller's own bookkeeping says it must observe. A consumer that
    /// treats that as complete advances its watermark past records it never
    /// saw, and every effect keyed on them (change data capture, triggers,
    /// streaming materialized views) is lost with nothing recorded anywhere.
    ///
    /// Truncation is supposed to hold below every consumer's watermark, so
    /// this is a broken invariant rather than an expected outcome; the error
    /// exists so a residual violation is loud instead of silent.
    #[error(
        "WAL replay requested from LSN {from_lsn}, but the earliest retained LSN is \
         {retained_floor_lsn} ({earliest_segment}) — LSNs {from_lsn}..{retained_floor_lsn} were \
         truncated and can never be replayed"
    )]
    ReplayBelowRetainedFloor {
        from_lsn: u64,
        retained_floor_lsn: u64,
        earliest_segment: String,
    },
}

impl WalError {
    /// Name the segment a version gap was found in.
    ///
    /// A reader reports [`WalError::UnsupportedVersion`] from header validation
    /// alone and holds no path. The callers that opened the file do, and only
    /// they can turn the finding into something an operator can act on. Every
    /// other error passes through unchanged.
    #[cfg(not(target_arch = "wasm32"))]
    pub(crate) fn with_segment_path(self, path: &std::path::Path) -> Self {
        match self {
            Self::UnsupportedVersion { version, supported } => Self::SegmentFormatVersion {
                path: path.display().to_string(),
                version,
                supported,
            },
            other => other,
        }
    }
}

pub type Result<T> = std::result::Result<T, WalError>;
