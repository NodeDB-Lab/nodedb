// SPDX-License-Identifier: Apache-2.0

use std::path::{Path, PathBuf};
use std::sync::Arc;

use crate::align::DEFAULT_ALIGNMENT;
use crate::double_write::{DoubleWriteBuffer, DwbDegradation, DwbMode, DwbProtection};
use crate::error::{Result, WalError};
use crate::time_anchors::TimeAnchors;

/// Default write buffer size: 2 MiB.
///
/// This is the batch size for group commit. Records accumulate here until
/// the buffer is full or `sync()` is called.
///
/// Matches `WalTuning::write_buffer_size` default. Override via
/// `WalWriterConfig::write_buffer_size` at construction time.
pub const DEFAULT_WRITE_BUFFER_SIZE: usize = 2 * 1024 * 1024;

/// Configuration for the WAL writer.
#[derive(Debug, Clone)]
pub struct WalWriterConfig {
    /// Size of the aligned write buffer (rounded up to alignment).
    pub write_buffer_size: usize,

    /// O_DIRECT alignment (typically 4096 for NVMe).
    pub alignment: usize,

    /// Whether to use O_DIRECT. Set to `false` for testing on filesystems
    /// that don't support it (e.g., tmpfs).
    pub use_direct_io: bool,

    /// Double-write buffer I/O mode. `None` means "mirror the parent" —
    /// `Direct` when `use_direct_io` is true, `Buffered` otherwise.
    /// `Some(DwbMode::Off)` disables the DWB entirely.
    pub dwb_mode: Option<DwbMode>,

    /// Commit-time anchors. When set, every sync that commits new records
    /// appends one `TimeAnchor` record to its batch and, once fsynced, records
    /// it here. `None` writes no anchors.
    pub time_anchors: Option<Arc<TimeAnchors>>,
}

impl Default for WalWriterConfig {
    fn default() -> Self {
        Self {
            write_buffer_size: DEFAULT_WRITE_BUFFER_SIZE,
            alignment: DEFAULT_ALIGNMENT,
            use_direct_io: true,
            dwb_mode: None,
            time_anchors: None,
        }
    }
}

pub(crate) fn resolve_dwb_mode(config: &WalWriterConfig) -> DwbMode {
    config
        .dwb_mode
        .unwrap_or_else(|| DwbMode::default_for_parent(config.use_direct_io))
}

/// A writer's double-write buffer and the protection standing that goes with
/// it. Kept together because "no buffer" means two very different things —
/// deliberately off, or a DWB that failed to open — and only the second one is
/// a degradation the caller has to know about.
pub(crate) struct DwbSetup {
    /// Where the DWB lives, so a degraded writer can reattach later. `None`
    /// only when the DWB is configured off.
    pub path: Option<PathBuf>,
    pub buffer: Option<DoubleWriteBuffer>,
    pub protection: DwbProtection,
}

pub(crate) fn open_dwb_for(config: &WalWriterConfig, path: &Path) -> DwbSetup {
    let mode = resolve_dwb_mode(config);
    if mode == DwbMode::Off {
        return DwbSetup {
            path: None,
            buffer: None,
            protection: DwbProtection::Off,
        };
    }
    let dwb_path = path.with_extension("dwb");
    let (buffer, protection) = match open_dwb_at(&dwb_path, mode) {
        Some(d) => (Some(d), DwbProtection::Active),
        None => (None, DwbProtection::Degraded(DwbDegradation::OpenFailed)),
    };
    DwbSetup {
        path: Some(dwb_path),
        buffer,
        protection,
    }
}

/// Open the DWB file itself. A failure is logged and reported as absence — the
/// caller records the degradation, and the WAL keeps working either way.
pub(crate) fn open_dwb_at(dwb_path: &Path, mode: DwbMode) -> Option<DoubleWriteBuffer> {
    match DoubleWriteBuffer::open(dwb_path, mode) {
        Ok(dwb) => Some(dwb),
        Err(e) => {
            tracing::warn!(
                path = %dwb_path.display(),
                error = %e,
                mode = ?mode,
                "failed to open DWB — torn-write protection lost for this writer"
            );
            None
        }
    }
}

/// Where a reopened segment resumes writing.
///
/// Every completed flush ends on an alignment boundary (the batch is padded
/// with a framed `Noop` record), so an unaligned recovery offset means the
/// final batch was torn by a crash mid-write. O_DIRECT cannot write at an
/// unaligned offset, so the writer resumes at the block boundary below the
/// committed end. That block can still hold committed bytes, such as the tail
/// of an intact record whose padding was torn. [`resume_carry`] reads them so
/// the writer's first write puts them back unchanged.
pub fn resume_offset(end_offset: u64, use_direct_io: bool, alignment: usize, path: &Path) -> u64 {
    if !use_direct_io {
        return end_offset;
    }
    let alignment = alignment as u64;
    let aligned = end_offset - (end_offset % alignment);
    if aligned != end_offset {
        tracing::warn!(
            path = %path.display(),
            end_offset,
            resume_offset = aligned,
            "WAL segment ends mid-block; the next write rewrites the block and keeps its committed bytes"
        );
    }
    aligned
}

/// The committed bytes of a reopened segment between `resume` and
/// `end_offset`.
///
/// The writer preloads them into its write buffer, so the block it resumes in
/// is rewritten with them intact. Empty when the writer resumes at the
/// committed end.
pub(crate) fn resume_carry(path: &Path, resume: u64, end_offset: u64) -> Result<Vec<u8>> {
    use std::os::unix::fs::FileExt as _;

    let len = usize::try_from(end_offset.saturating_sub(resume)).map_err(|_| {
        WalError::Io(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            "WAL resume carry does not fit usize",
        ))
    })?;
    if len == 0 {
        return Ok(Vec::new());
    }
    let file = std::fs::File::open(path)?;
    let mut carry = vec![0u8; len];
    file.read_exact_at(&mut carry, resume)?;
    Ok(carry)
}
