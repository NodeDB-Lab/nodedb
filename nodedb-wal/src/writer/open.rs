// SPDX-License-Identifier: Apache-2.0

//! Segment-file opening, with `O_DIRECT` support detection.
//!
//! `O_DIRECT` is rejected at `open(2)` with `EINVAL` on most overlayfs
//! configurations and on many network filesystems (and on tmpfs before Linux
//! 6.1, which added support for it). That failure is
//! reported as a distinct error rather than folded into a generic I/O error,
//! and it is never repaired by quietly reopening the segment buffered: the WAL
//! bypasses the page cache so its durability does not depend on writeback, and
//! a silent downgrade would take that guarantee away with nothing to observe.

use std::fs::{File, OpenOptions};
use std::path::Path;

use crate::error::{Result, WalError};

fn base_options() -> OpenOptions {
    let mut opts = OpenOptions::new();
    opts.create(true).write(true).append(false);
    opts
}

#[cfg(target_os = "linux")]
fn segment_options(use_direct_io: bool) -> OpenOptions {
    use std::os::unix::fs::OpenOptionsExt as _;

    let mut opts = base_options();
    if use_direct_io {
        // Bypass the page cache.
        opts.custom_flags(libc::O_DIRECT);
    }
    opts
}

#[cfg(not(target_os = "linux"))]
fn segment_options(_use_direct_io: bool) -> OpenOptions {
    base_options()
}

/// Open (or create) a WAL segment file, applying `O_DIRECT` when requested.
///
/// Returns [`WalError::DirectIoUnsupported`] when the open failed *because of*
/// the `O_DIRECT` flag, and [`WalError::Io`] for every other failure.
pub(crate) fn open_segment_file(path: &Path, use_direct_io: bool) -> Result<File> {
    direct_io_open_injection(path, use_direct_io)?;
    match segment_options(use_direct_io).open(path) {
        Ok(file) => Ok(file),
        Err(err) => Err(classify_open_error(path, use_direct_io, err)),
    }
}

/// Injection hook for "the filesystem refuses `O_DIRECT`".
///
/// Otherwise that verdict is reachable only by finding a mount that rejects the
/// flag, which no test can rely on having — Linux 6.1 gave tmpfs `O_DIRECT`
/// support, so the scratch filesystems tests run on now accept it. Injecting
/// the refusal keeps the startup-failure contract under test everywhere instead
/// of skipping wherever the local mounts happen to be capable.
#[cfg(feature = "failpoints")]
fn direct_io_open_injection(path: &Path, use_direct_io: bool) -> Result<()> {
    if !use_direct_io {
        return Ok(());
    }
    nodedb_types::fail_point_err!("wal::direct_io_unsupported", |_: String| {
        WalError::DirectIoUnsupported {
            path: path.display().to_string(),
        }
    });
    Ok(())
}

#[cfg(not(feature = "failpoints"))]
fn direct_io_open_injection(_path: &Path, _use_direct_io: bool) -> Result<()> {
    Ok(())
}

/// Decide whether a failed open is the filesystem refusing `O_DIRECT` or an
/// ordinary I/O failure.
///
/// `EINVAL` alone is not proof — a malformed path or an unsupported flag
/// combination reports it too — so a buffered open is tried alongside it. Only
/// when the buffered form succeeds is the direct-I/O flag demonstrably the
/// cause.
///
/// The probe deliberately does NOT reuse the segment path. Diagnosing a failure
/// must not change the directory it is diagnosing: opening the segment path
/// with `create(true)` would leave a zero-byte segment behind on a filesystem
/// the server is about to refuse to start on, and that stray file then looks
/// like a real rolled-but-empty segment to the next boot. A throwaway sibling
/// file answers the same question and is unlinked either way.
#[cfg(target_os = "linux")]
fn classify_open_error(path: &Path, use_direct_io: bool, err: std::io::Error) -> WalError {
    if use_direct_io && err.raw_os_error() == Some(libc::EINVAL) && buffered_open_works(path) {
        return WalError::DirectIoUnsupported {
            path: path.display().to_string(),
        };
    }
    WalError::Io(err)
}

/// Whether the segment's own directory accepts a buffered create.
///
/// Side-effect free: the probe file is removed whatever the outcome.
#[cfg(target_os = "linux")]
fn buffered_open_works(path: &Path) -> bool {
    let Some(dir) = path.parent() else {
        return false;
    };
    let probe = dir.join(".nodedb-direct-io-probe");
    let opened = base_options().open(&probe).is_ok();
    let _ = std::fs::remove_file(&probe);
    opened
}

/// Off Linux the direct-I/O flag is never applied, so no open failure can be
/// attributed to it.
#[cfg(not(target_os = "linux"))]
fn classify_open_error(_path: &Path, _use_direct_io: bool, err: std::io::Error) -> WalError {
    WalError::Io(err)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn buffered_open_creates_the_segment() {
        let dir = crate::test_tempdir().unwrap();
        let path = dir.path().join("seg.wal");
        let file = open_segment_file(&path, false).unwrap();
        drop(file);
        assert!(path.exists());
    }

    /// Diagnosing an `O_DIRECT` refusal must not litter the WAL directory.
    ///
    /// The classifier has to open something buffered to prove the flag was the
    /// problem. If it probes the segment path itself with `create(true)`, a
    /// server that is about to refuse to start leaves a zero-byte segment
    /// behind — which the next boot cannot tell apart from a segment that was
    /// rolled and never written to.
    #[cfg(target_os = "linux")]
    #[test]
    fn classifying_a_direct_io_refusal_leaves_no_files_behind() {
        let dir = crate::test_tempdir().unwrap();
        let path = dir.path().join("seg.wal");

        let err = std::io::Error::from_raw_os_error(libc::EINVAL);
        let verdict = classify_open_error(&path, true, err);
        assert!(
            matches!(verdict, WalError::DirectIoUnsupported { .. }),
            "expected a direct-I/O verdict, got {verdict:?}"
        );

        assert!(
            !path.exists(),
            "the diagnostic created the segment file it was only supposed to ask about"
        );
        let leftovers: Vec<_> = std::fs::read_dir(dir.path())
            .unwrap()
            .filter_map(|e| e.ok())
            .map(|e| e.file_name())
            .collect();
        assert!(
            leftovers.is_empty(),
            "diagnosing the failure left files in the WAL directory: {leftovers:?}"
        );
    }

    /// A missing parent directory is an ordinary I/O failure, not a verdict
    /// about the filesystem's `O_DIRECT` support.
    #[test]
    fn unrelated_open_failure_is_not_reported_as_direct_io_unsupported() {
        let dir = crate::test_tempdir().unwrap();
        let path = dir.path().join("absent").join("seg.wal");
        let err = open_segment_file(&path, true).unwrap_err();
        assert!(
            matches!(err, WalError::Io(_)),
            "expected a plain I/O error, got {err:?}"
        );
    }
}
