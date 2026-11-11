// SPDX-License-Identifier: BUSL-1.1

//! Unlink keyed checkpoint files from one generation directory, matched by
//! the key each file stores rather than by its name.
//!
//! A checkpoint file name is a hash of its key, so a name says nothing about
//! which collection owns the file. Reclaim reads each file's stored key (the
//! header only) and unlinks the files whose key matches.

use std::path::Path;

use tracing::debug;

use super::{ReclaimError, ReclaimStats, Result};
use crate::data::executor::checkpoint_decode_error::CheckpointDecodeError;
use crate::data::executor::checkpoint_key::{
    CheckpointKey, KEYED_CKPT_EXTENSION, read_checkpoint_key,
};

/// Suffix the atomic writer gives a staging file.
const TMP_SUFFIX: &str = ".tmp";

/// Remove one file, counting its bytes. A file that is already gone is not an
/// error: reclaim is idempotent and runs again after a partial failure.
fn unlink(engine: &'static str, path: &Path, stats: &mut ReclaimStats) -> Result<()> {
    let size = std::fs::metadata(path).map(|m| m.len()).unwrap_or(0);
    match std::fs::remove_file(path) {
        Ok(()) => {
            stats.files_unlinked = stats.files_unlinked.saturating_add(1);
            stats.bytes_freed = stats.bytes_freed.saturating_add(size);
            debug!(engine, path = %path.display(), size, "reclaim: unlinked checkpoint");
            Ok(())
        }
        Err(source) if source.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(source) => Err(ReclaimError::Io {
            operation: "unlink checkpoint",
            path: path.to_path_buf(),
            source,
        }),
    }
}

/// Unlink the files for one stem: the `.ckpt`, each sibling extension, and the
/// staging debris of each.
fn unlink_stem(
    engine: &'static str,
    gen_dir: &Path,
    stem: &str,
    siblings: &[&str],
    stats: &mut ReclaimStats,
) -> Result<()> {
    for ext in std::iter::once(KEYED_CKPT_EXTENSION).chain(siblings.iter().copied()) {
        unlink(engine, &gen_dir.join(format!("{stem}.{ext}")), stats)?;
        unlink(
            engine,
            &gen_dir.join(format!("{stem}.{ext}{TMP_SUFFIX}")),
            stats,
        )?;
    }
    Ok(())
}

fn key_error(engine: &'static str, path: &Path, detail: impl std::fmt::Display) -> ReclaimError {
    ReclaimError::Key {
        engine,
        path: path.to_path_buf(),
        detail: detail.to_string(),
    }
}

/// Unlink every file in `gen_dir` whose stored key satisfies `matches`, with
/// its `siblings` (extensions sharing the stem, such as a spatial `docmap`)
/// and the staging debris of each.
///
/// A missing directory is a no-op. A published `.ckpt` whose key cannot be
/// read fails the pass: its owner is unknown. A `.ckpt.tmp` with no readable
/// key is skipped: it is debris from a write that never finished, and the
/// manifest never names it.
pub(super) fn reclaim_keyed_matching(
    engine: &'static str,
    gen_dir: &Path,
    siblings: &[&str],
    matches: impl Fn(&CheckpointKey) -> bool,
    stats: &mut ReclaimStats,
) -> Result<()> {
    let entries = match std::fs::read_dir(gen_dir) {
        Ok(entries) => entries,
        Err(source) if source.kind() == std::io::ErrorKind::NotFound => return Ok(()),
        Err(source) => {
            return Err(ReclaimError::Io {
                operation: "read checkpoint live generation",
                path: gen_dir.to_path_buf(),
                source,
            });
        }
    };
    let published = format!(".{KEYED_CKPT_EXTENSION}");
    let staged = format!(".{KEYED_CKPT_EXTENSION}{TMP_SUFFIX}");
    let mut stems = Vec::new();
    for entry in entries {
        let entry = entry.map_err(|source| ReclaimError::Io {
            operation: "read checkpoint entry",
            path: gen_dir.to_path_buf(),
            source,
        })?;
        let path = entry.path();
        let Some(name) = path.file_name().and_then(|n| n.to_str()) else {
            continue;
        };
        let found = if let Some(stem) = name.strip_suffix(&published) {
            let key = read_checkpoint_key(&path).map_err(|e| key_error(engine, &path, e))?;
            (stem, key)
        } else if let Some(stem) = name.strip_suffix(&staged) {
            match read_checkpoint_key(&path) {
                Ok(key) => (stem, key),
                Err(_) => continue,
            }
        } else {
            continue;
        };
        if matches(&found.1) {
            stems.push(found.0.to_string());
        }
    }
    stems.sort();
    stems.dedup();
    for stem in stems {
        unlink_stem(engine, gen_dir, &stem, siblings, stats)?;
    }
    Ok(())
}

/// Unlink the files of exactly `key` in `gen_dir`, with its `siblings` and
/// staging debris. The name is computed from the key. A published file there
/// that stores another key fails the pass and stays in place. A file whose key
/// does not read at all is unlinked: it sits at this key's name, and leaving it
/// would block the drop on a corrupt file.
pub(super) fn reclaim_keyed_exact(
    engine: &'static str,
    gen_dir: &Path,
    key: &CheckpointKey,
    siblings: &[&str],
    stats: &mut ReclaimStats,
) -> Result<()> {
    let path = gen_dir.join(key.file_name());
    if path.exists() {
        match read_checkpoint_key(&path) {
            Ok(stored) if stored != *key => {
                return Err(key_error(
                    engine,
                    &path,
                    format!("holds key {stored}, expected key {key}"),
                ));
            }
            Err(error @ CheckpointDecodeError::KeyNameMismatch { .. }) => {
                return Err(key_error(engine, &path, error));
            }
            Ok(_) | Err(_) => {}
        }
    }
    unlink_stem(engine, gen_dir, &key.file_stem(), siblings, stats)
}
