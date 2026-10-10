// SPDX-License-Identifier: BUSL-1.1

//! WAL initialization, validation, replay, and tombstone loading.

use std::sync::Arc;

use tracing::info;

use crate::ServerConfig;
use crate::wal::WalManager;

/// Open, validate, and replay the WAL. Load the tombstone set from redb + WAL.
///
/// Returns `(wal, wal_records, tombstones)`. Exits the process on unrecoverable
/// corruption — a dirty WAL cannot be safely replayed.
pub fn init_wal(
    config: &ServerConfig,
) -> anyhow::Result<(
    Arc<WalManager>,
    Arc<[nodedb_wal::WalRecord]>,
    nodedb_wal::TombstoneSet,
)> {
    let wal_segment_target = config.checkpoint.wal_segment_target_bytes();
    let wal_dir = config.wal_dir();

    let wal = {
        let mut mgr =
            WalManager::open_with_tuning(&wal_dir, wal_segment_target, &config.tuning.wal)
                .map_err(|error| wal_open_error(&wal_dir, error))?;
        if let Some(ref enc) = config.encryption {
            let key = nodedb_wal::crypto::WalEncryptionKey::from_file(&enc.key_path)
                .map_err(crate::Error::Wal)?;
            mgr.set_encryption_ring(nodedb_wal::crypto::KeyRing::new(key))?;
            info!(key_path = %enc.key_path.display(), "WAL encryption enabled");
        }
        Arc::new(mgr)
    };
    info!(next_lsn = %wal.next_lsn(), "WAL ready");

    if let Err(e) = wal.validate_for_startup() {
        // Neutral on purpose: this wraps every validation error. The wrapped
        // error names the cause, such as a WAL format version this build
        // cannot read.
        tracing::error!(
            error = %e,
            "StartupError: WAL validation failed: cannot start with these WAL segments"
        );
        std::process::exit(1);
    }

    let wal_records: Arc<[nodedb_wal::WalRecord]> = match wal.replay() {
        Ok(records) => {
            if !records.is_empty() {
                info!(records = records.len(), "WAL records loaded for replay");
            }
            Arc::from(records.into_boxed_slice())
        }
        Err(e) => {
            tracing::error!(
                error = %e,
                "StartupError: WAL replay failed — cannot start with a corrupt or unreadable WAL"
            );
            std::process::exit(1);
        }
    };
    // Replay recorded the persisted time anchors. Records past the last one
    // are durable now, so they take the boot time.
    wal.anchor_recovered_tail();

    // Settle every record group a crash left broken before any replay, then
    // read the stream the settle completed.
    let wal_records = match crate::bootstrap::write_group_settle::settle_write_groups(
        &wal,
        &config.server.data_dir,
        &wal_records,
    )
    .and_then(|appended| {
        if appended {
            wal.replay()
                .map(|records| Arc::from(records.into_boxed_slice()))
        } else {
            Ok(wal_records)
        }
    }) {
        Ok(records) => records,
        Err(e) => {
            tracing::error!(
                error = %e,
                "StartupError: settling the write groups a crash left broken failed"
            );
            std::process::exit(1);
        }
    };

    tracing::warn!(
        catalog = %config.catalog_path().display(),
        "redb catalog stored unencrypted; use a dm-crypt/LUKS volume \
         for at-rest catalog encryption"
    );

    let tombstones = load_tombstones(config, &wal_records)?;
    Ok((wal, wal_records, tombstones))
}

/// Translate a WAL open failure into a startup error.
///
/// A filesystem that cannot do direct I/O gets its own message: the WAL is
/// `O_DIRECT` by design, the server will not downgrade itself to buffered
/// writes to get past this, and the operator is the only one who can decide
/// between the two ways out.
///
/// A store whose records are in a format this build cannot read gets one too.
/// The open reaches it first, because it recovers the newest segment, and the
/// WAL layer reports the segment and both versions; what it cannot know is which
/// actions exist, and those belong to the server.
fn wal_open_error(wal_dir: &std::path::Path, error: crate::Error) -> anyhow::Error {
    if let crate::Error::Wal(nodedb_wal::WalError::DirectIoUnsupported { .. }) = error {
        return anyhow::Error::new(nodedb_types::NodeDbError::wal_at(
            "open",
            direct_io_unsupported_message(wal_dir),
        ));
    }
    if let crate::Error::Wal(nodedb_wal::WalError::SegmentFormatVersion {
        path,
        version,
        supported,
    }) = &error
    {
        return anyhow::Error::new(crate::Error::VersionCompat {
            detail: crate::wal::manager::replay::version_gap_detail(path, *version, *supported),
        });
    }
    anyhow::Error::new(error)
}

/// The operator-facing text for an unsupported filesystem: what failed, where,
/// and both ways to resolve it.
fn direct_io_unsupported_message(wal_dir: &std::path::Path) -> String {
    format!(
        "the filesystem holding the WAL directory {} does not support O_DIRECT, and the WAL \
         will not fall back to buffered I/O because that silently weakens durability. \
         Either move the data directory onto a filesystem that supports O_DIRECT here (most \
         local filesystems, such as ext4, XFS, or a raw NVMe mount, do; some overlayfs \
         configurations, many network filesystems, and older kernels do not), or opt out \
         explicitly by setting NODEDB_WAL_DIRECT_IO=false (equivalently `direct_io = false` \
         under [tuning.wal] in the config file) and accepting page-cached WAL writes",
        wal_dir.display()
    )
}

fn load_tombstones(
    config: &ServerConfig,
    wal_records: &Arc<[nodedb_wal::WalRecord]>,
) -> anyhow::Result<nodedb_wal::TombstoneSet> {
    let catalog_path = config.catalog_path();
    let mut set = nodedb_wal::extract_tombstones(wal_records)
        .map_err(|error| anyhow::anyhow!("extract WAL tombstones: {error}"))?;
    let catalog = match crate::bootstrap::catalog_open::CatalogForRead::open(&catalog_path) {
        Ok(Some(catalog)) => catalog,
        // No catalog yet: a genuine fresh start, so the WAL's own tombstones
        // are the whole truth.
        Ok(None) => return Ok(set),
        // A catalog exists and could not be read. Dropping the PERSISTED
        // tombstones here would resurrect deleted rows on replay, so this is a
        // hard failure rather than a quiet degradation.
        Err(error) => {
            return Err(anyhow::anyhow!(
                "catalog exists but could not be opened to load persisted WAL tombstones: {error}"
            ));
        }
    };
    let persisted = catalog
        .load_wal_tombstones()
        .map_err(|error| anyhow::anyhow!("load persisted WAL tombstones: {error}"))?;
    if !persisted.is_empty() {
        info!(
            persisted = persisted.len(),
            in_wal = set.len(),
            "merging persisted collection tombstones into replay set"
        );
    }
    set.extend(persisted);
    Ok(set)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::PathBuf;

    /// The operator reading this line in the startup log must learn where the
    /// WAL lives and both ways out — otherwise the only fix they can guess is
    /// the wrong one (deleting the data directory).
    #[test]
    fn direct_io_message_names_the_directory_and_both_remedies() {
        let msg = direct_io_unsupported_message(&PathBuf::from("/srv/nodedb/data/wal"));
        assert!(msg.contains("/srv/nodedb/data/wal"), "{msg}");
        assert!(msg.contains("O_DIRECT"), "{msg}");
        assert!(msg.contains("NODEDB_WAL_DIRECT_IO=false"), "{msg}");
        assert!(msg.contains("direct_io = false"), "{msg}");
    }

    /// The unsupported-filesystem case is the only one that gets the
    /// relocate-or-opt-out message; every other WAL failure keeps its own.
    #[test]
    fn only_direct_io_unsupported_is_translated() {
        let dir = PathBuf::from("/srv/nodedb/data/wal");

        let unsupported = wal_open_error(
            &dir,
            crate::Error::Wal(nodedb_wal::WalError::DirectIoUnsupported {
                path: dir.display().to_string(),
            }),
        );
        assert!(unsupported.to_string().contains("NODEDB_WAL_DIRECT_IO"));

        let other = wal_open_error(&dir, crate::Error::Wal(nodedb_wal::WalError::Sealed));
        assert!(!other.to_string().contains("NODEDB_WAL_DIRECT_IO"));
    }
}
