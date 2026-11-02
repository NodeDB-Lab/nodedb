// SPDX-License-Identifier: BUSL-1.1

//! The durable outcome of a resolved timeseries install, which WAL catch-up
//! rebuilds events from.
//!
//! ## Why it exists
//!
//! A WAL record carries the rows its resolve produced, not the rows its
//! install stored. An install that stores by column name stores values its
//! resolve did not see, and rejects each row whose value conflicts with the
//! schema at its log position. The record alone cannot say which rows landed
//! or how they read. The install writes that outcome here, and catch-up
//! rebuilds events from it.
//!
//! An install whose schema fit is exact stores every row as carried, with the
//! image its resolve took. It writes no outcome, and catch-up rebuilds its
//! events from the record.
//!
//! ## Where it lives
//!
//! Each core owns `ts-outcomes/core-{id}` under the data directory. One file
//! per record LSN holds every by-name install of that record on the core,
//! each keyed by its collection and the digest of its batch. The install
//! writes the file durably before the first row lands. A crash before that
//! write leaves no landed row, so restart replay installs the record again and
//! writes the outcome. The Event Plane consumer of the core removes each file
//! once its persisted watermark passes the file's LSN.

use std::collections::BTreeSet;
use std::path::{Path, PathBuf};

use super::resolved_ingest::{ResolvedTsBatch, TsDriftPolicy};

/// Directory name of the outcome store under the data directory.
const OUTCOME_ROOT: &str = "ts-outcomes";
/// Extension of one record's outcome file.
const OUTCOME_EXT: &str = "outcome";
/// Leading byte of an encoded outcome file.
const OUTCOME_FORMAT_VERSION: u8 = 1;

/// What one by-name install of one resolved batch stored.
#[derive(Debug, Clone, PartialEq, zerompk::ToMessagePack, zerompk::FromMessagePack)]
#[msgpack(map)]
pub struct TsInstallOutcome {
    pub collection: String,
    /// [`batch_digest`] of the batch the install stored.
    pub digest: Vec<u8>,
    /// Positions in the batch's `rows` of the rows that landed, ascending.
    pub landed: Vec<u32>,
    /// The image of each landed row as a scan reads it, in `landed` order.
    pub images: Vec<Vec<u8>>,
}

/// Every by-name install outcome of one record on one core.
#[derive(Debug, Clone, Default, PartialEq, zerompk::ToMessagePack, zerompk::FromMessagePack)]
#[msgpack(map)]
struct TsRecordOutcome {
    installs: Vec<TsInstallOutcome>,
}

fn outcome_error(path: &Path, action: &str, detail: &dyn std::fmt::Display) -> crate::Error {
    crate::Error::Storage {
        engine: "timeseries".to_string(),
        detail: format!(
            "install outcome: failed to {action} {}: {detail}",
            path.display()
        ),
    }
}

/// The outcome store of core `core_id` under `data_dir`.
pub fn outcome_dir(data_dir: &Path, core_id: usize) -> PathBuf {
    data_dir.join(OUTCOME_ROOT).join(format!("core-{core_id}"))
}

fn outcome_file_name(lsn: u64) -> String {
    format!("{lsn:020}.{OUTCOME_EXT}")
}

/// The LSN an outcome file name names. `None` for any other file.
fn outcome_file_lsn(name: &str) -> Option<u64> {
    name.strip_suffix(OUTCOME_EXT)
        .and_then(|stem| stem.strip_suffix('.'))
        .and_then(|lsn| lsn.parse().ok())
}

/// The identity of `batch` within its record: a digest of its bytes under
/// one drift policy, so the logged record and the installed payload digest
/// alike.
pub fn batch_digest(batch: &ResolvedTsBatch) -> crate::Result<Vec<u8>> {
    use sha2::{Digest, Sha256};
    let bytes = batch
        .clone()
        .with_drift(TsDriftPolicy::ApplyByName)
        .to_bytes()?;
    Ok(Sha256::digest(&bytes).to_vec())
}

fn read_record_outcome(path: &Path) -> crate::Result<Option<TsRecordOutcome>> {
    let bytes = match std::fs::read(path) {
        Ok(bytes) => bytes,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(e) => return Err(outcome_error(path, "read", &e)),
    };
    let Some((&version, body)) = bytes.split_first() else {
        return Err(outcome_error(path, "decode", &"the file is empty"));
    };
    if version != OUTCOME_FORMAT_VERSION {
        return Err(outcome_error(
            path,
            "decode",
            &format!("format version {version}, expected {OUTCOME_FORMAT_VERSION}"),
        ));
    }
    zerompk::from_msgpack(body)
        .map(Some)
        .map_err(|e| outcome_error(path, "decode", &e))
}

/// Record `outcome` for the record at `lsn` in the store `dir`, durably. An
/// earlier outcome of the same collection and digest is replaced.
pub fn write_install_outcome(dir: &Path, lsn: u64, outcome: TsInstallOutcome) -> crate::Result<()> {
    std::fs::create_dir_all(dir).map_err(|e| outcome_error(dir, "create", &e))?;
    let name = outcome_file_name(lsn);
    let path = dir.join(&name);
    let mut record = read_record_outcome(&path)?.unwrap_or_default();
    record
        .installs
        .retain(|held| held.collection != outcome.collection || held.digest != outcome.digest);
    record.installs.push(outcome);
    let mut bytes = vec![OUTCOME_FORMAT_VERSION];
    let body = zerompk::to_msgpack_vec(&record).map_err(|e| outcome_error(&path, "encode", &e))?;
    bytes.extend_from_slice(&body);
    nodedb_wal::segment::atomic_write_fsync(dir, &name, &bytes)
        .map_err(|e| outcome_error(&path, "write", &e))
}

/// Remove every outcome file in `dir` whose LSN is at or below `through`.
pub fn prune_outcomes_through(dir: &Path, through: u64) -> crate::Result<()> {
    let entries = match std::fs::read_dir(dir) {
        Ok(entries) => entries,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(()),
        Err(e) => return Err(outcome_error(dir, "list", &e)),
    };
    for entry in entries.flatten() {
        let name = entry.file_name();
        let Some(lsn) = name.to_str().and_then(outcome_file_lsn) else {
            continue;
        };
        if lsn <= through {
            let path = entry.path();
            match std::fs::remove_file(&path) {
                Ok(()) => {}
                Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
                Err(e) => return Err(outcome_error(&path, "remove", &e)),
            }
        }
    }
    Ok(())
}

/// The outcome store of one core, as catch-up reads it.
#[derive(Debug, Default)]
pub struct TsOutcomeIndex {
    dir: PathBuf,
    /// The LSNs that have an outcome file.
    lsns: BTreeSet<u64>,
}

impl TsOutcomeIndex {
    /// List the store `dir`. A missing directory holds no outcome.
    pub fn load(dir: &Path) -> crate::Result<Self> {
        let mut lsns = BTreeSet::new();
        match std::fs::read_dir(dir) {
            Ok(entries) => {
                for entry in entries.flatten() {
                    if let Some(lsn) = entry.file_name().to_str().and_then(outcome_file_lsn) {
                        lsns.insert(lsn);
                    }
                }
            }
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
            Err(e) => return Err(outcome_error(dir, "list", &e)),
        }
        Ok(Self {
            dir: dir.to_path_buf(),
            lsns,
        })
    }

    /// The outcome of the install of `batch` into `collection` by the record
    /// at `lsn`. `Ok(None)` when the install wrote none: it stored every row
    /// as the batch carries it.
    pub fn lookup(
        &self,
        lsn: u64,
        collection: &str,
        batch: &ResolvedTsBatch,
    ) -> crate::Result<Option<TsInstallOutcome>> {
        if !self.lsns.contains(&lsn) {
            return Ok(None);
        }
        let Some(record) = read_record_outcome(&self.dir.join(outcome_file_name(lsn)))? else {
            return Ok(None);
        };
        let digest = batch_digest(batch)?;
        Ok(record
            .installs
            .into_iter()
            .find(|install| install.collection == collection && install.digest == digest))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::engine::timeseries::columnar_memtable::{ColumnType, TimeKind};

    fn batch(now_ms: i64) -> ResolvedTsBatch {
        ResolvedTsBatch {
            measurement: "metrics".into(),
            columns: vec![("timestamp".into(), ColumnType::Timestamp(TimeKind::Millis))],
            timestamp_idx: 0,
            drift: TsDriftPolicy::Refuse,
            now_ms,
            resolved_bytes: 0,
            emits_events: true,
            rows: Vec::new(),
            rejected: 0,
            first_rejection: None,
            carried_system: false,
        }
    }

    fn outcome(batch: &ResolvedTsBatch) -> TsInstallOutcome {
        TsInstallOutcome {
            collection: "metrics".into(),
            digest: batch_digest(batch).expect("digest"),
            landed: vec![1],
            images: vec![vec![0x80]],
        }
    }

    #[test]
    fn an_outcome_is_found_by_record_collection_and_batch() {
        let dir = tempfile::tempdir().expect("tempdir");
        let first = batch(1_000);
        let second = batch(2_000);
        write_install_outcome(dir.path(), 7, outcome(&first)).expect("write");
        write_install_outcome(dir.path(), 7, outcome(&second)).expect("write");

        let index = TsOutcomeIndex::load(dir.path()).expect("load");
        let logged = first.clone().with_drift(TsDriftPolicy::ApplyByName);
        assert_eq!(
            index.lookup(7, "metrics", &logged).expect("lookup"),
            Some(outcome(&first)),
            "the drift policy does not change a batch's identity"
        );
        assert!(index.lookup(7, "other", &first).expect("lookup").is_none());
        assert!(
            index
                .lookup(8, "metrics", &first)
                .expect("lookup")
                .is_none()
        );
    }

    #[test]
    fn pruning_removes_outcomes_at_or_below_the_watermark() {
        let dir = tempfile::tempdir().expect("tempdir");
        let held = batch(1_000);
        for lsn in [3, 5, 9] {
            write_install_outcome(dir.path(), lsn, outcome(&held)).expect("write");
        }
        prune_outcomes_through(dir.path(), 5).expect("prune");
        let index = TsOutcomeIndex::load(dir.path()).expect("load");
        assert!(index.lookup(5, "metrics", &held).expect("lookup").is_none());
        assert!(index.lookup(9, "metrics", &held).expect("lookup").is_some());
    }

    #[test]
    fn a_missing_store_holds_no_outcome() {
        let dir = tempfile::tempdir().expect("tempdir");
        let index = TsOutcomeIndex::load(&dir.path().join("absent")).expect("load");
        assert!(
            index
                .lookup(1, "metrics", &batch(0))
                .expect("lookup")
                .is_none()
        );
    }
}
