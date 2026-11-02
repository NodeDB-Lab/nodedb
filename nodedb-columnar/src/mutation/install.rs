// SPDX-License-Identifier: Apache-2.0

//! Install of row versions at given stamps: the write a backup restore
//! replays.
//!
//! A live write picks the stamp of each version it creates and ends the
//! version it supersedes. A restore knows both stamps of every version.
//! [`MutationEngine::install_version`] appends a version at exactly its
//! `sys_from` and ends it at exactly its `sys_to`. Installing the versions of
//! each key in `sys_from` order rebuilds the PK index, the delete bitmaps, the
//! per-key chains and the closed-key index the live writes built.

use std::collections::HashMap;

use nodedb_types::surrogate::Surrogate;
use nodedb_types::value::Value;

use crate::error::ColumnarError;
use crate::pk_index::RowLocation;
use crate::wal_record::{ColumnarWalRecord, encode_row_for_wal};

use super::engine::{MutationEngine, MutationResult};

/// One version of a batch install ([`MutationEngine::install_versions`]).
pub struct InstallRow<'a> {
    /// The row's cells in schema column order.
    pub values: &'a [Value],
    pub surrogate: Option<Surrogate>,
    /// The ordinal the version starts at.
    pub sys_from: i64,
    /// The ordinal the version ends at. `None` for an open version.
    pub sys_to: Option<i64>,
    /// A clone copy: it installs only where its key holds no version, live,
    /// ended or closed. A write into the clone then wins over a copy.
    pub clone_copy: bool,
}

/// The index bindings of a key before a batch install first wrote it.
struct PriorKey {
    /// The row the PK index bound. The install ended it.
    bound: Option<RowLocation>,
    /// The tombstone the closed-key index bound.
    closed: Option<RowLocation>,
}

impl MutationEngine {
    /// Install one row version created at `sys_from` and ended at `sys_to`.
    ///
    /// The row is appended to the memtable stamped `sys_from`. A row its key
    /// binds ends at `sys_from`, as an overwrite ends it. An open version
    /// (`sys_to` is `None`) is bound in the PK index. An ended version
    /// records `sys_to`, joins its key's chain, and leaves its key unbound.
    /// A version of a bitemporal collection that ends at its own `sys_from`
    /// is a tombstone: the closed-key index binds it, as a close binds it.
    ///
    /// A version the key already binds at exactly `sys_from`, with the same
    /// cells, is installed already: the call writes nothing, so a resumed
    /// install is idempotent. A key holds one version per ordinal, so a
    /// bound row in a flushed segment at `sys_from` is that version.
    ///
    /// `InvalidVersionInterval` when `sys_to` is below `sys_from`, when the
    /// row the key binds starts above `sys_from`, or when it starts at
    /// `sys_from` with other cells. Any error leaves the engine unchanged.
    pub fn install_version(
        &mut self,
        values: &[Value],
        surrogate: Option<Surrogate>,
        sys_from: i64,
        sys_to: Option<i64>,
    ) -> Result<MutationResult, ColumnarError> {
        let pk_bytes = self.extract_pk_bytes(values)?;
        if let Some(to) = sys_to
            && to < sys_from
        {
            return Err(ColumnarError::InvalidVersionInterval {
                sys_from,
                sys_to: to,
            });
        }
        let prior = self.pk_index.get(&pk_bytes).copied();
        if let Some(prior_from) = prior.and_then(|loc| self.sys_from_at(loc))
            && prior_from > sys_from
        {
            return Err(ColumnarError::InvalidVersionInterval {
                sys_from: prior_from,
                sys_to: sys_from,
            });
        }
        if let Some(loc) = prior
            && sys_to.is_none()
            && self.sys_from_at(loc) == Some(sys_from)
        {
            if self.bound_cells_differ(loc, values)? {
                return Err(ColumnarError::InvalidVersionInterval {
                    sys_from,
                    sys_to: sys_from,
                });
            }
            return Ok(MutationResult {
                wal_records: Vec::new(),
            });
        }

        // Every fallible step runs before any state changes.
        let row_data = encode_row_for_wal(values)?;
        self.memtable.append_row(values)?;
        self.stamp_memtable_row(sys_from);

        let mut wal_records = Vec::with_capacity(3);
        if let Some(prior) = prior {
            self.pk_index.remove(&pk_bytes);
            wal_records.push(self.end_row(pk_bytes.clone(), prior, sys_from));
        }
        wal_records.push(ColumnarWalRecord::InsertRow {
            collection: self.collection.clone(),
            row_data,
            sys_from,
        });

        let location = RowLocation {
            segment_id: self.memtable_segment_id,
            row_index: self.memtable_row_counter,
        };
        self.memtable_surrogates.push(surrogate);
        self.memtable_row_counter += 1;
        match sys_to {
            None => self.pk_index.upsert(pk_bytes, location),
            Some(to) => {
                wal_records.push(self.end_row(pk_bytes.clone(), location, to));
                if to == sys_from && self.schema.is_bitemporal() {
                    self.closed_index.upsert(pk_bytes, location);
                }
            }
        }
        Ok(MutationResult { wal_records })
    }

    /// Install every version of `rows` in order, or none of them.
    ///
    /// Each row follows [`Self::install_version`]. When a row fails, the
    /// rows this call already installed are undone: the memtable is cut
    /// back, each touched key's PK and closed-key bindings are restored, the
    /// rows the install ended are open again, and the error is returned.
    ///
    /// Returns one `MutationResult` per row, in order.
    pub fn install_versions<'a>(
        &mut self,
        rows: impl IntoIterator<Item = InstallRow<'a>>,
    ) -> Result<Vec<MutationResult>, ColumnarError> {
        let start = self.memtable.row_count();
        let mut priors: HashMap<Vec<u8>, PriorKey> = HashMap::new();
        let mut results = Vec::new();
        for row in rows {
            match self.install_batch_row(&row, &mut priors) {
                Ok(result) => results.push(result),
                Err(e) => {
                    self.undo_install(start, priors);
                    return Err(e);
                }
            }
        }
        Ok(results)
    }

    /// Install one batch row and record the bindings its key had before the
    /// batch first wrote it.
    fn install_batch_row(
        &mut self,
        row: &InstallRow<'_>,
        priors: &mut HashMap<Vec<u8>, PriorKey>,
    ) -> Result<MutationResult, ColumnarError> {
        let pk_bytes = self.extract_pk_bytes(row.values)?;
        let prior = PriorKey {
            bound: self.pk_index.get(&pk_bytes).copied(),
            closed: self.closed_index.get(&pk_bytes).copied(),
        };
        if row.clone_copy
            && (prior.bound.is_some()
                || prior.closed.is_some()
                || !self.chains.get(&pk_bytes).is_empty())
        {
            return Ok(MutationResult {
                wal_records: Vec::new(),
            });
        }
        let result = self.install_version(row.values, row.surrogate, row.sys_from, row.sys_to)?;
        priors.entry(pk_bytes).or_insert(prior);
        Ok(result)
    }

    /// Whether the memtable row at `loc` holds cells other than `values`. A
    /// row in a flushed segment is the one version its key holds at its
    /// ordinal, so it never differs.
    fn bound_cells_differ(
        &self,
        loc: RowLocation,
        values: &[Value],
    ) -> Result<bool, ColumnarError> {
        if loc.segment_id != self.memtable_segment_id {
            return Ok(false);
        }
        let Ok(row_idx) = usize::try_from(loc.row_index) else {
            return Ok(false);
        };
        let stored = self
            .memtable
            .get_row(row_idx)
            .map_err(|e| self.memtable_read_fault(e))?;
        Ok(stored.is_some_and(|cells| cells.as_slice() != values))
    }

    /// Undo the rows a failed batch install wrote from memtable row `start`
    /// on.
    fn undo_install(&mut self, start: usize, priors: HashMap<Vec<u8>, PriorKey>) {
        for (pk_bytes, prior) in priors {
            match prior.bound {
                Some(location) => {
                    self.reopen_row(&pk_bytes, location);
                    self.pk_index.upsert(pk_bytes.clone(), location);
                }
                None => {
                    self.pk_index.remove(&pk_bytes);
                }
            }
            match prior.closed {
                Some(location) => self.closed_index.upsert(pk_bytes, location),
                None => {
                    self.closed_index.remove(&pk_bytes);
                }
            }
        }
        self.cut_memtable_to(start);
    }
}

#[cfg(test)]
mod tests {
    use nodedb_types::columnar::{ColumnDef, ColumnType, ColumnarSchema};

    use super::*;
    use crate::pk_index::encode_pk;
    use crate::versions::VersionCut;

    fn schema() -> ColumnarSchema {
        ColumnarSchema::new(vec![
            ColumnDef::required("id", ColumnType::Int64).with_primary_key(),
            ColumnDef::required("name", ColumnType::String),
        ])
        .expect("valid")
    }

    fn row(id: i64, name: &str) -> Vec<Value> {
        vec![Value::Integer(id), Value::String(name.into())]
    }

    fn rows_at(engine: &MutationEngine, cut: VersionCut) -> Vec<Vec<Value>> {
        let mut rows: Vec<Vec<Value>> = engine
            .scan_memtable_versions(cut)
            .map(|r| r.map(|(_, _, values)| values))
            .collect::<Result<_, _>>()
            .expect("read");
        rows.sort_by_key(|r| format!("{r:?}"));
        rows
    }

    /// Every version of `engine`'s memtable as an install row, sorted by
    /// `sys_from`.
    fn history(engine: &MutationEngine) -> Vec<(Vec<Value>, i64, Option<i64>)> {
        let seg = engine.memtable_segment_id();
        let mut versions: Vec<(Vec<Value>, i64, Option<i64>)> = engine
            .scan_memtable_versions(VersionCut::All)
            .map(|r| {
                let (row, _, values) = r.expect("read");
                let loc = RowLocation {
                    segment_id: seg,
                    row_index: row,
                };
                let from = engine.sys_from_at(loc).expect("stamped");
                (values, from, engine.sys_to_at(loc))
            })
            .collect();
        versions.sort_by_key(|v| v.1);
        versions
    }

    fn install_all(engine: &mut MutationEngine, versions: &[(Vec<Value>, i64, Option<i64>)]) {
        engine
            .install_versions(versions.iter().map(|(values, from, to)| InstallRow {
                values,
                surrogate: None,
                sys_from: *from,
                sys_to: *to,
                clone_copy: false,
            }))
            .expect("install");
    }

    const CUTS: [i64; 12] = [9, 10, 15, 20, 24, 25, 29, 30, 35, 40, 45, 60];

    /// Put, overwrite, delete and re-insert across two keys: installing the
    /// versions in `sys_from` order gives the same rows at every cut, the
    /// same current row per key, and the same chains.
    #[test]
    fn installed_versions_reproduce_the_history() {
        let mut live = MutationEngine::new("t".into(), schema());
        live.insert(&row(1, "a"), 10).expect("put");
        live.insert(&row(2, "x"), 15).expect("put");
        live.insert(&row(1, "b"), 20).expect("overwrite");
        live.delete(&Value::Integer(2), 25).expect("delete");
        live.delete(&Value::Integer(1), 30).expect("delete");
        live.insert(&row(1, "c"), 40).expect("re-insert");
        live.insert(&row(2, "y"), 45).expect("re-insert");

        let mut restored = MutationEngine::new("t".into(), schema());
        install_all(&mut restored, &history(&live));

        for at in CUTS {
            assert_eq!(
                rows_at(&restored, VersionCut::At(at)),
                rows_at(&live, VersionCut::At(at)),
                "cut {at}"
            );
        }
        assert_eq!(
            rows_at(&restored, VersionCut::Current),
            rows_at(&live, VersionCut::Current)
        );
        assert_eq!(
            rows_at(&restored, VersionCut::All),
            rows_at(&live, VersionCut::All)
        );
        for id in [1, 2] {
            let pk = encode_pk(&Value::Integer(id));
            assert!(restored.pk_index().contains(&pk));
            assert_eq!(
                restored.version_chains().get(&pk).len(),
                live.version_chains().get(&pk).len()
            );
        }
        assert_eq!(history(&restored), history(&live));
        // A write after the restore stamps above the restored versions.
        restored.insert(&row(1, "d"), 41).expect("write");
        let bound = restored
            .pk_index()
            .get(&encode_pk(&Value::Integer(1)))
            .copied()
            .expect("bound");
        assert_eq!(restored.sys_from_at(bound), Some(41));
    }

    fn bitemporal_schema() -> ColumnarSchema {
        ColumnarSchema::new(vec![
            ColumnDef::required("_ts_system", ColumnType::Int64),
            ColumnDef::required("id", ColumnType::Int64).with_primary_key(),
            ColumnDef::required("v", ColumnType::Int64),
        ])
        .expect("valid")
    }

    /// A bitemporal close installs as a tombstone: the closed-key index
    /// binds it, the key is unbound, and every cut reads as before.
    #[test]
    fn an_installed_tombstone_closes_its_key() {
        let version =
            |ts: i64, v: i64| vec![Value::Integer(ts), Value::Integer(1), Value::Integer(v)];
        let mut live = MutationEngine::new("t".into(), bitemporal_schema());
        live.insert(&version(100, 10), 0).expect("insert");
        live.update(&Value::Integer(1), &version(200, 20), None, 0)
            .expect("update");
        live.close(&version(300, 20), None, 0).expect("close");

        let mut restored = MutationEngine::new("t".into(), bitemporal_schema());
        install_all(&mut restored, &history(&live));

        let pk = encode_pk(&Value::Integer(1));
        assert!(!restored.pk_index().contains(&pk));
        assert_eq!(
            restored.closed_location(&pk).map(|l| l.row_index),
            live.closed_location(&pk).map(|l| l.row_index)
        );
        for at in [99, 100, 150, 200, 299, 300, 400] {
            assert_eq!(
                rows_at(&restored, VersionCut::At(at)),
                rows_at(&live, VersionCut::At(at)),
                "cut {at}"
            );
        }
        assert_eq!(
            rows_at(&restored, VersionCut::All),
            rows_at(&live, VersionCut::All)
        );
    }

    /// Installing the same version again writes nothing, so a resumed copy
    /// is idempotent. Other cells at the same stamp are refused.
    #[test]
    fn a_repeated_install_of_one_version_writes_nothing() {
        let mut engine = MutationEngine::new("t".into(), schema());
        let once = row(1, "a");
        for _ in 0..2 {
            engine
                .install_versions([InstallRow {
                    values: &once,
                    surrogate: None,
                    sys_from: 10,
                    sys_to: None,
                    clone_copy: false,
                }])
                .expect("install");
        }
        assert_eq!(engine.memtable().row_count(), 1);
        assert!(engine.version_chains().is_empty());
        assert!(matches!(
            engine.install_version(&row(1, "other"), None, 10, None),
            Err(ColumnarError::InvalidVersionInterval { .. })
        ));
        assert_eq!(engine.memtable().row_count(), 1);
    }

    /// A clone copy of a key that holds a newer user version is skipped, and
    /// a copy of a key that holds no version installs at its stamp.
    #[test]
    fn a_clone_copy_installs_only_where_the_key_holds_no_version() {
        let mut engine = MutationEngine::new("t".into(), schema());
        engine.insert(&row(1, "user"), 90).expect("user write");
        let held = row(1, "source");
        let fresh = row(2, "source");
        engine
            .install_versions([
                InstallRow {
                    values: &held,
                    surrogate: None,
                    sys_from: 10,
                    sys_to: None,
                    clone_copy: true,
                },
                InstallRow {
                    values: &fresh,
                    surrogate: None,
                    sys_from: 10,
                    sys_to: None,
                    clone_copy: true,
                },
            ])
            .expect("install");
        assert_eq!(
            rows_at(&engine, VersionCut::Current),
            vec![row(1, "user"), row(2, "source")]
        );
        assert!(
            engine.version_chains().is_empty(),
            "the skipped copy ends nothing"
        );
        let bound = engine
            .pk_index()
            .get(&encode_pk(&Value::Integer(2)))
            .copied()
            .expect("bound");
        assert_eq!(engine.sys_from_at(bound), Some(10));
    }

    /// A version that ends before it starts is refused and changes nothing.
    /// A failed batch undoes the rows it installed.
    #[test]
    fn a_refused_install_leaves_the_engine_unchanged() {
        let mut engine = MutationEngine::new("t".into(), schema());
        engine.insert(&row(1, "a"), 10).expect("put");
        assert!(matches!(
            engine.install_version(&row(2, "x"), None, 20, Some(19)),
            Err(ColumnarError::InvalidVersionInterval { .. })
        ));
        assert!(matches!(
            engine.install_version(&row(1, "x"), None, 5, None),
            Err(ColumnarError::InvalidVersionInterval { .. })
        ));
        assert_eq!(engine.memtable().row_count(), 1);

        let good = row(1, "b");
        let ended = row(2, "y");
        let bad = vec![Value::Integer(3), Value::Null];
        let err = engine
            .install_versions([
                InstallRow {
                    values: &good,
                    surrogate: None,
                    sys_from: 20,
                    sys_to: None,
                    clone_copy: false,
                },
                InstallRow {
                    values: &ended,
                    surrogate: None,
                    sys_from: 21,
                    sys_to: Some(22),
                    clone_copy: false,
                },
                InstallRow {
                    values: &bad,
                    surrogate: None,
                    sys_from: 23,
                    sys_to: None,
                    clone_copy: false,
                },
            ])
            .unwrap_err();
        assert!(matches!(err, ColumnarError::NullViolation(_)));
        assert_eq!(rows_at(&engine, VersionCut::All), vec![row(1, "a")]);
        assert_eq!(engine.memtable().row_count(), 1);
        assert!(engine.version_chains().is_empty());
        let bound = engine
            .pk_index()
            .get(&encode_pk(&Value::Integer(1)))
            .copied();
        assert_eq!(bound.map(|l| l.row_index), Some(0));
        assert!(!engine.pk_index().contains(&encode_pk(&Value::Integer(2))));
    }

    /// A key's newest ordinal spans its bound row and its ended versions. A
    /// version installed as `[at, at)` ends the bound row at `at`, leaves
    /// the key unbound, and is visible at no cut: a RESTORE ends a key the
    /// backup holds absent this way.
    #[test]
    fn a_zero_width_install_ends_the_bound_row() {
        let mut engine = MutationEngine::new("t".into(), schema());
        let pk = encode_pk(&Value::Integer(1));
        assert_eq!(engine.key_newest_ordinal(&pk), (None, false));
        engine
            .install_version(&row(1, "a"), None, 10, None)
            .expect("install");
        assert_eq!(engine.key_newest_ordinal(&pk), (Some(10), true));
        engine
            .install_version(&row(1, "a"), None, 40, Some(40))
            .expect("end");
        assert_eq!(engine.key_newest_ordinal(&pk), (Some(40), false));
        assert_eq!(rows_at(&engine, VersionCut::At(20)), vec![row(1, "a")]);
        assert!(rows_at(&engine, VersionCut::At(40)).is_empty());
        assert!(rows_at(&engine, VersionCut::Current).is_empty());
    }
}
