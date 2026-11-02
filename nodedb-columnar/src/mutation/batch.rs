// SPDX-License-Identifier: Apache-2.0

//! All-or-nothing multi-row insert.

use std::collections::HashMap;

use nodedb_types::surrogate::Surrogate;
use nodedb_types::value::Value;

use crate::error::ColumnarError;
use crate::pk_index::RowLocation;

use super::engine::{MutationEngine, MutationResult};

/// One row of a batch insert.
pub struct BatchRow<'a> {
    pub values: &'a [Value],
    pub surrogate: Option<Surrogate>,
    /// The system-time ordinal the row's write takes.
    pub stamp: i64,
}

/// What a batch insert does with a row whose primary key is already bound.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BatchConflict {
    /// Replace the bound row, as [`MutationEngine::insert`] does.
    Upsert,
    /// Skip the row, as [`MutationEngine::insert_if_absent`] does.
    Skip,
}

/// The PK binding a batch found before it first wrote that PK.
struct PriorBinding {
    location: Option<RowLocation>,
    /// Whether the batch ended `location`.
    ended: bool,
}

impl MutationEngine {
    /// Insert every row of `rows`, or none of them.
    ///
    /// Each row follows the single-row rules for `conflict`. When a row
    /// fails, the rows this call already wrote are undone: the memtable is
    /// cut back, their PK bindings are restored, the rows they replaced are
    /// open again, and the error is returned.
    ///
    /// Returns one `MutationResult` per row, in order. A skipped row has an
    /// empty `wal_records`.
    pub fn insert_batch<'a>(
        &mut self,
        rows: impl IntoIterator<Item = BatchRow<'a>>,
        conflict: BatchConflict,
    ) -> Result<Vec<MutationResult>, ColumnarError> {
        let start = self.memtable.row_count();
        let mut priors: HashMap<Vec<u8>, PriorBinding> = HashMap::new();
        let mut results = Vec::new();
        for row in rows {
            match self.insert_batch_row(&row, conflict, &mut priors) {
                Ok(result) => results.push(result),
                Err(e) => {
                    self.undo_batch(start, priors);
                    return Err(e);
                }
            }
        }
        Ok(results)
    }

    /// Write one batch row and record the PK binding it replaced.
    fn insert_batch_row(
        &mut self,
        row: &BatchRow<'_>,
        conflict: BatchConflict,
        priors: &mut HashMap<Vec<u8>, PriorBinding>,
    ) -> Result<MutationResult, ColumnarError> {
        let pk_bytes = self.extract_pk_bytes(row.values)?;
        let prior = self.pk_index.get(&pk_bytes).copied();
        if conflict == BatchConflict::Skip && prior.is_some() {
            return Ok(MutationResult {
                wal_records: Vec::new(),
            });
        }
        let sys_from = self.version_stamp(row.values, row.stamp, &[pk_bytes.as_slice()]);
        let replaced = prior.map(|loc| (pk_bytes.clone(), loc));

        let mut wal_records = Vec::with_capacity(2);
        let key = pk_bytes.clone();
        self.commit_row(
            row.values,
            pk_bytes,
            row.surrogate,
            replaced.as_slice(),
            sys_from,
            &mut wal_records,
        )?;
        priors.entry(key).or_insert(PriorBinding {
            location: prior,
            ended: prior.is_some(),
        });
        Ok(MutationResult { wal_records })
    }

    /// Undo the rows a failed batch wrote from memtable row `start` on.
    fn undo_batch(&mut self, start: usize, priors: HashMap<Vec<u8>, PriorBinding>) {
        for (pk_bytes, prior) in priors {
            match prior.location {
                Some(location) => {
                    if prior.ended {
                        self.reopen_row(&pk_bytes, location);
                    }
                    self.pk_index.upsert(pk_bytes, location);
                }
                None => {
                    self.pk_index.remove(&pk_bytes);
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

    fn batch(rows: &[Vec<Value>]) -> Vec<BatchRow<'_>> {
        rows.iter()
            .map(|values| BatchRow {
                values,
                surrogate: None,
                stamp: 50,
            })
            .collect()
    }

    #[test]
    fn failed_batch_applies_nothing() {
        let mut engine = MutationEngine::new("t".into(), schema());
        engine.insert(&row(1, "a"), 10).expect("insert");

        let rows = vec![
            row(1, "a2"),
            row(2, "b"),
            row(2, "b2"),
            vec![Value::Integer(3), Value::Null],
        ];
        let err = engine
            .insert_batch(batch(&rows), BatchConflict::Upsert)
            .unwrap_err();
        assert!(matches!(err, ColumnarError::NullViolation(_)));

        let live: Vec<Vec<Value>> = engine
            .scan_memtable_rows()
            .collect::<Result<_, _>>()
            .expect("read");
        assert_eq!(live, vec![row(1, "a")]);
        assert_eq!(engine.memtable().row_count(), 1);
        assert_eq!(engine.memtable_surrogates().len(), 1);
        assert_eq!(engine.pk_index().len(), 1);
        let loc = engine
            .pk_index()
            .get(&encode_pk(&Value::Integer(1)))
            .copied();
        assert_eq!(loc.map(|l| l.row_index), Some(0));
        assert!(engine.version_chains().is_empty(), "the undo reopens row 1");
        assert_eq!(engine.segment_versions(0).map(|v| v.len()), Some(1));

        // An index cut by the undo starts live when a new row lands on it.
        engine.insert(&row(4, "d"), 60).expect("insert after undo");
        engine.insert(&row(5, "e"), 60).expect("insert after undo");
        let live: Vec<Vec<Value>> = engine
            .scan_memtable_rows()
            .collect::<Result<_, _>>()
            .expect("read");
        assert_eq!(live, vec![row(1, "a"), row(4, "d"), row(5, "e")]);
    }

    #[test]
    fn batch_upserts_and_skips_like_single_rows() {
        let mut engine = MutationEngine::new("t".into(), schema());
        engine.insert(&row(1, "a"), 10).expect("insert");

        let rows = vec![row(1, "a2"), row(2, "b")];
        let results = engine
            .insert_batch(batch(&rows), BatchConflict::Upsert)
            .expect("batch");
        assert_eq!(results.len(), 2);
        let live: Vec<Vec<Value>> = engine
            .scan_memtable_rows()
            .collect::<Result<_, _>>()
            .expect("read");
        assert_eq!(live, vec![row(1, "a2"), row(2, "b")]);

        let rows = vec![row(2, "b2"), row(3, "c")];
        let results = engine
            .insert_batch(batch(&rows), BatchConflict::Skip)
            .expect("batch");
        assert!(results[0].wal_records.is_empty());
        assert_eq!(results[1].wal_records.len(), 1);
        let live: Vec<Vec<Value>> = engine
            .scan_memtable_rows()
            .collect::<Result<_, _>>()
            .expect("read");
        assert_eq!(live, vec![row(1, "a2"), row(2, "b"), row(3, "c")]);
    }
}
