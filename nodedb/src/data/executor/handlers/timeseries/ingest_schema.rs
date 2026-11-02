// SPDX-License-Identifier: BUSL-1.1

//! Resolving the schema a timeseries memtable is created with.
//!
//! Split from the ingest handler because this is where a declared collection
//! can silently become an inferred one — a distinct concern from ingesting
//! rows, and the one that decides what every later read of the collection is
//! shaped like.

use crate::engine::timeseries::columnar_memtable::ColumnType;
use crate::engine::timeseries::ilp;
use crate::engine::timeseries::ilp_ingest;

use crate::data::executor::core_loop::CoreLoop;

impl CoreLoop {
    /// Schema for a collection's very first memtable.
    ///
    /// A collection created through DDL declares its columns and its
    /// `TIME_KEY`; that declaration is the schema, so the time key keeps its
    /// name and position and every declared column exists from the first row
    /// on. A tag or field the declaration does not name follows the declared
    /// columns, as `evolve_schema` adds it to an existing memtable. Without
    /// it, the first batch into a declared collection drops every undeclared
    /// value. Only a collection with no declaration — raw ILP protocol ingest
    /// into a measurement that was never created — falls back to inferring a
    /// shape from the batch itself.
    ///
    /// That fallback is lossy in more ways than the one usually noticed, so it
    /// must stay reachable ONLY for genuinely undeclared measurements. Against
    /// a declared collection it silently substitutes:
    ///
    /// - **the time key's NAME** — `infer_schema` hard-names it `timestamp`, so
    ///   a collection declaring `ts TIMESTAMP TIME_KEY` stores the value
    ///   correctly and projects NULL under `ts`. This is the visible symptom
    ///   and the only one a column-by-column comparison catches.
    /// - **column ORDER** — inferred order is time key, then tags, then fields
    ///   in first-seen order, discarding any declared interleaving.
    /// - **column TYPES** — inference maps string fields to `Symbol`, ints to
    ///   `Int64`, and both floats and bools to `Float64`. A declared
    ///   `TIMESTAMP` column carrying a numeric field becomes `Int64`, and a
    ///   `BOOL` collapses to `Float64`. Same class as the columnar engine's
    ///   `Geometry`-degraded-to-`String`, which `load_columnar_schema_seed`
    ///   exists to prevent.
    /// - **bitemporal column POSITIONS** — the reserved columns are appended to
    ///   an inferred base rather than sitting where the declaration put them.
    ///
    /// A collection whose declaration happens to match what inference produces
    /// — string tags, float fields, time key first — looks correct afterwards,
    /// which is why a break here can go unnoticed for a long time.
    pub(super) fn initial_ts_schema(
        &self,
        task: &crate::data::executor::task::ExecutionTask,
        tid: crate::types::TenantId,
        collection: &str,
        lines: &[ilp::IlpLine<'_>],
    ) -> crate::engine::timeseries::columnar_memtable::ColumnarSchema {
        if let Some(mut schema) =
            self.declared_ts_memtable_schema(task.request.database_id, tid, collection)
        {
            extend_with_undeclared(&mut schema, lines);
            return schema;
        }
        // Falling back is correct for an undeclared measurement and a silent
        // data-shape change for a declared one, and nothing downstream can tell
        // the two apart afterwards — the collection simply reads back with
        // different column names and types. Say so once, at the only point that
        // still knows, and say WHICH of the three ways the declaration failed to
        // resolve so it does not have to be reconstructed from a symptom later.
        let key = (task.request.database_id, tid, collection.to_string());
        let entry = self.doc_configs.get(&key);
        let reason = match entry {
            None if self.doc_configs.is_empty() => "doc_configs is empty for this core",
            None => "no doc_configs entry under this (database, tenant, collection) key",
            Some(config) if config.timeseries.is_none() => {
                "the registered config carries no timeseries declaration"
            }
            Some(_) => "the declaration is present but its TIME_KEY names no declared column",
        };
        tracing::warn!(
            collection,
            database_id = task.request.database_id.as_u64(),
            tenant_id = tid.as_u64(),
            doc_config_entries = self.doc_configs.len(),
            reason,
            "timeseries schema inferred from the batch: the declared column names, order and \
             types are NOT in effect for this collection"
        );
        ilp_ingest::infer_schema(lines)
    }

    /// Names of the symbol columns `lines` resolve against once the ingest
    /// has created or evolved the memtable. Reads live state only.
    pub(super) fn ts_symbol_columns_for_ingest(
        &self,
        database_id: crate::types::DatabaseId,
        tid: crate::types::TenantId,
        collection: &str,
        lines: &[ilp::IlpLine<'_>],
    ) -> Vec<String> {
        let key = (database_id, tid, collection.to_string());
        let columns = match self.columnar_memtables.get(&key) {
            Some(memtable) => {
                let schema = memtable.schema();
                let mut columns = schema.columns.clone();
                columns.extend(ilp_ingest::new_columns(schema, lines));
                columns
            }
            None => match self.declared_ts_memtable_schema(database_id, tid, collection) {
                Some(mut schema) => {
                    extend_with_undeclared(&mut schema, lines);
                    schema.columns
                }
                None => ilp_ingest::infer_schema(lines).columns,
            },
        };
        columns
            .into_iter()
            .filter(|(_, col_type)| *col_type == ColumnType::Symbol)
            .map(|(name, _)| name)
            .collect()
    }
}

/// Append to a declared `schema` the columns `lines` name that it does not
/// hold, typed and ordered as `evolve_schema` adds them to a live memtable.
fn extend_with_undeclared(
    schema: &mut crate::engine::timeseries::columnar_memtable::ColumnarSchema,
    lines: &[ilp::IlpLine<'_>],
) {
    for (name, column_type) in ilp_ingest::new_columns(schema, lines) {
        schema.columns.push((name, column_type));
        schema.codecs.push(nodedb_codec::ColumnCodec::Auto);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::engine::timeseries::columnar_memtable::{ColumnarSchema, TimeKind};

    #[test]
    fn a_declared_schema_takes_the_undeclared_columns_of_its_first_batch() {
        let mut schema = ColumnarSchema {
            columns: vec![("ts".into(), ColumnType::Timestamp(TimeKind::Millis))],
            timestamp_idx: 0,
            codecs: vec![nodedb_codec::ColumnCodec::Auto],
        };
        let parsed = ilp::parse_batch("m,host=a value=\"a\",n=2i 1000000000")
            .expect("parse line")
            .into_lines();
        extend_with_undeclared(&mut schema, &parsed);
        assert_eq!(
            schema.columns,
            vec![
                ("ts".into(), ColumnType::Timestamp(TimeKind::Millis)),
                ("host".into(), ColumnType::Symbol),
                // A string field is text, never a series dimension.
                ("value".into(), ColumnType::Text),
                ("n".into(), ColumnType::Int64),
            ]
        );
        assert_eq!(schema.codecs.len(), schema.columns.len());
    }
}
