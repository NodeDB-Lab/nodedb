// SPDX-License-Identifier: BUSL-1.1

//! What the install of a resolved timeseries ingest stores, known before it
//! installs.
//!
//! The install and the preview share every step that decides a landed row:
//! the schema fit, the schema the memtable evolves to, each row's landing
//! values, the system-time stamp, and the image each landed row records.
//! The preview runs them against the live memtable's schema.
//!
//! An earlier ingest of the same slice into the same collection evolves
//! that schema before this install runs. The preview need not apply it: the
//! later ingest resolved against the schema the earlier one resolved to, so
//! the earlier one's columns lead the later one's, and the live schema
//! evolved by both is the live schema evolved by the later one alone.
//!
//! Two inputs exist only at the install:
//! - the live schema, when a write outside the slice evolves it between
//!   the stage and the install;
//! - the system-time ordinal a by-name row stores, which the install takes
//!   from its log entry. Only a bitemporal row shows it.

use crate::data::executor::core_loop::CoreLoop;
use crate::engine::timeseries::columnar_memtable::{ColumnValue, ColumnarSchema};
use crate::engine::timeseries::resolved_ingest::ResolvedTsBatch;
use crate::types::{DatabaseId, TenantId};

use super::ingest_dispatch::TimeseriesApplyMode;
use super::ingest_resolved_fit::{CollKey, SchemaFit, fit_against, landing_values};

/// Stamp every landing row's system-time column with `system_ordinal`. A
/// clone copy keeps the ordinal each row carries. Returns the carried
/// ordinals, which the install folds into the shared clock.
pub(super) fn stamp_system_ordinal(
    schema: &ColumnarSchema,
    carried_system: bool,
    row_values: &mut [Option<Vec<ColumnValue>>],
    system_ordinal: i64,
) -> Vec<i64> {
    let mut carried = Vec::new();
    let Some(system_idx) = schema.ts_system_idx() else {
        return carried;
    };
    for values in row_values.iter_mut().flatten() {
        let Some(cell) = values.get_mut(system_idx) else {
            continue;
        };
        if carried_system && let ColumnValue::Int64(ordinal) = *cell {
            carried.push(ordinal);
            continue;
        }
        *cell = ColumnValue::Int64(system_ordinal);
    }
    carried
}

/// The image of each landed row, in row order. An exact fit stored the
/// values its resolve imaged. A by-name fit's images are `by_name`, rendered
/// from the live schema.
pub(super) fn landed_images<'a>(
    batch: &'a ResolvedTsBatch,
    landed: &[usize],
    by_name: Option<&'a [Vec<u8>]>,
) -> Vec<&'a [u8]> {
    match by_name {
        None => landed
            .iter()
            .filter_map(|position| batch.rows.get(*position))
            .map(|row| row.image.as_slice())
            .collect(),
        Some(rendered) => rendered.iter().map(Vec::as_slice).collect(),
    }
}

impl CoreLoop {
    /// The image of each row the committed-redo install of the resolved
    /// batch `payload` into `collection` lands, in row order, as a scan reads
    /// it. Changes nothing.
    pub(in crate::data::executor) fn preview_resolved_ingest_images(
        &self,
        database_id: DatabaseId,
        tid: TenantId,
        collection: &str,
        payload: &[u8],
    ) -> crate::Result<Vec<Vec<u8>>> {
        let batch = ResolvedTsBatch::from_bytes(payload)?;
        let key: CollKey = (database_id, tid, collection.to_string());
        let live = self.columnar_memtables.get(&key).map(|mt| mt.schema());
        let fit = fit_against(live, &batch);
        let mut schema = match live {
            Some(live) => live.clone(),
            None => self.resolved_memtable_schema(&key, &batch),
        };
        for (name, column_type) in &batch.columns {
            schema.evolve(name.clone(), *column_type);
        }
        let mut row_values = landing_values(fit, &schema, &batch);
        let system_ordinal = self.ts_batch_ordinal(TimeseriesApplyMode::RedoInstall, batch.now_ms);
        stamp_system_ordinal(
            &schema,
            batch.carried_system,
            &mut row_values,
            system_ordinal,
        );
        let landed: Vec<usize> = row_values
            .iter()
            .enumerate()
            .filter(|(_, values)| values.is_some())
            .map(|(position, _)| position)
            .collect();
        let by_name = match fit {
            SchemaFit::Exact => None,
            SchemaFit::ByName => Some(self.by_name_images(&schema, &row_values)?),
        };
        // The install records only the images a row carries.
        Ok(landed_images(&batch, &landed, by_name.as_deref())
            .into_iter()
            .filter(|image| !image.is_empty())
            .map(<[u8]>::to_vec)
            .collect())
    }
}

#[cfg(test)]
mod tests {
    use super::super::ingest_resolved_test_support::{
        drained_images, install, install_committed, key, resolve, stored_row, wire_events,
    };
    use crate::bridge::envelope::{ErrorCode, Status};
    use crate::data::executor::core_loop::tests::make_core_with_dir;
    use crate::engine::timeseries::resolved_ingest::TsDriftPolicy;

    /// Two ingests resolve against the same schema and add different
    /// columns. The first installs. The second finds the schema changed,
    /// stores nothing and refuses, then resolves again and installs. Each
    /// ingest's event equals the row it stored.
    #[test]
    fn interleaved_ingests_that_add_columns_emit_their_stored_rows() {
        let dir = tempfile::tempdir().expect("tempdir");
        let (mut core, _tx, _rx) = make_core_with_dir(dir.path());
        let mut events = wire_events(&mut core);

        let first = resolve(&core, "metrics alpha=1 1000000000", 1_000);
        let second = resolve(&core, "metrics beta=2 2000000000", 2_000);

        assert_eq!(install(&mut core, &first, 1).status, Status::Ok);
        assert_eq!(drained_images(&mut events), vec![stored_row(&core, 0)]);

        let refused = install(&mut core, &second, 2);
        assert_eq!(refused.status, Status::Error);
        assert_eq!(
            refused.error_code.as_deref(),
            Some(&ErrorCode::OllpRetryRequired)
        );
        assert!(
            drained_images(&mut events).is_empty(),
            "a refusal stores and emits nothing"
        );
        assert_eq!(
            core.columnar_memtables.get(&key()).map(|mt| mt.row_count()),
            Some(1)
        );

        let second = resolve(&core, "metrics beta=2 2000000000", 2_000);
        assert_eq!(install(&mut core, &second, 3).status, Status::Ok);
        assert_eq!(drained_images(&mut events), vec![stored_row(&core, 1)]);
    }

    /// A committed apply cannot refuse, whatever drift its batch names: it
    /// stores each value under its column name, and a column the batch does
    /// not name takes its empty value.
    #[test]
    fn an_apply_by_name_install_stores_values_under_their_columns() {
        let dir = tempfile::tempdir().expect("tempdir");
        let (mut core, _tx, _rx) = make_core_with_dir(dir.path());

        let first = resolve(&core, "metrics alpha=1 1000000000", 1_000);
        let second = resolve(&core, "metrics beta=2 2000000000", 2_000);
        let by_name = second.clone().with_drift(TsDriftPolicy::ApplyByName);
        assert_eq!(install(&mut core, &first, 1).status, Status::Ok);
        assert_eq!(install_committed(&mut core, &second, 2).status, Status::Ok);
        assert_eq!(install(&mut core, &by_name, 3).status, Status::Ok);
        assert_eq!(
            core.columnar_memtables.get(&key()).map(|mt| mt.row_count()),
            Some(3)
        );

        let rmpv::Value::Map(fields) = stored_row(&core, 1) else {
            panic!("a stored row is a map");
        };
        let field = |name: &str| {
            fields
                .iter()
                .find(|(key, _)| key.as_str() == Some(name))
                .map(|(_, value)| value.clone())
        };
        assert_eq!(field("beta"), Some(rmpv::Value::F64(2.0)));
        assert!(
            matches!(field("alpha"), Some(rmpv::Value::Nil)),
            "a column the batch does not name is empty"
        );
    }
}
