// SPDX-License-Identifier: BUSL-1.1

//! What a resolved timeseries install stored, as its readers need it.
//!
//! - A by-name install stores values its resolve did not see. The images of
//!   its landing rows are rendered before the first row lands, from a
//!   scratch memtable under the live schema, through the same row emitter a
//!   scan uses.
//! - A by-name install of a batch whose events some consumer reads writes
//!   its durable outcome (`install_outcome`) before the first row lands, so
//!   WAL catch-up rebuilds exactly the events the install emits.
//! - A committed redo install records its counts and landed images in the
//!   open apply scope, so the apply answers the counts and a Calvin install
//!   renders its `RETURNING` rows from the stored rows.

use crate::data::executor::core_loop::CoreLoop;
use crate::data::executor::handlers::transaction::redo_apply::{RedoApplyPass, TsInstalled};
use crate::engine::timeseries::columnar_memtable::{
    ColumnValue, ColumnarMemtable, ColumnarMemtableConfig, ColumnarSchema,
};
use crate::engine::timeseries::install_counts::TsInstallCount;
use crate::engine::timeseries::install_outcome::{
    TsInstallOutcome, batch_digest, outcome_dir, write_install_outcome,
};
use crate::engine::timeseries::resolved_ingest::ResolvedTsBatch;

use super::raw_scan::emit_memtable_rows_at;

impl CoreLoop {
    /// The image of each landing row of a by-name install, in row order, as
    /// a scan reads the row once stored under `schema`. `row_values` holds
    /// each row's values in `schema` order, `None` for a rejected row.
    pub(super) fn by_name_images(
        &self,
        schema: &ColumnarSchema,
        row_values: &[Option<Vec<ColumnValue>>],
    ) -> crate::Result<Vec<Vec<u8>>> {
        let mut scratch = ColumnarMemtable::new(
            schema.clone(),
            ColumnarMemtableConfig::from_tuning(&self.ts_tuning),
        );
        for values in row_values.iter().flatten() {
            scratch.ingest_row(0, values)?;
        }
        let indices: Vec<usize> = (0..row_values.iter().flatten().count()).collect();
        emit_memtable_rows_at(&scratch, &indices)?
            .iter()
            .map(|row| {
                let mut bytes = Vec::new();
                rmpv::encode::write_value(&mut bytes, row).map_err(|error| {
                    crate::Error::Serialization {
                        format: "msgpack".into(),
                        detail: format!("timeseries stored row image: {error}"),
                    }
                })?;
                Ok(bytes)
            })
            .collect()
    }

    /// Write the durable outcome of the by-name install of `batch` into
    /// `collection` by the record at `lsn`: the rows that land and their
    /// images.
    pub(super) fn write_ts_install_outcome(
        &self,
        lsn: u64,
        collection: &str,
        batch: &ResolvedTsBatch,
        row_values: &[Option<Vec<ColumnValue>>],
        images: &[Vec<u8>],
    ) -> crate::Result<()> {
        let landed = row_values
            .iter()
            .enumerate()
            .filter(|(_, values)| values.is_some())
            .filter_map(|(position, _)| u32::try_from(position).ok())
            .collect();
        write_install_outcome(
            &outcome_dir(&self.data_dir, self.core_id),
            lsn,
            TsInstallOutcome {
                collection: collection.to_string(),
                digest: batch_digest(batch)?,
                landed,
                images: images.to_vec(),
            },
        )
    }

    /// Record in the open install scope what one committed redo install of a
    /// resolved batch stored. Outside an install pass there is no scope to
    /// record in.
    pub(super) fn note_redo_ts_install(&mut self, count: TsInstallCount, images: Vec<Vec<u8>>) {
        if let Some(scope) = self.redo_apply.scope.as_mut()
            && scope.pass == RedoApplyPass::Install
        {
            scope.ts_installs.push(TsInstalled { count, images });
        }
    }
}
