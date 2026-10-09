// SPDX-License-Identifier: BUSL-1.1

//! The payload an installed Calvin slice answers with.

use super::images::{RowLocation, StoredRow};
use super::reply::{CalvinReply, InstalledTimeseries};
use crate::bridge::envelope::ErrorCode;
use crate::data::executor::core_loop::CoreLoop;
use crate::data::executor::handlers::returning_rows::build_rejecting_rows_payload;
use crate::data::executor::handlers::transaction::redo_apply::TsInstalled;
use crate::data::executor::response_codec::IngestRejection;
use crate::data::executor::task::ExecutionTask;
use crate::engine::timeseries::install_counts::TsInstallCounts;
use crate::util::rmpv_value::rmpv_to_value;

impl CoreLoop {
    /// The payload `reply` answers with once the redo install has written
    /// the slice. Post-images are read from base after the install, so each row is what
    /// the install stored.
    ///
    /// A row the plan wrote that base does not hold after the install is an
    /// install that dropped a write, and the reply refuses it.
    ///
    /// `ts_installs` holds what each resolved timeseries batch of the
    /// install stored. A count reply answers their apply counts, and a
    /// timeseries `RETURNING` reply renders the rows they stored.
    pub(in crate::data::executor) fn calvin_reply_payload(
        &self,
        task: &ExecutionTask,
        tid: u64,
        reply: CalvinReply,
        ts_installs: &[TsInstalled],
    ) -> Result<Vec<u8>, ErrorCode> {
        let images = match reply {
            CalvinReply::Count(payload) => {
                // A resolved timeseries install answers its apply counts: the
                // rows it stored and the lines and rows it rejected.
                let counts = TsInstallCounts::new(
                    ts_installs
                        .iter()
                        .map(|installed| installed.count.clone())
                        .collect(),
                );
                if counts.is_empty() {
                    return Ok(payload);
                }
                return counts.to_bytes().map_err(ErrorCode::from);
            }
            CalvinReply::Rows(payload) => return Ok(payload),
            CalvinReply::InstalledTimeseries(installed) => {
                return installed_timeseries_rows(&installed, ts_installs);
            }
            CalvinReply::PostImages(images) => images,
        };
        let at = RowLocation {
            engine: images.engine,
            database_id: task.request.database_id.as_u64(),
            tid,
            collection: &images.collection,
        };
        let mut rows = Vec::with_capacity(images.rows.len());
        for (identity, surrogate) in &images.rows {
            let Some(bytes) = self.calvin_base_row(&at, identity, *surrogate)? else {
                return Err(ErrorCode::Internal {
                    detail: format!(
                        "calvin RETURNING: row '{}' of '{}' is absent after the install that \
                         wrote it",
                        identity.as_str(),
                        images.collection
                    ),
                });
            };
            rows.push(StoredRow {
                identity: identity.clone(),
                surrogate: *surrogate,
                bytes,
            });
        }
        self.calvin_render_rows(&at, &images.spec, &images.rls_filters, &rows)
    }
}

/// The `RETURNING` rows of `installed`'s own install: the rows it stored,
/// with the lines and rows it rejected reported beside them.
fn installed_timeseries_rows(
    installed: &InstalledTimeseries,
    ts_installs: &[TsInstalled],
) -> Result<Vec<u8>, ErrorCode> {
    let Some(install) = ts_installs
        .get(installed.ordinal)
        .filter(|install| install.count.collection == installed.collection)
    else {
        return Err(ErrorCode::Internal {
            detail: format!(
                "calvin RETURNING: the install of timeseries ingest {} into '{}' is absent \
                 from the installed record",
                installed.ordinal, installed.collection
            ),
        });
    };
    let docs = install
        .images
        .iter()
        .map(|image| {
            crate::util::bounded_msgpack::read_value(image)
                .map(|row| rmpv_to_value(&row))
                .map_err(|e| ErrorCode::Internal {
                    detail: format!("calvin RETURNING: invalid stored row image: {e}"),
                })
        })
        .collect::<Result<Vec<_>, ErrorCode>>()?;
    let rejected = install.count.rejected;
    let rejection = (rejected > 0).then(|| IngestRejection {
        collection: installed.collection.clone(),
        lines: rejected,
    });
    build_rejecting_rows_payload(&installed.spec, &installed.rls_filters, &docs, rejection)
        .map_err(ErrorCode::from)
}
