// SPDX-License-Identifier: BUSL-1.1

//! The payload an installed Calvin slice answers with.

use super::reply::CalvinReply;
use crate::bridge::envelope::ErrorCode;
use crate::control::server::shared::sql::affected_payload::read_affected_fields;
use crate::data::executor::core_loop::CoreLoop;
use crate::data::executor::handlers::transaction::redo_apply::TsInstalled;
use crate::engine::timeseries::install_counts::TsInstallCounts;

impl CoreLoop {
    /// The payload `reply` answers with once the redo install has written
    /// the slice.
    ///
    /// `ts_installs` holds what each resolved timeseries batch of the
    /// install stored. A count reply answers their apply counts.
    pub(in crate::data::executor) fn calvin_reply_payload(
        &self,
        reply: CalvinReply,
        ts_installs: &[TsInstalled],
    ) -> Result<Vec<u8>, ErrorCode> {
        match reply {
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
                // The plans beside the ingests staged this count. A resolved
                // ingest stages none of its own.
                let plain = read_affected_fields(&payload).map_or(0, |fields| fields.affected);
                counts
                    .with_plain_count(plain)
                    .to_bytes()
                    .map_err(ErrorCode::from)
            }
            CalvinReply::Rows(payload) => Ok(payload),
            // The Control Plane answers the stored reply in place of this.
            CalvinReply::Frozen => Ok(Vec::new()),
        }
    }
}
