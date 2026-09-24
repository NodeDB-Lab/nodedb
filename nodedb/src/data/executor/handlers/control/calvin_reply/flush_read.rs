// SPDX-License-Identifier: BUSL-1.1

//! The payload a flushed Calvin transaction answers with.

use super::images::{RowLocation, StoredRow};
use super::reply::CalvinReply;
use crate::bridge::envelope::ErrorCode;
use crate::data::executor::core_loop::CoreLoop;
use crate::data::executor::task::ExecutionTask;

impl CoreLoop {
    /// The payload `reply` answers with once the flush has installed the
    /// transaction. Post-images are read from base now, so each row is what
    /// the install stored.
    ///
    /// A row the plan wrote that base does not hold after the install is an
    /// install that dropped a write, and the reply refuses it.
    pub(in crate::data::executor) fn calvin_reply_payload(
        &self,
        task: &ExecutionTask,
        tid: u64,
        reply: CalvinReply,
    ) -> Result<Vec<u8>, ErrorCode> {
        let images = match reply {
            CalvinReply::Count(payload) | CalvinReply::Rows(payload) => return Ok(payload),
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
