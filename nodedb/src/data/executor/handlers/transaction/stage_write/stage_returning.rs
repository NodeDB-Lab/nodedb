// SPDX-License-Identifier: BUSL-1.1

//! `RETURNING` on a staged Document write.
//!
//! A staged write whose plan carries a spec answers with its affected count
//! and its rows projected per the spec, as one [`StagedReturningReply`]. A
//! write's rows are the images it staged: the post-image an insert, put,
//! update or upsert staged, and the pre-image a delete removed. The
//! projection is the one the autocommit handlers build
//! ([`build_stored_rows_payload`]), gated by the plan's read filters, so a
//! row a trigger's BEFORE body rewrote returns as it was staged.

use nodedb_physical::physical_plan::ReturningSpec;

use super::context::StageCtx;
use crate::bridge::envelope::{ErrorCode, Response, Status};
use crate::data::executor::core_loop::CoreLoop;
use crate::data::executor::handlers::returning_rows::{StoredRow, build_stored_rows_payload};
use crate::data::executor::response_codec::StagedReturningReply;
use crate::data::executor::task::ExecutionTask;

/// A plan's `RETURNING` spec and the read filters gating its rows.
#[derive(Clone, Copy)]
pub(in crate::data::executor) struct StageReturning<'a> {
    pub spec: &'a ReturningSpec,
    pub rls_filters: &'a [u8],
}

impl<'a> StageReturning<'a> {
    /// The plan's spec, `None` when the plan carries no `RETURNING`.
    pub(in crate::data::executor) fn of(
        spec: &'a Option<ReturningSpec>,
        rls_filters: &'a [u8],
    ) -> Option<Self> {
        spec.as_ref().map(|spec| Self { spec, rls_filters })
    }
}

/// Which image of a staged row `RETURNING` projects.
#[derive(Clone, Copy, PartialEq, Eq)]
pub(super) enum StagedImage {
    /// The row the write staged.
    After,
    /// The row as it stood before the write: a delete's.
    Before,
}

/// Where a set of staged rows lives, for decoding them.
pub(super) struct StagedRowsOf<'a> {
    pub task: &'a ExecutionTask,
    pub database_id: u64,
    pub tid: u64,
    pub collection: &'a str,
}

impl CoreLoop {
    /// Run the point write `stage` on `ctx` and, when `returning` is set,
    /// answer with the row it staged projected per the spec.
    pub(super) fn stage_point_returning(
        &mut self,
        ctx: &StageCtx<'_>,
        returning: Option<StageReturning<'_>>,
        image: StagedImage,
        stage: impl FnOnce(&mut Self, &StageCtx<'_>) -> Response,
    ) -> Response {
        let Some(returning) = returning else {
            return stage(self, ctx);
        };
        let before = match image {
            StagedImage::Before => match self.stage_current_body(ctx) {
                Ok(body) => body,
                Err(e) => return self.response_error(ctx.task, e),
            },
            StagedImage::After => None,
        };
        let response = stage(self, ctx);
        if response.status != Status::Ok {
            return response;
        }
        let affected = match staged_affected(&response) {
            Ok(affected) => affected,
            Err(e) => return self.response_error(ctx.task, e),
        };
        let row = if affected == 0 {
            None
        } else {
            match image {
                StagedImage::Before => before,
                StagedImage::After => match self.stage_current_body(ctx) {
                    Ok(body) => body,
                    Err(e) => return self.response_error(ctx.task, e),
                },
            }
        };
        let rows: Vec<StoredRow<'_>> = row
            .as_deref()
            .map(|body| (&ctx.document_id, body))
            .into_iter()
            .collect();
        self.staged_returning_response(
            StagedRowsOf {
                task: ctx.task,
                database_id: ctx.database_id,
                tid: ctx.tid,
                collection: ctx.collection,
            },
            returning,
            affected,
            &rows,
        )
    }

    /// Answer a staged point write whose key is unbound in this database: it
    /// matches no row, so it stages nothing and reports zero rows.
    pub(super) fn stage_point_matched_nothing(
        &self,
        task: &ExecutionTask,
        tid: u64,
        collection: &str,
        returning: Option<StageReturning<'_>>,
    ) -> Response {
        let Some(returning) = returning else {
            return self.stage_count_response(task, 0);
        };
        self.staged_returning_response(
            StagedRowsOf {
                task,
                database_id: task.request.database_id.as_u64(),
                tid,
                collection,
            },
            returning,
            0,
            &[],
        )
    }

    /// Answer a staged write with its `affected` count and `rows` projected
    /// per `returning`.
    pub(super) fn staged_returning_response(
        &self,
        of: StagedRowsOf<'_>,
        returning: StageReturning<'_>,
        affected: u64,
        rows: &[StoredRow<'_>],
    ) -> Response {
        let schema = self.resolve_strict_schema(of.database_id, of.tid, of.collection);
        let identity_column = self.identity_column(of.database_id, of.tid, of.collection);
        let reply = build_stored_rows_payload(
            returning.spec,
            returning.rls_filters,
            schema.as_ref(),
            &identity_column,
            rows,
            &self.crs_context(),
        )
        .and_then(|rows| {
            zerompk::to_msgpack_vec(&StagedReturningReply { affected, rows }).map_err(|error| {
                crate::Error::Codec {
                    detail: format!("staged RETURNING reply: {error}"),
                }
            })
        });
        match reply {
            Ok(payload) => self.response_with_payload(of.task, payload),
            Err(e) => self.response_error(
                of.task,
                ErrorCode::Internal {
                    detail: format!("RETURNING encode: {e}"),
                },
            ),
        }
    }
}

/// The affected count a staged write's count reply reports.
fn staged_affected(response: &Response) -> crate::Result<u64> {
    let reply = nodedb_types::value_from_msgpack(response.payload.as_ref()).map_err(|error| {
        crate::Error::Codec {
            detail: format!("staged write reply: {error}"),
        }
    })?;
    match reply {
        nodedb_types::Value::Object(fields) => match fields.get("affected") {
            Some(nodedb_types::Value::Integer(n)) if *n >= 0 => Ok(*n as u64),
            _ => Err(crate::Error::Internal {
                detail: "a staged write's reply carries no affected count".into(),
            }),
        },
        _ => Err(crate::Error::Internal {
            detail: "a staged write's reply is not a map".into(),
        }),
    }
}
