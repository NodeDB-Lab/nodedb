// SPDX-License-Identifier: BUSL-1.1

//! The reply and the staged images of a resolved timeseries ingest: the
//! rows its install stores and the lines it rejects, known when the plan
//! stages.

use nodedb_physical::physical_plan::{PhysicalPlan, TimeseriesOp};
use nodedb_types::Value;

use super::staged_images::{PlanRows, plan_images};
use super::target::ReturningTarget;
use crate::bridge::envelope::ErrorCode;
use crate::data::executor::core_loop::CoreLoop;
use crate::data::executor::handlers::returning_rows::build_rejecting_rows_payload;
use crate::data::executor::response_codec::{IngestRejection, StagedPlanImages};
use crate::data::executor::task::ExecutionTask;
use crate::types::TenantId;
use crate::util::rmpv_value::rmpv_to_value;

/// The documents of stored timeseries row images, in order.
pub(super) fn stored_ts_docs(images: &[Vec<u8>]) -> Result<Vec<Value>, ErrorCode> {
    images
        .iter()
        .map(|image| {
            crate::util::bounded_msgpack::read_value(image)
                .map(|row| rmpv_to_value(&row))
                .map_err(|e| ErrorCode::Internal {
                    detail: format!("calvin RETURNING: invalid stored row image: {e}"),
                })
        })
        .collect()
}

/// What staging a resolved ingest's `RETURNING` decides: its reply and
/// its images.
pub(super) struct ResolvedIngestReturning {
    /// The rendered rows, with the rejected lines reported beside them.
    pub reply: Vec<u8>,
    pub images: StagedPlanImages,
}

impl CoreLoop {
    /// The reply and the images of the resolved ingest `plan` at slice index
    /// `index`: the rows its install lands, each keyed by its ordinal among
    /// them, and the lines it rejects.
    ///
    /// The install lands exactly these rows. A Calvin ingest locks its
    /// collection whole `Exclusive` from grant to install, and a gated append
    /// takes the collection `Intent`. So no other write changes the
    /// collection's schema or rows between this stage and the install.
    pub(super) fn calvin_resolved_ingest_returning(
        &self,
        task: &ExecutionTask,
        tid: u64,
        plan: &PhysicalPlan,
        target: &ReturningTarget<'_>,
        index: u32,
    ) -> Result<ResolvedIngestReturning, ErrorCode> {
        let PhysicalPlan::Timeseries(TimeseriesOp::Ingest {
            collection,
            payload,
            ..
        }) = plan
        else {
            return Err(ErrorCode::Internal {
                detail: "calvin RETURNING: a resolved ingest reply target is not an ingest".into(),
            });
        };
        let preview = self
            .preview_resolved_ingest_images(
                task.request.database_id,
                TenantId::new(tid),
                collection.as_str(),
                payload,
            )
            .map_err(ErrorCode::from)?;
        let docs = stored_ts_docs(&preview.images)?;
        let rejection = (preview.rejected > 0).then(|| IngestRejection {
            collection: target.collection.to_string(),
            lines: preview.rejected,
        });
        let reply = build_rejecting_rows_payload(
            target.spec,
            target.rls_filters,
            &docs,
            rejection,
            &self.crs_context(),
        )
        .map_err(ErrorCode::from)?;
        let mut images = plan_images(
            index,
            target.collection,
            target.rls_filters,
            PlanRows::keyed_by_ordinal(docs)?,
            &self.crs_context(),
        )?;
        images.rejected = preview.rejected;
        Ok(ResolvedIngestReturning { reply, images })
    }
}
