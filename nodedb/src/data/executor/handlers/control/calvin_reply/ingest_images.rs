// SPDX-License-Identifier: BUSL-1.1

//! The staged images of a resolved timeseries ingest: the rows its install
//! stores, known when the plan stages.

use nodedb_physical::physical_plan::{PhysicalPlan, TimeseriesOp};
use nodedb_types::Value;

use super::staged_images::{PlanRows, plan_images};
use super::target::ReturningTarget;
use crate::bridge::envelope::ErrorCode;
use crate::data::executor::core_loop::CoreLoop;
use crate::data::executor::response_codec::StagedPlanImages;
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

impl CoreLoop {
    /// The images of the resolved ingest `plan` at slice index `index`: the
    /// rows its install lands, each keyed by its ordinal among them.
    pub(super) fn calvin_resolved_ingest_images(
        &self,
        task: &ExecutionTask,
        tid: u64,
        plan: &PhysicalPlan,
        target: &ReturningTarget<'_>,
        index: u32,
    ) -> Result<StagedPlanImages, ErrorCode> {
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
        let images = self
            .preview_resolved_ingest_images(
                task.request.database_id,
                TenantId::new(tid),
                collection.as_str(),
                payload,
            )
            .map_err(ErrorCode::from)?;
        let rows = PlanRows::keyed_by_ordinal(stored_ts_docs(&images)?)?;
        plan_images(
            index,
            target.collection,
            target.rls_filters,
            rows,
            &self.crs_context(),
        )
    }
}
