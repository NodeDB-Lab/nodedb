// SPDX-License-Identifier: BUSL-1.1

//! Gateway execution results, and their Data-Plane response shape.
//!
//! A transport that renders a Data-Plane `Response` gets the same shape from
//! the gateway that local SPSC dispatch gives it. The shape keeps the error
//! status and code of a `NotFound` verdict, the read watermark, and the
//! vShards the read scanned.

use crate::Error;
use crate::bridge::envelope::{ErrorCode, Payload, Response, Status};
use crate::control::server::shared::clone_write::CloneCheckedTask;
use crate::types::{Lsn, ReadShards, RequestId, VShardId};

use super::core::{Gateway, QueryContext, authorized_plan_for_context};

/// A gateway execution's payloads, per-shard read watermarks, and read
/// vShards, as [`GatewayOutcome::into_parts`] returns them.
pub type GatewayParts = (Vec<Vec<u8>>, Vec<(VShardId, Lsn)>, ReadShards);

/// Everything one gateway execution observed across its routes.
pub struct GatewayOutcome {
    /// One payload, fused when several routes answered.
    pub payloads: Vec<Vec<u8>>,
    /// One `(vshard, watermark_lsn)` per participating shard.
    pub shard_watermarks: Vec<(VShardId, Lsn)>,
    /// The vShards every route read.
    pub read_vshards: ReadShards,
    /// A single-route plan's owning core refused it with `ErrorCode::NotFound`.
    ///
    /// Always `false` for a fan-out: there it means a shard holds no slice.
    pub not_found: bool,
}

impl GatewayOutcome {
    /// Payloads, per-shard watermarks, and read vShards.
    pub fn into_parts(self) -> GatewayParts {
        (self.payloads, self.shard_watermarks, self.read_vshards)
    }

    /// The response local SPSC dispatch returns for the same task.
    pub fn into_response(self) -> Response {
        let watermark_lsn = self
            .shard_watermarks
            .iter()
            .map(|(_, lsn)| *lsn)
            .max()
            .unwrap_or(Lsn::ZERO);
        if self.not_found {
            return not_found_response(watermark_lsn, self.read_vshards);
        }
        let payload = self
            .payloads
            .into_iter()
            .next()
            .map(Payload::from_vec)
            .unwrap_or_else(Payload::empty);
        response(Status::Ok, None, payload, watermark_lsn, self.read_vshards)
    }
}

impl Gateway {
    /// Execute one authorized task and return its Data-Plane response shape.
    ///
    /// A `NotFound` verdict, local or remote, returns as an error-status
    /// response carrying the vShards the read scanned, the same as local
    /// SPSC dispatch returns it. The caller records a phantom read from it
    /// and renders the verdict. Every other error returns as its typed `Err`.
    pub async fn execute_response(
        &self,
        ctx: &QueryContext,
        checked: CloneCheckedTask,
    ) -> Result<Response, Error> {
        let (plan, user_roles, _lease) = authorized_plan_for_context(ctx, checked)?;
        Ok(self
            .execute_plan_outcome(ctx, plan, &user_roles)
            .await?
            .into_response())
    }
}

fn not_found_response(watermark_lsn: Lsn, read_vshards: ReadShards) -> Response {
    response(
        Status::Error,
        Some(ErrorCode::NotFound),
        Payload::empty(),
        watermark_lsn,
        read_vshards,
    )
}

fn response(
    status: Status,
    error_code: Option<ErrorCode>,
    payload: Payload,
    watermark_lsn: Lsn,
    read_vshards: ReadShards,
) -> Response {
    Response {
        request_id: RequestId::new(0),
        status,
        attempt: 0,
        partial: false,
        payload,
        watermark_lsn,
        error_code: error_code.map(Box::new),
        stage_vote: None,
        read_vshards,
        write_set: Vec::new(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn read_vshards() -> ReadShards {
        ReadShards::single(VShardId::new(3))
    }

    fn outcome(not_found: bool) -> GatewayOutcome {
        GatewayOutcome {
            payloads: vec![vec![0x90]],
            shard_watermarks: vec![
                (VShardId::new(3), Lsn::new(7)),
                (VShardId::new(4), Lsn::new(9)),
            ],
            read_vshards: read_vshards(),
            not_found,
        }
    }

    #[test]
    fn a_not_found_verdict_keeps_its_error_status_and_code() {
        let resp = outcome(true).into_response();
        assert_eq!(resp.status, Status::Error);
        assert_eq!(resp.error_code.as_deref(), Some(&ErrorCode::NotFound));
        assert_eq!(resp.watermark_lsn, Lsn::new(9));
        assert_eq!(resp.read_vshards, read_vshards());
    }

    #[test]
    fn a_success_keeps_its_payload_watermark_and_read_vshards() {
        let resp = outcome(false).into_response();
        assert_eq!(resp.status, Status::Ok);
        assert!(resp.error_code.is_none());
        assert_eq!(resp.payload.to_vec(), vec![0x90u8]);
        assert_eq!(resp.watermark_lsn, Lsn::new(9));
        assert_eq!(resp.read_vshards, read_vshards());
    }
}
