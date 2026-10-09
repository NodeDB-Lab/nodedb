// SPDX-License-Identifier: BUSL-1.1

//! A proposal the data-group leader's write gate routed to the Calvin
//! sequencer.
//!
//! The leader refuses the propose with `RouteToSequencer` when a key of the
//! write is held and a Calvin transaction can sequence it. The proposer then
//! submits the same write, decoded from the entry it encoded, through the
//! scheduler. The scheduler queues it FIFO behind the holder on every
//! replica. Its answer stands for the apply the propose would have returned.

use std::sync::Weak;

use crate::bridge::envelope::{Response, Status};
use crate::control::server::shared::write_admission::{bare_ok_response, route_write_to_calvin};
use crate::control::state::SharedState;
use crate::control::wal_replication::ReplicatedEntry;
use crate::control::wal_replication::decode::decode_parsed_entry;
use crate::control::wal_replication::{AppliedWait, ProposedWrite};
use crate::types::RequestId;

/// The proposal of the encoded entry `data`, run through the Calvin
/// sequencer. No entry landed in the data-group log.
pub(super) fn routed_write(state: &Weak<SharedState>, data: Vec<u8>) -> ProposedWrite {
    let state = state.clone();
    let applied: AppliedWait = Box::pin(async move {
        let state = state.upgrade().ok_or_else(|| crate::Error::Internal {
            detail: "calvin route of a gated proposal: node is shutting down".into(),
        })?;
        let entry = ReplicatedEntry::from_bytes(&data).ok_or_else(|| crate::Error::Internal {
            detail: "calvin route of a gated proposal: the proposer's own entry does not decode"
                .into(),
        })?;
        let Some((database_id, (tenant_id, vshard_id, plan, _))) = decode_parsed_entry(&entry)?
        else {
            return Err(crate::Error::Internal {
                detail: "calvin route of a gated proposal: the entry carries no plan, yet the \
                         leader's write gate named it sequenceable"
                    .into(),
            });
        };
        let event_source = crate::event::EventSource::from(entry.event_source);
        let response = route_write_to_calvin(
            &state,
            tenant_id,
            database_id,
            vshard_id,
            plan,
            event_source,
        )
        .await?
        .unwrap_or_else(|| bare_ok_response(RequestId::new(0)));
        applied_result(&response)
    });
    ProposedWrite { at: None, applied }
}

/// The apply result a routed write's answer stands for: its payload and
/// written version, or its typed Data-Plane verdict.
fn applied_result(response: &Response) -> crate::Result<(Vec<u8>, crate::types::Lsn)> {
    if response.status == Status::Ok {
        return Ok((response.payload.to_vec(), response.read_version_lsn));
    }
    match response.error_code.as_deref() {
        Some(code) => Err(crate::Error::DataPlane(code.clone())),
        None => Err(crate::Error::Internal {
            detail: "calvin route of a gated proposal: the scheduler answered an error with no \
                     error code"
                .into(),
        }),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::bridge::envelope::ErrorCode;

    #[test]
    fn an_ok_answer_carries_its_payload_and_version() {
        let mut response = bare_ok_response(RequestId::new(1));
        response.payload = crate::bridge::envelope::Payload::from_vec(vec![7]);
        response.read_version_lsn = crate::types::Lsn::new(9);
        let (payload, version) = applied_result(&response).expect("ok");
        assert_eq!(payload, vec![7]);
        assert_eq!(version, crate::types::Lsn::new(9));
    }

    #[test]
    fn an_error_answer_keeps_its_verdict() {
        let mut response = bare_ok_response(RequestId::new(1));
        response.status = Status::Error;
        response.error_code = Some(Box::new(ErrorCode::OllpRetryRequired));
        assert!(matches!(
            applied_result(&response),
            Err(crate::Error::DataPlane(ErrorCode::OllpRetryRequired))
        ));
    }
}
