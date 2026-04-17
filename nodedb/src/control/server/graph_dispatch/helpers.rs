//! Shared helpers for `bfs` and `shortest_path`: payload encoding +
//! `Response` construction.

use crate::bridge::envelope::Response;
use crate::types::{Lsn, RequestId};

pub(super) fn encode_path<S: serde::Serialize>(path: &[S]) -> crate::Result<Vec<u8>> {
    sonic_rs::to_vec(path).map_err(|e| crate::Error::Serialization {
        format: "json".into(),
        detail: e.to_string(),
    })
}

pub(super) fn ok_response(payload: Vec<u8>) -> Response {
    Response {
        request_id: RequestId::new(0),
        status: crate::bridge::envelope::Status::Ok,
        attempt: 1,
        partial: false,
        payload: crate::bridge::envelope::Payload::from_vec(payload),
        watermark_lsn: Lsn::ZERO,
        error_code: None,
    }
}
