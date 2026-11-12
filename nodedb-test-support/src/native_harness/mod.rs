// SPDX-License-Identifier: BUSL-1.1

//! Shared native-protocol end-to-end test harness.
//!
//! Spawns a full NodeDB server (Data Plane core + native listener + response
//! poller) bound to an ephemeral port, plus handshake/frame helpers for
//! driving the wire protocol directly.

mod frames;
mod server;

pub use frames::{
    FrameEncoding, do_handshake, do_handshake_from, open_trust_session, read_frame, response_bound,
    send_api_key_auth, send_request, send_request_encoded, send_request_msgpack,
    send_request_within, send_sql, write_frame,
};
pub use server::NativeTestServer;
