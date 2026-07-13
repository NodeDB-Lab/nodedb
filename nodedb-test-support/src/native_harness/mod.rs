// SPDX-License-Identifier: BUSL-1.1

//! Shared native-protocol end-to-end test harness.
//!
//! Spawns a full NodeDB server (Data Plane core + native listener + response
//! poller) bound to an ephemeral port, plus handshake/frame helpers for
//! driving the wire protocol directly.

mod frames;
mod server;

pub use frames::{
    do_handshake, read_frame, send_api_key_auth, send_request, send_sql, write_frame,
};
pub use server::NativeTestServer;
