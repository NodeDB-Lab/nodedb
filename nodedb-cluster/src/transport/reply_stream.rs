// SPDX-License-Identifier: BUSL-1.1

//! The send half of an inbound RPC stream, reset when its request went
//! unanswered.
//!
//! A sender reads a stream finished without a reply as a refusal before the
//! handler: the request never ran, and a resend is safe. quinn finishes a
//! dropped `SendStream`, so a stream dropped once its handler started would
//! read the same way. A node that shuts down mid-request, a reply that fails
//! to write, and a handler panic all drop it there.
//!
//! [`ReplyStream`] closes that gap. Once armed, it resets the stream on drop
//! unless the request was answered. The sender reads a reset as a written
//! request with no answer, an unknown outcome it never resends.

use std::ops::{Deref, DerefMut};

/// The QUIC stream error code of a request whose handler started and that
/// got no answer.
const UNANSWERED_RESET_CODE: u32 = 1;

/// The send half of one inbound RPC stream.
pub(super) struct ReplyStream {
    send: quinn::SendStream,
    /// Whether the handler started and no answer went out yet.
    armed: bool,
}

impl ReplyStream {
    pub(super) fn new(send: quinn::SendStream) -> Self {
        Self { send, armed: false }
    }

    /// Mark the request as handed to its handler. A drop after this point
    /// resets the stream.
    pub(super) fn arm(&mut self) {
        self.armed = true;
    }

    /// Mark the request as answered. The drop then leaves the stream to
    /// quinn's finish.
    pub(super) fn answered(&mut self) {
        self.armed = false;
    }
}

impl Deref for ReplyStream {
    type Target = quinn::SendStream;

    fn deref(&self) -> &Self::Target {
        &self.send
    }
}

impl DerefMut for ReplyStream {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.send
    }
}

impl Drop for ReplyStream {
    fn drop(&mut self) {
        if self.armed {
            // A stream already closed by the connection needs no reset.
            let _ = self
                .send
                .reset(quinn::VarInt::from_u32(UNANSWERED_RESET_CODE));
        }
    }
}
