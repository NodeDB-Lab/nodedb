// SPDX-License-Identifier: BUSL-1.1

//! What a source cluster does for the mirrors that follow it.
//!
//! - [`cut_loop`] — places a mirror cut in every data group while a link holds
//!   a pin.
//! - [`pins`] — each link's hold on the Raft logs, and its bounds.
//! - [`filter`] — what leaves this cluster for one database.
//! - [`service`] — answers an authenticated client's hello, fetch and ack.
//! - [`listener`] — starts the mTLS listener from the `[mirror]` config.

pub mod cut_loop;
pub mod filter;
pub mod listener;
pub mod pins;
pub mod service;
