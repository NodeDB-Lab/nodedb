// SPDX-License-Identifier: BUSL-1.1

//! Granted txn dispatch: the leader stages static and active (dependent-read)
//! txns on the Data Plane, and a follower holds them.

mod active_dispatch;
mod bind_identities;
mod follow;
mod incarnation;
mod primary_write;
mod static_dispatch;
mod unstaged;
