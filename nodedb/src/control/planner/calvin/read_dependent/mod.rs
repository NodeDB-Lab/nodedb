// SPDX-License-Identifier: BUSL-1.1

//! Read-dependent Calvin transactions: cross-shard writes whose values
//! depend on rows of another vShard.

pub mod item_move;
mod recon;

pub use item_move::{CrossShardItemMove, move_item_across_shards};
