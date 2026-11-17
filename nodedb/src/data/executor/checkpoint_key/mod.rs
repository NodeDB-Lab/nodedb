// SPDX-License-Identifier: BUSL-1.1

//! Fixed-length, key-carrying names for per-collection checkpoint files.

mod file;
mod key;

pub(crate) use file::{
    is_keyed_checkpoint_path, read_checkpoint_key, read_keyed_checkpoint,
    read_keyed_checkpoint_expecting, write_keyed_checkpoint, write_keyed_checkpoint_as,
};
pub(crate) use key::{CheckpointEngine, CheckpointKey, KEYED_CKPT_EXTENSION};
