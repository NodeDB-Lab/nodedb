// SPDX-License-Identifier: BUSL-1.1

//! Why a multi-part txn's parts do not assemble into local plans.

use nodedb_physical::physical_plan::wire::WireError;

/// A part set that does not assemble. Every replica holding the same parts
/// fails with the same error, so the txn is rejected alike everywhere.
#[derive(Debug, thiserror::Error)]
pub(in crate::control::cluster::calvin::scheduler::driver::core) enum PartsError {
    #[error("the txn reached dispatch with no assembly of its parts")]
    NoAssembly,
    #[error("part {part} task {task} is a control-plane-only plan")]
    ControlPlaneOnly { part: u32, task: u32 },
    #[error("part {part} task {task} is not a write")]
    NotAWrite { part: u32, task: u32 },
    #[error("part {part} task {task} is unroutable: {reason}")]
    Unroutable {
        part: u32,
        task: u32,
        reason: &'static str,
    },
    #[error("part {part} interrupts the chunks of task {task}")]
    InterruptsChunks { part: u32, task: u32 },
    #[error("part {part} tasks are malformed: {source}")]
    PartTasksInvalid {
        part: u32,
        source: nodedb_cluster::calvin::types::PartTasksError,
    },
    #[error("part {part} names a task past the last task index")]
    TaskIndexOverflow { part: u32 },
    #[error("part {part} task {task} routes to other vShards than its homes")]
    HomesMismatch { part: u32, task: u32 },
    #[error("task {task} arrives in more than one part")]
    TaskRepeated { task: u32 },
    #[error("part {part} does not continue the chunks of task {task}")]
    BrokenChunks { part: u32, task: u32 },
    #[error("task {task} chunks run past its {total_len} bytes")]
    ChunksOverrun { task: u32, total_len: u64 },
    #[error("task {task} does not decode: {source}")]
    TaskUndecodable { task: u32, source: WireError },
    #[error("task {task} stopped at byte {received} of {total_len}")]
    Unfinished {
        task: u32,
        received: usize,
        total_len: u64,
    },
    #[error("local plans do not encode: {source}")]
    PlansDoNotEncode { source: zerompk::Error },
}
