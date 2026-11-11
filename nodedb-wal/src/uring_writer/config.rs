// SPDX-License-Identifier: Apache-2.0

use crate::align::DEFAULT_ALIGNMENT;

/// io_uring WAL writer configuration.
#[derive(Debug, Clone)]
pub struct UringWriterConfig {
    /// Size of the aligned write buffer.
    pub write_buffer_size: usize,
    /// O_DIRECT alignment.
    pub alignment: usize,
    /// io_uring queue depth.
    pub ring_depth: u32,
    /// Use O_DIRECT.
    pub use_direct_io: bool,
}

impl Default for UringWriterConfig {
    fn default() -> Self {
        Self {
            write_buffer_size: crate::writer::DEFAULT_WRITE_BUFFER_SIZE,
            alignment: DEFAULT_ALIGNMENT,
            ring_depth: 64,
            use_direct_io: true,
        }
    }
}
