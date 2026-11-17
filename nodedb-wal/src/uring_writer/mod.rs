// SPDX-License-Identifier: Apache-2.0

//! io_uring-based WAL writer for Thread-per-Core data plane.
//!
//! This replaces `pwrite` + `fsync` with io_uring submission, enabling:
//!
//! - **Async I/O without threads**: the TPC core submits write + fsync SQEs
//!   and continues processing other work until the CQE arrives.
//! - **Batched submissions**: multiple records are written to the aligned buffer,
//!   then a single `IORING_OP_WRITE` + `IORING_OP_FSYNC` pair is submitted.
//! - **O_DIRECT compatible**: the aligned buffer satisfies O_DIRECT constraints.
//! - **No record cap from the buffer**: a record larger than the buffer is
//!   written alone after the buffered batch, framed as a batch of one.
//!
//! ## Integration
//!
//! In the real Data Plane, the io_uring instance is shared with the TPC event
//! loop. This module provides the WAL-specific SQE preparation; the caller
//! owns the ring and calls `submit()` + `wait()`.

mod config;
mod core;
mod submit;

pub use config::UringWriterConfig;
pub use core::UringWriter;
