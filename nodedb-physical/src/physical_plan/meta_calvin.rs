// SPDX-License-Identifier: Apache-2.0

//! Calvin-specific identity types carried by `MetaOp` Calvin variants.
//!
//! [`PassiveReadKeyId`] keys `MetaOp::CalvinExecuteActive::injected_reads`.
//! It lives in `nodedb-types`, so the replicated transaction class in
//! `nodedb-cluster` names the same identity.

pub use nodedb_types::calvin::{PassiveKey, PassiveReadKeyId};
