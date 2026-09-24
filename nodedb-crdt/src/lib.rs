// SPDX-License-Identifier: Apache-2.0

//! # nodedb-crdt
//!
//! CRDT engine with SQL constraint validation for NodeDB.
//!
//! ## The CRDT / SQL Paradox
//!
//! CRDTs are AP (Available + Partition-tolerant): agents compute optimistic
//! deltas offline and sync later. SQL constraints are CP (Consistent +
//! Partition-tolerant): UNIQUE indexes, foreign keys, etc. must hold globally.
//!
//! This crate bridges the gap:
//!
//! 1. **Optimistic local writes** — agents apply deltas to their local `LoroDoc`
//!    without constraint checks (AP behavior for availability).
//! 2. **Constraint validation at commit** — when deltas sync to the leader,
//!    constraints are validated against the committed state.
//! 3. **Dead-letter queue** — rejected deltas are routed to a DLQ with
//!    compensation hints so the application can recover gracefully.
//! 4. **Pre-validation** — optional fast-reject against the leader's state
//!    before the full Raft round-trip, reducing wasted consensus bandwidth.

pub mod auth;
pub mod constraint;
pub mod constraint_checks;
pub mod dead_letter;
pub mod deferred;
pub mod error;
pub mod list_ops;
mod loro_value;
pub mod policy;
pub mod pre_validate;
pub mod row_lookup;
pub mod signing;
pub mod state;
pub mod validator;

pub use auth::CrdtAuthContext;
pub use constraint::{Constraint, ConstraintKind, ConstraintSet};
pub use dead_letter::{CompensationHint, DeadLetter, DeadLetterQueue, EnqueueDeadLetterArgs};
pub use deferred::DeferredQueue;
pub use error::{CrdtError, Result};
pub use policy::{
    CollectionPolicy, ConflictPolicy, PolicyRegistry, PolicyResolution, ResolvedAction,
};
pub use row_lookup::RowLookup;
pub use signing::{DeltaSigner, DeviceRegistry};
pub use state::{CrdtDeltaPreview, CrdtDeltaPreviewLimits, CrdtState, ImportAdmission};
pub use state::{DEFAULT_MAX_DELTA_BYTES, DEFAULT_MAX_POST_IMAGE_BYTES};
pub use validator::{ValidationOutcome, Validator};
