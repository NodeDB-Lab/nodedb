// SPDX-License-Identifier: BUSL-1.1

//! MATCH pattern executor — runs pattern matching on the CSR index.
//!
//! Takes a parsed `MatchQuery` and produces a result set of bound variable
//! assignments. Each assignment is a row mapping variable names to node/edge IDs.

pub(super) mod continuation;
pub(super) mod core;
pub(super) mod edge_props;
pub(super) mod expansion;
pub(super) mod order;
pub(super) mod overlay_expand;
pub(super) mod predicates;
pub(super) mod properties;
pub(super) mod row;
pub(super) mod types;
pub(super) mod varlen_named;

pub use self::continuation::{execute_continuation, execute_varlen_resume};
pub use self::core::{MatchExecCtx, execute, rows_to_msgpack};
pub use self::edge_props::StagedEdgeProperties;
pub use self::expansion::VarLenCaps;
pub use self::order::finish_match_rows;
pub use self::predicates::{DocumentCut, PropertyLookup};
pub use self::row::row_fingerprint;
pub use self::types::{
    BindingRow, ContinuationSeed, MatchOutcome, UnresolvedExpansion, VarLenResume,
};
