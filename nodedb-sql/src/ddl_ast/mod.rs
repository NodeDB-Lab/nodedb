// SPDX-License-Identifier: Apache-2.0

//! Typed AST for NodeDB-specific DDL statements.
//!
//! Every DDL command the system supports is represented as a variant
//! of [`NodedbStatement`]. The DDL router matches on this enum
//! instead of string prefixes, so the compiler catches missing
//! handlers when a new DDL is added.
//!
//! The parser ([`parse`]) converts raw SQL into a `NodedbStatement`
//! using whitespace-split token matching — the same technique the
//! old string-prefix router used, but producing a typed output.

pub mod alter_ops;
pub mod collection_type;
pub mod graph_parse;
pub mod graph_types;
pub mod parse;
pub mod statement;

pub use alter_ops::{
    AlterCollectionOp, AlterRoleOp, AlterUserOp, ConflictPolicyKind, ConstraintKindKeyword,
};
pub use collection_type::build_collection_type;
pub use graph_parse::{FusionParams, parse_search_using_fusion};
pub use graph_types::{GraphDirection, GraphEdgeTuple, GraphProperties};
pub use nodedb_types::QuotaSpec;
pub use nodedb_types::{MirrorMode, MirrorStatus};
pub use parse::parse;
pub use statement::{
    AlterDatabaseOperation, AlterTenantOperation, AuthStmt, AutomationStmt, CloneAsOf, ClusterStmt,
    CollectionStmt, DatabaseStmt, GraphStmt, MiscStmt, NodedbStatement, OidcClaimMappingClause,
    PolicyStmt, StreamViewStmt, TenantSelector,
};
