// SPDX-License-Identifier: Apache-2.0

//! Read path (1100s), query (1200s), auth (2000s) constructors.

use std::fmt;

use super::super::code::ErrorCode;
use super::super::details::ErrorDetails;
use super::super::types::NodeDbError;

impl NodeDbError {
    pub fn collection_not_found(collection: impl Into<String>) -> Self {
        let collection = collection.into();
        Self {
            code: ErrorCode::COLLECTION_NOT_FOUND,
            message: format!("collection '{collection}' not found"),
            details: ErrorDetails::CollectionNotFound { collection },
            cause: None,
        }
    }

    /// Collection is mid-purge; new scans are refused until the purge
    /// ack. Distinct from `collection_not_found` so clients can
    /// differentiate "try again in a moment" from "does not exist".
    pub fn collection_draining(collection: impl Into<String>) -> Self {
        let collection = collection.into();
        Self {
            code: ErrorCode::COLLECTION_DRAINING,
            message: format!(
                "collection '{collection}' is draining for hard-delete; \
                 retry after the purge completes"
            ),
            details: ErrorDetails::CollectionDraining { collection },
            cause: None,
        }
    }

    /// Collection is soft-deleted (retention window active). Distinct
    /// from `collection_not_found` so clients can surface the
    /// `UNDROP COLLECTION <name>` hint instead of "does not exist".
    pub fn collection_deactivated(
        collection: impl Into<String>,
        retention_expires_at_ns: u64,
    ) -> Self {
        let collection = collection.into();
        let undrop_hint = format!("UNDROP COLLECTION {collection}");
        Self {
            code: ErrorCode::COLLECTION_DEACTIVATED,
            message: format!(
                "collection '{collection}' was dropped and is within its retention \
                 window; restore it with `{undrop_hint}` before it is hard-deleted"
            ),
            details: ErrorDetails::CollectionDeactivated {
                collection,
                retention_expires_at_ns,
                undrop_hint,
            },
            cause: None,
        }
    }

    pub fn document_not_found(
        collection: impl Into<String>,
        document_id: impl Into<String>,
    ) -> Self {
        let collection = collection.into();
        let document_id = document_id.into();
        Self {
            code: ErrorCode::DOCUMENT_NOT_FOUND,
            message: format!("document '{document_id}' not found in '{collection}'"),
            details: ErrorDetails::DocumentNotFound {
                collection,
                document_id,
            },
            cause: None,
        }
    }

    pub fn plan_error(detail: impl fmt::Display) -> Self {
        Self {
            code: ErrorCode::PLAN_ERROR,
            message: format!("query plan error: {detail}"),
            details: ErrorDetails::PlanError {
                phase: "unspecified".into(),
                detail: detail.to_string(),
            },
            cause: None,
        }
    }

    pub fn plan_error_at(phase: impl Into<String>, detail: impl Into<String>) -> Self {
        let phase = phase.into();
        let detail = detail.into();
        Self {
            code: ErrorCode::PLAN_ERROR,
            message: format!("query plan error [{phase}]: {detail}"),
            details: ErrorDetails::PlanError { phase, detail },
            cause: None,
        }
    }

    pub fn fan_out_exceeded(shards_touched: u16, limit: u16) -> Self {
        Self {
            code: ErrorCode::FAN_OUT_EXCEEDED,
            message: format!("query fan-out exceeded: {shards_touched} shards > limit {limit}"),
            details: ErrorDetails::FanOutExceeded {
                shards_touched,
                limit,
            },
            cause: None,
        }
    }

    pub fn sql_not_enabled() -> Self {
        Self {
            code: ErrorCode::SQL_NOT_ENABLED,
            message: "SQL not enabled (compile with 'sql' feature)".into(),
            details: ErrorDetails::SqlNotEnabled,
            cause: None,
        }
    }

    /// A function call in a query names no registered scalar, aggregate, or
    /// window function. Distinct from `plan_error` so clients can match on
    /// the specific code (SQLSTATE `42883`, `undefined_function`) rather
    /// than parsing the message.
    pub fn undefined_function(name: impl Into<String>) -> Self {
        let name = name.into();
        Self {
            code: ErrorCode::UNDEFINED_FUNCTION,
            message: format!("function {name}(...) does not exist"),
            details: ErrorDetails::UndefinedFunction { name },
            cause: None,
        }
    }

    /// A NOT NULL column received an explicit NULL or no value at all.
    /// Distinct from `constraint_violation` so clients can match on the
    /// specific code (SQLSTATE `23502`, `not_null_violation`).
    pub fn not_null_violation(table: impl Into<String>, column: impl Into<String>) -> Self {
        let table = table.into();
        let column = column.into();
        Self {
            code: ErrorCode::NOT_NULL_VIOLATION,
            message: format!(
                "null value in column '{column}' violates not-null constraint in table '{table}'"
            ),
            details: ErrorDetails::NotNullViolation { table, column },
            cause: None,
        }
    }

    /// Expression evaluation divided or took a modulus by zero. Distinct
    /// from `plan_error` so clients can match on the specific code
    /// (SQLSTATE `22012`, `division_by_zero`) rather than parsing the
    /// message.
    pub fn division_by_zero() -> Self {
        Self {
            code: ErrorCode::DIVISION_BY_ZERO,
            message: "division by zero".to_string(),
            details: ErrorDetails::DivisionByZero,
            cause: None,
        }
    }

    /// A LIMIT/OFFSET/FETCH bound did not resolve to `[0, usize::MAX]`.
    /// Distinct from `plan_error` so clients match the code, SQLSTATE
    /// `2201W`, instead of parsing the message.
    pub fn invalid_limit_value(clause: impl Into<String>, value: impl Into<String>) -> Self {
        let clause = clause.into();
        let value = value.into();
        Self {
            code: ErrorCode::INVALID_LIMIT_VALUE,
            message: format!("invalid {clause} value: {value}"),
            details: ErrorDetails::InvalidLimitValue { clause, value },
            cause: None,
        }
    }

    pub fn authorization_denied(resource: impl Into<String>) -> Self {
        let resource = resource.into();
        Self {
            code: ErrorCode::AUTHORIZATION_DENIED,
            message: format!("authorization denied on {resource}"),
            details: ErrorDetails::AuthorizationDenied { resource },
            cause: None,
        }
    }

    pub fn auth_expired(detail: impl fmt::Display) -> Self {
        Self {
            code: ErrorCode::AUTH_EXPIRED,
            message: format!("auth expired: {detail}"),
            details: ErrorDetails::AuthExpired,
            cause: None,
        }
    }

    /// Vector insert or index rejected: vector `dim` exceeds tenant's
    /// `max_vector_dim` quota.
    pub fn tenant_vector_dim_exceeded(dim: u32, limit: u32) -> Self {
        Self {
            code: ErrorCode::TENANT_VECTOR_DIM_EXCEEDED,
            message: format!("vector dimension {dim} exceeds tenant quota max_vector_dim={limit}"),
            details: ErrorDetails::TenantVectorDimExceeded { dim, limit },
            cause: None,
        }
    }

    /// Graph traversal rejected: requested `depth` exceeds tenant's
    /// `max_graph_depth` quota.
    pub fn tenant_graph_depth_exceeded(depth: u32, limit: u32) -> Self {
        Self {
            code: ErrorCode::TENANT_GRAPH_DEPTH_EXCEEDED,
            message: format!(
                "graph traversal depth {depth} exceeds tenant quota max_graph_depth={limit}"
            ),
            details: ErrorDetails::TenantGraphDepthExceeded { depth, limit },
            cause: None,
        }
    }

    /// `CLONE DATABASE` rejected: the clone chain is already at `depth` levels,
    /// which equals or exceeds `MAX_CLONE_DEPTH` (8).
    pub fn clone_depth_exceeded(depth: u32, limit: u32) -> Self {
        Self {
            code: ErrorCode::CLONE_DEPTH_EXCEEDED,
            message: format!(
                "clone chain depth {depth} exceeds the maximum of {limit}; \
                 materialize a clone to flatten the chain before cloning again"
            ),
            details: ErrorDetails::CloneDepthExceeded { depth, limit },
            cause: None,
        }
    }

    /// `CLONE DATABASE` rejected: the source database is a mirror and cannot
    /// be cloned until it is promoted to a writable primary.
    pub fn cannot_clone_mirror(database: impl Into<String>) -> Self {
        let database = database.into();
        Self {
            code: ErrorCode::CANNOT_CLONE_MIRROR,
            message: format!(
                "database '{database}' is a mirror and cannot be cloned; \
                 promote it first with ALTER DATABASE {database} PROMOTE"
            ),
            details: ErrorDetails::CannotCloneMirror { database },
            cause: None,
        }
    }

    /// `DROP DATABASE` rejected: one or more databases are cloned from this
    /// source. `dependents` lists the dependent database names.
    pub fn clone_dependency(source: impl Into<String>, dependents: Vec<String>) -> Self {
        let source = source.into();
        let dep_list = dependents.join(", ");
        Self {
            code: ErrorCode::CLONE_DEPENDENCY,
            message: format!(
                "cannot drop database '{source}': it is the source for clone(s): {dep_list}"
            ),
            details: ErrorDetails::CloneDependency { dependents },
            cause: None,
        }
    }

    /// Bitemporal `AS OF` query predates the clone's creation LSN; the clone
    /// did not exist at that point in time.
    pub fn clone_predates_query_time(as_of_lsn: u64, created_at_lsn: u64) -> Self {
        Self {
            code: ErrorCode::CLONE_PREDATES_QUERY_TIME,
            message: format!(
                "AS OF LSN {as_of_lsn} predates this clone's creation LSN {created_at_lsn}; \
                 the database did not exist at that point in time"
            ),
            details: ErrorDetails::ClonePredatesQueryTime {
                as_of_lsn,
                created_at_lsn,
            },
            cause: None,
        }
    }

    /// Write rejected: `collection` (engine `engine`) is a `Shadowed` or
    /// `Materializing` clone in `database` — merging target and source rows
    /// without a suppression record would silently duplicate them on read.
    /// `reason` names why no suppression record can be written: the engine
    /// has no copy-up/tombstone module at all, or this write's shape is
    /// outside the module it does have.
    pub fn clone_write_requires_materialize(
        collection: impl Into<String>,
        engine: impl Into<String>,
        database: impl Into<String>,
        reason: &str,
    ) -> Self {
        let collection = collection.into();
        let engine = engine.into();
        let database = database.into();
        Self {
            code: ErrorCode::CLONE_WRITE_REQUIRES_MATERIALIZE,
            message: format!(
                "collection '{collection}' (engine '{engine}') {reason}; \
                 run `ALTER DATABASE {database} MATERIALIZE` before writing to it"
            ),
            details: ErrorDetails::CloneWriteRequiresMaterialize {
                collection,
                engine,
                database,
            },
            cause: None,
        }
    }
}
