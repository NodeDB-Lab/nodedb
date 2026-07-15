// SPDX-License-Identifier: BUSL-1.1

//! `From` impls wiring domain errors into `Error` and converting to the
//! public `NodeDbError` boundary type.

use nodedb_types::error::NodeDbError;

use crate::types::TenantId;

use super::Error;

// ---------------------------------------------------------------------------
// From impls for domain-specific errors → Error
// ---------------------------------------------------------------------------

impl From<nodedb_query::expr_parse::ExprParseError> for Error {
    fn from(e: nodedb_query::expr_parse::ExprParseError) -> Self {
        Self::BadRequest {
            detail: e.to_string(),
        }
    }
}

impl From<crate::control::pubsub::TopicError> for Error {
    fn from(e: crate::control::pubsub::TopicError) -> Self {
        Self::BadRequest {
            detail: e.to_string(),
        }
    }
}

impl From<crate::engine::timeseries::ilp::IlpError> for Error {
    fn from(e: crate::engine::timeseries::ilp::IlpError) -> Self {
        Self::BadRequest {
            detail: e.to_string(),
        }
    }
}

impl From<crate::engine::timeseries::columnar_segment::SegmentError> for Error {
    fn from(e: crate::engine::timeseries::columnar_segment::SegmentError) -> Self {
        Self::Storage {
            engine: "timeseries".into(),
            detail: e.to_string(),
        }
    }
}

impl From<crate::engine::timeseries::query::QueryError> for Error {
    fn from(e: crate::engine::timeseries::query::QueryError) -> Self {
        Self::Storage {
            engine: "timeseries".into(),
            detail: e.to_string(),
        }
    }
}

impl From<crate::control::security::crl::CrlError> for Error {
    fn from(e: crate::control::security::crl::CrlError) -> Self {
        Self::Config {
            detail: e.to_string(),
        }
    }
}

impl From<crate::control::security::jwt::JwtError> for Error {
    fn from(e: crate::control::security::jwt::JwtError) -> Self {
        Self::RejectedAuthz {
            tenant_id: TenantId::new(0),
            resource: e.to_string(),
        }
    }
}

impl From<crate::storage::quarantine::engines::FtsOrQuarantine> for Error {
    fn from(e: crate::storage::quarantine::engines::FtsOrQuarantine) -> Self {
        Self::SegmentCorrupted {
            detail: e.to_string(),
        }
    }
}

// ---------------------------------------------------------------------------
// From<Error> for NodeDbError — the public API boundary conversion
// ---------------------------------------------------------------------------

impl From<Error> for NodeDbError {
    fn from(e: Error) -> Self {
        match e {
            // Write path
            Error::RejectedConstraint {
                collection, detail, ..
            } => NodeDbError::constraint_violation(collection, detail),
            Error::RejectedAuthz { resource, .. } => NodeDbError::authorization_denied(resource),
            err @ Error::TxnOverlayMemoryExceeded { .. } => {
                NodeDbError::bad_request(err.to_string())
            }
            err @ Error::OffsetRegression { .. } => NodeDbError::bad_request(err.to_string()),
            Error::DeadlineExceeded { .. } => NodeDbError::deadline_exceeded(),
            Error::ConflictRetry {
                collection,
                document_id,
            } => NodeDbError::write_conflict(collection, document_id),
            Error::CalvinSerializationConflict => NodeDbError::write_conflict(
                "cross-shard",
                "global OCC verdict was abort (read-set validation failed)",
            ),
            Error::SourceFrozen { database_id } => NodeDbError::write_conflict(
                format!("database:{database_id}"),
                "source database is frozen for clone materialization; retry shortly".to_owned(),
            ),
            Error::RejectedPrevalidation { constraint, reason } => {
                NodeDbError::prevalidation_rejected(constraint, reason)
            }
            Error::AppendOnlyViolation {
                collection, detail, ..
            } => NodeDbError::append_only_violation(collection, detail),
            Error::BalanceViolation {
                collection, detail, ..
            } => NodeDbError::balance_violation(collection, detail),
            Error::PeriodLocked {
                collection, detail, ..
            } => NodeDbError::period_locked(collection, detail),
            Error::RetentionViolation {
                collection, detail, ..
            } => NodeDbError::retention_violation(collection, detail),
            Error::LegalHoldActive {
                collection, detail, ..
            } => NodeDbError::legal_hold_active(collection, detail),
            Error::StateTransitionViolation {
                collection, detail, ..
            } => NodeDbError::state_transition_violation(collection, detail),
            Error::TransitionCheckViolation {
                collection, detail, ..
            } => NodeDbError::transition_check_violation(collection, detail),
            Error::TypeGuardViolation {
                collection, detail, ..
            } => NodeDbError::type_guard_violation(collection, detail),
            Error::TypeMismatch {
                collection, detail, ..
            } => NodeDbError::type_mismatch(collection, detail),
            Error::OverflowError { collection, key } => {
                NodeDbError::overflow(collection, format!("key {key}"))
            }
            Error::InsufficientBalance {
                collection,
                key,
                detail,
            } => NodeDbError::insufficient_balance(collection, format!("key {key}: {detail}")),
            Error::RateExceeded { gate, detail, .. } => NodeDbError::rate_exceeded(gate, detail),

            // Read path
            Error::CollectionNotFound { collection, .. } => {
                NodeDbError::collection_not_found(collection)
            }
            Error::DocumentNotFound {
                collection,
                document_id,
            } => NodeDbError::document_not_found(collection, document_id),
            Error::CollectionDeactivated {
                collection,
                retention_expires_at_ns,
                ..
            } => NodeDbError::collection_deactivated(collection, retention_expires_at_ns),

            // Routing / Cluster
            Error::NoLeader { vshard_id } => {
                NodeDbError::no_leader(format!("vshard {vshard_id} has no serving leader"))
            }
            Error::NotLeader { leader_addr, .. } => NodeDbError::not_leader(leader_addr),
            Error::FanOutExceeded {
                shards_touched,
                limit,
            } => NodeDbError::fan_out_exceeded(shards_touched, limit),
            err @ Error::CrossCollectionNotColocated { .. } => {
                NodeDbError::bad_request(err.to_string())
            }

            // Client input
            Error::BadRequest { detail } => NodeDbError::bad_request(detail),
            Error::QuotaOvercommit { field, detail } => {
                NodeDbError::quota_overcommit(field, detail)
            }
            Error::PlanError { detail } => NodeDbError::plan_error(detail),
            Error::RetryableSchemaChanged { descriptor } => {
                NodeDbError::plan_error(format!("retryable schema change on {descriptor}"))
            }
            Error::RetryableLeaderChange {
                group_id,
                log_index,
            } => NodeDbError::dispatch(format!(
                "raft leader change overwrote entry at group {group_id} index {log_index}; retry exhausted"
            )),
            Error::ExecutionLimitExceeded { detail } => NodeDbError::bad_request(detail),
            Error::LimitExceeded {
                limit_name,
                value,
                max,
            } => {
                NodeDbError::bad_request(format!("{limit_name} = {value} exceeds server cap {max}"))
            }

            // Infrastructure — flatten to opaque public variants
            Error::Wal(wal_err) => NodeDbError::wal(wal_err),
            Error::Dispatch { detail } => NodeDbError::dispatch(detail),
            Error::Storage { detail, .. } => NodeDbError::storage(detail),
            Error::ColdStorage { detail } => NodeDbError::cold_storage(detail),
            Error::Serialization { format, detail } => NodeDbError::serialization(format, detail),
            Error::Codec { detail } => NodeDbError::codec(detail),
            Error::SegmentCorrupted { detail } => NodeDbError::segment_corrupted(detail),
            Error::MemoryExhausted { engine } => NodeDbError::memory_exhausted(engine),
            Error::Backpressure { engine } => NodeDbError::memory_exhausted(engine.to_string()),
            Error::Crdt(crdt_err) => NodeDbError::internal(crdt_err),
            Error::Io(io_err) => NodeDbError::storage(io_err),
            Error::Config { detail } => NodeDbError::config(detail),
            Error::Encryption { detail } => NodeDbError::encryption(detail),
            Error::Bridge { detail } => NodeDbError::bridge(detail),
            Error::VersionCompat { detail } => NodeDbError::cluster(detail),
            Error::Internal { detail } => NodeDbError::internal(detail),
            err @ Error::DescriptorVersionAnomaly { .. } => NodeDbError::internal(err.to_string()),
            Error::Promql(e) => NodeDbError::bad_request(e.to_string()),
            Error::DependentObjectsExist {
                tenant_id: _,
                root_kind,
                root_name,
                dependent_count,
                dependents,
            } => {
                let names: Vec<String> =
                    dependents.iter().map(|(k, n)| format!("{k}:{n}")).collect();
                NodeDbError::bad_request(format!(
                    "cannot drop {root_kind} '{root_name}': {dependent_count} dependent(s) exist ({})",
                    names.join(", ")
                ))
            }
            Error::CascadeCycle {
                tenant_id: _,
                root,
                depth,
            } => NodeDbError::internal(format!(
                "cascade cycle / depth-limit ({depth}) exceeded on '{root}'"
            )),
            Error::CrossShardInExplicitTransaction => NodeDbError::bad_request(
                "cross-shard write inside explicit transaction block is not supported. \
                 Calvin cross-shard atomicity requires auto-commit (single-statement). \
                 Options: 1) Remove BEGIN/COMMIT to use auto-commit. \
                 2) SET cross_shard_txn = 'best_effort_non_atomic' for non-atomic dispatch."
                    .to_owned(),
            ),
            Error::SequencerUnavailable => NodeDbError::bad_request(
                "cross-shard transactions require a cluster deployment with the Calvin sequencer; \
                 this node is running in embedded/local mode"
                    .to_owned(),
            ),
            Error::OllpExhausted { retries } => NodeDbError::bad_request(format!(
                "OLLP dependent-read exhausted {retries} retries; the predicate's matching set \
                 kept changing across retries. Consider rephrasing as a static-key UPDATE if possible."
            )),
            Error::SessionCapExceeded { cap } => NodeDbError::bad_request(format!(
                "session cap ({cap}) exceeded — rejecting new login"
            )),
            Error::TenantVectorDimExceeded { dim, limit } => {
                NodeDbError::tenant_vector_dim_exceeded(dim, limit)
            }
            Error::TenantGraphDepthExceeded { depth, limit } => {
                NodeDbError::tenant_graph_depth_exceeded(depth, limit)
            }
            Error::RoleInheritanceCycle { child, parent } => NodeDbError::bad_request(format!(
                "role inheritance cycle: granting '{parent}' as parent of '{child}' would create a cycle"
            )),
            Error::RoleInheritanceDepthExceeded { depth, limit } => {
                NodeDbError::bad_request(format!(
                    "role inheritance depth {depth} exceeds the maximum allowed depth of {limit}"
                ))
            }
            Error::MirrorReadOnly { database } => NodeDbError::mirror_read_only(database),
            Error::StaleReadNotLeader {
                database,
                source_cluster,
                ..
            } => NodeDbError::stale_read_not_leader(database, source_cluster),

            Error::SessionIdleTimeout => {
                NodeDbError::bad_request("session terminated: idle timeout exceeded".to_owned())
            }
            Error::SessionTokenExpired => {
                NodeDbError::bad_request("session terminated: OIDC token expired".to_owned())
            }
            Error::SessionKilledByAdmin => {
                NodeDbError::bad_request("session terminated by administrator".to_owned())
            }
            Error::SessionUserDropped => {
                NodeDbError::bad_request("session terminated: user account dropped".to_owned())
            }
            Error::OidcProviderTenantUnbound => NodeDbError::bad_request(
                "OIDC: authenticated provider has no tenant binding".to_owned(),
            ),
            Error::OidcProviderTenantUnavailable { .. } => NodeDbError::bad_request(
                "OIDC: authenticated provider tenant is unavailable".to_owned(),
            ),
            Error::OidcNoDefaultDatabase { sub } => NodeDbError::bad_request(format!(
                "OIDC: no default database resolved for sub '{sub}'"
            )),
            // A Data-Plane error code that propagated to the public boundary.
            // Faithfully surface the typed code's meaning; fall back to internal
            // for codes without a dedicated public constructor.
            Error::DataPlane(code) => {
                use crate::bridge::envelope::ErrorCode as Ec;
                match code {
                    Ec::RejectedConstraint { constraint, detail } => {
                        NodeDbError::constraint_violation(constraint, detail)
                    }
                    Ec::RejectedAuthz => NodeDbError::authorization_denied(""),
                    Ec::DeadlineExceeded => NodeDbError::deadline_exceeded(),
                    Ec::ConflictRetry => NodeDbError::write_conflict("", ""),
                    other => NodeDbError::internal(format!("{other:?}")),
                }
            }
        }
    }
}

// ---------------------------------------------------------------------------
// TypedClusterError ↔ Error conversions
// ---------------------------------------------------------------------------

/// Convert a wire-level typed cluster error into the internal `Error` type.
///
/// Used by the C-β gateway layer (C-γ) to translate remote executor errors
/// into actionable local errors. The `NotLeader` variant preserves the
/// machine-readable group/term fields so the gateway retry loop can update
/// its routing table.
impl From<nodedb_cluster::rpc_codec::TypedClusterError> for Error {
    fn from(e: nodedb_cluster::rpc_codec::TypedClusterError) -> Self {
        use nodedb_cluster::rpc_codec::TypedClusterError;
        match e {
            TypedClusterError::NotLeader {
                group_id,
                leader_node_id,
                leader_addr,
                ..
            } => Error::NotLeader {
                // Clamp group_id to valid vShard range — group IDs may exceed 1024
                // for cluster-managed Raft groups; best-effort for display purposes.
                vshard_id: crate::types::VShardId::new(
                    (group_id as u32).min(crate::types::VShardId::COUNT - 1),
                ),
                leader_node: leader_node_id.unwrap_or(0),
                leader_addr: leader_addr.unwrap_or_default(),
            },
            TypedClusterError::DescriptorMismatch { collection, .. } => {
                Error::RetryableSchemaChanged {
                    descriptor: collection,
                }
            }
            TypedClusterError::DeadlineExceeded { .. } => Error::DeadlineExceeded {
                request_id: crate::types::RequestId::new(0),
            },
            TypedClusterError::Internal { message, .. } => Error::Internal { detail: message },
        }
    }
}

/// Build a `TypedClusterError::NotLeader` from an `Error::NotLeader`.
impl From<Error> for nodedb_cluster::rpc_codec::TypedClusterError {
    fn from(e: Error) -> Self {
        use nodedb_cluster::rpc_codec::TypedClusterError;
        match e {
            Error::NotLeader {
                vshard_id,
                leader_node,
                leader_addr,
            } => TypedClusterError::NotLeader {
                group_id: vshard_id.as_u32() as u64,
                leader_node_id: if leader_node == 0 {
                    None
                } else {
                    Some(leader_node)
                },
                leader_addr: if leader_addr.is_empty() {
                    None
                } else {
                    Some(leader_addr)
                },
                term: 0,
            },
            Error::DeadlineExceeded { .. } => TypedClusterError::DeadlineExceeded { elapsed_ms: 0 },
            other => TypedClusterError::Internal {
                code: 0,
                message: other.to_string(),
            },
        }
    }
}
