// SPDX-License-Identifier: BUSL-1.1

//! The Data-Plane verdict for each CRDT engine error.
//!
//! A client request causes most CRDT errors: a malformed delta, a version
//! that no longer exists, a write that names an absent document. Each one
//! keeps a client class here. Only server faults and broken invariants
//! answer `Internal`, which a client sees as `XX000`.

use nodedb_crdt::CrdtError;

use super::error_code::ErrorCode;

/// Exhaustive, so a new CRDT error picks its own class rather than
/// defaulting to `Internal`.
impl From<&CrdtError> for ErrorCode {
    fn from(e: &CrdtError) -> Self {
        match e {
            CrdtError::ConstraintViolation {
                constraint,
                collection,
                detail,
            } => Self::RejectedConstraint {
                constraint: constraint.clone(),
                detail: format!("{detail} (collection `{collection}`)"),
            },
            // Delta or snapshot bytes the client sent are malformed, past a
            // limit, or of a shape no collection holds. Class `22`.
            client @ (CrdtError::ImportTooLarge { .. }
            | CrdtError::ImportMalformed { .. }
            | CrdtError::ImportInvalidOperationRange
            | CrdtError::ImportOperationLimitExceeded { .. }
            | CrdtError::PreviewDeltaTooLarge { .. }
            | CrdtError::PreviewMalformed { .. }
            | CrdtError::PreviewInvalidOperationRange
            | CrdtError::PreviewOperationLimitExceeded { .. }
            | CrdtError::PreviewWriteSetLimitExceeded { .. }
            | CrdtError::PreviewTargetMismatch { .. }
            | CrdtError::PreviewPostImageTooLarge { .. }
            | CrdtError::NonMapRootContainer { .. }
            | CrdtError::NonMapRowValue { .. }
            | CrdtError::TypedValueMalformed { .. }
            | CrdtError::BlockListPathUnresolved { .. }
            | CrdtError::BlockListIndexOutOfBounds { .. }) => Self::DataException {
                detail: client.to_string(),
            },
            // A value of the wrong kind for the field or path it targets.
            // `42804`.
            client @ (CrdtError::ScalarFieldShadowsContainer { .. }
            | CrdtError::BlockListNotMovable { .. }) => Self::DatatypeMismatch {
                detail: client.to_string(),
            },
            CrdtError::RowAbsentAtVersion { collection, row_id } => Self::UndefinedObject {
                object: format!(
                    "document \"{row_id}\" in collection \"{collection}\" at the target version"
                ),
            },
            CrdtError::BlockListRowAbsent { collection, row_id } => Self::UndefinedObject {
                object: format!("document \"{row_id}\" in collection \"{collection}\""),
            },
            CrdtError::UnknownCollection(collection) => Self::UndefinedObject {
                object: format!("collection \"{collection}\""),
            },
            client @ CrdtError::VersionBeforeCompactionBoundary { .. } => {
                Self::ObjectNotInPrerequisiteState {
                    object: "CRDT version".into(),
                    detail: client.to_string(),
                }
            }
            // Nothing applied. The same bytes succeed once the missing
            // causal history arrives, or once the dead-letter queue drains.
            retry @ (CrdtError::ImportPendingDependencies
            | CrdtError::PreviewPendingDependencies
            | CrdtError::DlqFull { .. }) => Self::RetryableRefusal {
                reason: retry.to_string(),
            },
            denied @ (CrdtError::AuthExpired { .. } | CrdtError::InvalidSignature { .. }) => {
                Self::RejectedAuthz {
                    resource: denied.to_string(),
                }
            }
            // A replayed sequence number fails the same way every time.
            replay @ CrdtError::ReplayDetected { .. } => Self::RejectedPrevalidation {
                reason: replay.to_string(),
            },
            // Server faults: a Loro failure, a pending transaction on the
            // authoritative document, or an apply this node could not run.
            fault @ (CrdtError::DeltaApplyFailed(_)
            | CrdtError::SourceTransactionPending { .. }
            | CrdtError::PreviewSourceTransactionPending { .. }
            | CrdtError::Loro(_)) => Self::Internal {
                detail: fault.to_string(),
            },
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn code(e: CrdtError) -> ErrorCode {
        ErrorCode::from(&e)
    }

    #[test]
    fn a_missing_document_is_an_undefined_object() {
        let absent = code(CrdtError::RowAbsentAtVersion {
            collection: "notes".into(),
            row_id: "doc".into(),
        });
        assert!(
            matches!(&absent, ErrorCode::UndefinedObject { object } if object.contains("\"doc\"")),
            "got {absent:?}"
        );
        assert!(matches!(
            code(CrdtError::BlockListRowAbsent {
                collection: "pages".into(),
                row_id: "p".into(),
            }),
            ErrorCode::UndefinedObject { .. }
        ));
        assert!(matches!(
            code(CrdtError::UnknownCollection("c".into())),
            ErrorCode::UndefinedObject { .. }
        ));
    }

    #[test]
    fn a_version_below_the_compaction_boundary_is_not_in_prerequisite_state() {
        assert!(matches!(
            code(CrdtError::VersionBeforeCompactionBoundary {
                peer: 1,
                requested: 2,
                discarded: 3,
            }),
            ErrorCode::ObjectNotInPrerequisiteState { .. }
        ));
    }

    #[test]
    fn malformed_client_input_is_a_data_exception() {
        for e in [
            CrdtError::ImportMalformed { detail: "x".into() },
            CrdtError::ImportTooLarge {
                limit: 1,
                actual: 2,
            },
            CrdtError::ImportInvalidOperationRange,
            CrdtError::PreviewMalformed { detail: "x".into() },
            CrdtError::PreviewDeltaTooLarge {
                limit: 1,
                actual: 2,
            },
            CrdtError::PreviewPostImageTooLarge {
                limit: 1,
                actual: 2,
            },
            CrdtError::NonMapRowValue {
                collection: "c".into(),
                row_id: "r".into(),
                value: "a List container".into(),
            },
            CrdtError::BlockListIndexOutOfBounds {
                list_path: "blocks".into(),
                index: 9,
                len: 1,
            },
        ] {
            let mapped = code(e);
            assert!(
                matches!(mapped, ErrorCode::DataException { .. }),
                "got {mapped:?}"
            );
        }
    }

    #[test]
    fn a_wrong_kind_of_value_is_a_datatype_mismatch() {
        assert!(matches!(
            code(CrdtError::ScalarFieldShadowsContainer {
                collection: "c".into(),
                row_id: "r".into(),
                field: "blocks".into(),
            }),
            ErrorCode::DatatypeMismatch { .. }
        ));
    }

    #[test]
    fn missing_causal_history_is_retryable() {
        for e in [
            CrdtError::ImportPendingDependencies,
            CrdtError::PreviewPendingDependencies,
        ] {
            assert!(matches!(code(e), ErrorCode::RetryableRefusal { .. }));
        }
    }

    #[test]
    fn a_constraint_violation_keeps_its_constraint() {
        let mapped = code(CrdtError::ConstraintViolation {
            constraint: "users_email_unique".into(),
            collection: "users".into(),
            detail: "duplicate".into(),
        });
        assert!(
            matches!(&mapped, ErrorCode::RejectedConstraint { constraint, detail }
                if constraint == "users_email_unique" && detail.contains("users")),
            "got {mapped:?}"
        );
    }

    #[test]
    fn only_server_faults_stay_internal() {
        for e in [
            CrdtError::SourceTransactionPending { operations: 1 },
            CrdtError::PreviewSourceTransactionPending { operations: 1 },
            CrdtError::Loro("boom".into()),
            CrdtError::DeltaApplyFailed("boom".into()),
        ] {
            assert!(matches!(code(e), ErrorCode::Internal { .. }));
        }
    }

    /// pgwire, the native public code and a node hop all render one class.
    #[test]
    fn a_crdt_error_renders_its_class_on_every_surface() {
        use nodedb_types::error::sqlstate;

        use crate::control::server::pgwire::types::error_map::numeric_code_to_sqlstate;
        use crate::control::server::pgwire::types::error_to_sqlstate;

        let cases = [
            (
                CrdtError::RowAbsentAtVersion {
                    collection: "notes".into(),
                    row_id: "doc".into(),
                },
                sqlstate::UNDEFINED_OBJECT,
            ),
            (
                CrdtError::VersionBeforeCompactionBoundary {
                    peer: 1,
                    requested: 2,
                    discarded: 3,
                },
                sqlstate::OBJECT_NOT_IN_PREREQUISITE_STATE,
            ),
            (
                CrdtError::ImportMalformed { detail: "x".into() },
                sqlstate::DATA_EXCEPTION,
            ),
            (
                CrdtError::ScalarFieldShadowsContainer {
                    collection: "c".into(),
                    row_id: "r".into(),
                    field: "blocks".into(),
                },
                sqlstate::DATATYPE_MISMATCH,
            ),
            (
                CrdtError::SourceTransactionPending { operations: 1 },
                sqlstate::INTERNAL_ERROR,
            ),
            (CrdtError::Loro("boom".into()), sqlstate::INTERNAL_ERROR),
        ];
        for (crdt, expected) in cases {
            let label = crdt.to_string();
            let err = crate::Error::Crdt(crdt);
            let (_, pg_state, _) = error_to_sqlstate(&err);
            assert_eq!(pg_state, expected, "{label}");

            let public = crate::error_classify::classify(&err);
            assert_eq!(
                numeric_code_to_sqlstate(public.code()).get(..2),
                expected.get(..2),
                "{label}: public code {}",
                public.code()
            );

            let hopped =
                crate::Error::from(nodedb_cluster::rpc_codec::TypedClusterError::from(err));
            assert_eq!(
                error_to_sqlstate(&hopped).1,
                expected,
                "{label} after a hop"
            );
        }
    }

    #[test]
    fn a_crdt_error_crossing_the_bridge_keeps_its_class() {
        let mapped = ErrorCode::from(crate::Error::Crdt(CrdtError::RowAbsentAtVersion {
            collection: "notes".into(),
            row_id: "doc".into(),
        }));
        assert!(
            matches!(mapped, ErrorCode::UndefinedObject { .. }),
            "got {mapped:?}"
        );
    }
}
