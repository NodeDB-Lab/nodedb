// SPDX-License-Identifier: BUSL-1.1

//! pgwire error shape: `(SQLSTATE, message)`.

use super::gateway_map::GatewayErrorMap;
use crate::Error;

impl GatewayErrorMap {
    /// Map a gateway error into `(sqlstate, message)` for pgwire.
    ///
    /// One table answers for the direct and the routed path. This mapper's 14
    /// arms restated the direct table, but its own catch-all sent the 26
    /// variants the direct table classifies and it did not list to `XX000`.
    /// A client must not see a different class because a statement crossed the
    /// gateway.
    pub fn to_pgwire(err: &Error) -> (&'static str, String) {
        let (_severity, state, message) =
            crate::control::server::pgwire::types::error_to_sqlstate(err);
        (state, message)
    }
}

#[cfg(test)]
mod tests {
    use super::super::test_fixtures::{
        authz, deadline, internal, not_found, not_leader, schema_changed, serialization,
    };
    use super::*;
    use nodedb_types::error::sqlstate;

    #[test]
    fn pgwire_not_leader() {
        let (code, _msg) = GatewayErrorMap::to_pgwire(&not_leader());
        assert_eq!(code, sqlstate::DATABASE_DROPPED);
    }

    #[test]
    fn pgwire_deadline() {
        let (code, _) = GatewayErrorMap::to_pgwire(&deadline());
        assert_eq!(code, sqlstate::QUERY_CANCELED);
    }

    /// The gateway previously stated this arm itself; both paths now answer
    /// `INTERNAL_ERROR` from the shared table, with the variant's own message.
    #[test]
    fn pgwire_schema_changed() {
        let (code, msg) = GatewayErrorMap::to_pgwire(&schema_changed());
        assert_eq!(code, sqlstate::INTERNAL_ERROR);
        assert!(msg.contains("users"));
    }

    #[test]
    fn pgwire_not_found() {
        let (code, msg) = GatewayErrorMap::to_pgwire(&not_found());
        assert_eq!(code, sqlstate::UNDEFINED_TABLE);
        assert!(msg.contains("missing_col"));
    }

    #[test]
    fn pgwire_authz() {
        let (code, _) = GatewayErrorMap::to_pgwire(&authz());
        assert_eq!(code, sqlstate::INSUFFICIENT_PRIVILEGE);
    }

    #[test]
    fn pgwire_internal() {
        let (code, _) = GatewayErrorMap::to_pgwire(&internal());
        assert_eq!(code, sqlstate::INTERNAL_ERROR);
    }

    #[test]
    fn pgwire_serialization() {
        let (code, _) = GatewayErrorMap::to_pgwire(&serialization());
        assert_eq!(code, sqlstate::INTERNAL_ERROR);
    }

    /// A routed fault keeps the class the direct path gives it. The gateway's
    /// own arms restated the direct table, but the 26 variants the direct table
    /// classifies and the gateway did not list reached the client as XX000.
    #[test]
    fn gateway_and_direct_mapper_agree_on_classified_errors() {
        use crate::types::{DatabaseId, TenantId};

        let samples = vec![
            Error::RejectedConstraint {
                collection: "orders".into(),
                constraint: "unique".into(),
                detail: "duplicate key".into(),
            },
            Error::TxnOverlayMemoryExceeded { limit: 1 << 20 },
            Error::UndefinedFunction {
                name: "no_such_fn".into(),
            },
            Error::UndefinedObject {
                kind: "sequence",
                name: "s".into(),
            },
            Error::ObjectNotInPrerequisiteState {
                object: "s".into(),
                detail: "currval before nextval".into(),
            },
            Error::UndefinedColumn {
                column: "nope".into(),
            },
            Error::AmbiguousColumn {
                column: "id".into(),
            },
            Error::UnknownStrictField {
                collection: "c".into(),
                column: "x".into(),
            },
            Error::DivisionByZero,
            Error::InvalidLimitValue {
                clause: "LIMIT",
                value: "-1".into(),
            },
            Error::DocumentNotFound {
                collection: "c".into(),
                document_id: "d".into(),
            },
            Error::ConflictRetry {
                collection: "c".into(),
                document_id: "d".into(),
            },
            Error::CalvinSerializationConflict,
            Error::CalvinParticipantError,
            Error::SourceFrozen {
                database_id: DatabaseId::new(7),
            },
            Error::CloneWriteRequiresMaterialize {
                collection: "c".into(),
                engine: "kv".into(),
                database: "db".into(),
                reason: "shadowed",
            },
            Error::RateExceeded {
                gate: "write".into(),
                detail: "over budget".into(),
                retry_after_ms: 50,
            },
            Error::MemoryExhausted {
                engine: "kv".into(),
            },
            Error::FanOutExceeded {
                shards_touched: 9,
                limit: 8,
            },
            Error::MaterializedSumTargetNotFound {
                target_collection: "t".into(),
                join_column: "k".into(),
                join_value: "1".into(),
            },
            Error::CollectionDeactivated {
                tenant_id: TenantId::new(0),
                collection: "c".into(),
                retention_expires_at_ns: 1,
            },
            Error::FeatureNotSupported {
                detail: "sparse".into(),
            },
            Error::BackupTenantMismatch {
                expected: 1,
                actual: 2,
            },
            Error::BackupKeyMismatch,
        ];

        for err in samples {
            let (_sev, direct, _msg) =
                crate::control::server::pgwire::types::error_to_sqlstate(&err);
            let (gateway, _) = GatewayErrorMap::to_pgwire(&err);
            assert_ne!(
                direct,
                sqlstate::INTERNAL_ERROR,
                "sample has no class of its own: {err:?}"
            );
            assert_eq!(gateway, direct, "routed class differs for {err:?}");
        }
    }
}
