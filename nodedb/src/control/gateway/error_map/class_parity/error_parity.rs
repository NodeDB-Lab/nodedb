// SPDX-License-Identifier: BUSL-1.1

//! Every `crate::Error` variant keeps its class on every surface and across a node hop.

use nodedb_types::error::sqlstate;

use crate::control::server::pgwire::types::error_map::numeric_code_to_sqlstate;
use crate::control::server::pgwire::types::error_to_sqlstate;

use super::super::GatewayErrorMap;
use super::error_index::ERROR_VARIANT_COUNT;
use super::error_index::error_variant_index;
use super::error_samples::error_samples;
use super::support::HopEncoder;
use super::support::class;

#[test]
fn every_error_variant_has_a_sample() {
    let mut seen = [false; ERROR_VARIANT_COUNT];
    for err in error_samples() {
        seen[error_variant_index(&err)] = true;
    }
    let missing: Vec<usize> = (0..ERROR_VARIANT_COUNT).filter(|i| !seen[*i]).collect();
    assert!(
        missing.is_empty(),
        "error variants with no sample: {missing:?}"
    );
}

/// Every `crate::Error` variant answers the HTTP status its pgwire SQLSTATE
/// class has. Only an internal or system error reads as a 500.
#[test]
fn every_error_variant_has_the_http_status_of_its_sqlstate() {
    for err in error_samples() {
        let (_, pg_state, _) = error_to_sqlstate(&err);
        let (status, _) = GatewayErrorMap::to_http(&err);
        assert_eq!(
            status,
            GatewayErrorMap::sqlstate_to_http(pg_state),
            "{err:?} is {pg_state} on pgwire"
        );
        let server_fault = matches!(class(pg_state), "XX" | "58");
        assert_eq!(
            status == 500,
            server_fault,
            "{err:?} is {pg_state} on pgwire but HTTP {status}"
        );
    }
}

/// Every `crate::Error` variant renders the SQLSTATE class it renders locally
/// after it crosses a node hop, through both wire encoders and the decoder.
#[test]
fn every_error_variant_keeps_its_class_across_a_node_hop() {
    use nodedb_cluster::rpc_codec::TypedClusterError;

    use crate::control::cluster::data_plane_error_wire::execution_error_to_typed;

    let encoders: [HopEncoder; 2] = [
        ("execution_error_to_typed", execution_error_to_typed),
        ("From<Error>", TypedClusterError::from),
    ];
    for (name, encode) in encoders {
        for (err, twin) in error_samples().into_iter().zip(error_samples()) {
            let (_, local, _) = error_to_sqlstate(&err);
            let rebuilt = crate::Error::from(encode(twin));
            let (_, remote, _) = error_to_sqlstate(&rebuilt);
            assert_eq!(
                class(remote),
                class(local),
                "{name}: {err:?} is {local} locally but {remote} after the hop as {rebuilt:?}"
            );
        }
    }
}

/// The SQLSTATE each Control-Plane variant renders where it has a class of
/// its own, pinned by variant index.
fn classified_sqlstates() -> Vec<(usize, &'static str)> {
    vec![
        (3, sqlstate::INVALID_PARAMETER_VALUE),
        (27, sqlstate::TOO_MANY_CONNECTIONS),
        (28, sqlstate::SERIALIZATION_FAILURE),
        (29, sqlstate::SYNTAX_ERROR),
        (30, sqlstate::SYNTAX_ERROR),
        (31, sqlstate::SYNTAX_ERROR),
        (32, sqlstate::ACTIVE_SQL_TRANSACTION),
        (34, sqlstate::QUERY_CANCELED.0),
        (44, sqlstate::QUOTA_OVERCOMMIT),
        (62, sqlstate::SYNTAX_ERROR),
        (63, sqlstate::SYNTAX_ERROR),
        // A CRDT constraint violation renders its constraint's class, never
        // the internal-error default.
        (74, sqlstate::UNIQUE_VIOLATION),
        (87, sqlstate::SYNTAX_ERROR),
        (88, sqlstate::DEPENDENT_OBJECTS_STILL_EXIST),
        (90, sqlstate::ACTIVE_SQL_TRANSACTION),
        (91, sqlstate::SYNTAX_ERROR),
        (92, sqlstate::SYNTAX_ERROR),
        (93, sqlstate::SYNTAX_ERROR),
        (95, sqlstate::SYNTAX_ERROR),
        (96, sqlstate::SYNTAX_ERROR),
        // A transaction snapshot past its age limit retries the block.
        (97, sqlstate::SERIALIZATION_FAILURE),
        (98, sqlstate::SYNTAX_ERROR),
        (99, sqlstate::SYNTAX_ERROR),
        (100, sqlstate::SYNTAX_ERROR),
        (101, sqlstate::QUOTA_EXCEEDED),
        (102, sqlstate::QUOTA_EXCEEDED),
        (127, sqlstate::DISK_FULL),
        (103, sqlstate::SYNTAX_ERROR),
        (104, sqlstate::SYNTAX_ERROR),
        (106, sqlstate::READ_ONLY_SQL_TRANSACTION),
        (107, sqlstate::STALE_READ_NOT_LEADER),
        (108, sqlstate::DEPENDENT_OBJECTS_STILL_EXIST),
        (109, sqlstate::DEPENDENT_OBJECTS_STILL_EXIST),
        (116, sqlstate::NUMERIC_VALUE_OUT_OF_RANGE),
        (117, sqlstate::INVALID_TEXT_REPRESENTATION),
        (118, sqlstate::DATATYPE_MISMATCH),
        (119, sqlstate::INVALID_DATETIME_FORMAT),
        (120, sqlstate::DATETIME_FIELD_OVERFLOW),
        (125, sqlstate::GROUPING_ERROR),
    ]
}

/// A client-facing Control-Plane variant renders its own SQLSTATE, never the
/// internal-error default.
#[test]
fn client_facing_variants_render_their_own_sqlstate() {
    let expected = classified_sqlstates();
    let mut seen = 0;
    for err in error_samples() {
        let index = error_variant_index(&err);
        if let Some((_, state)) = expected.iter().find(|(i, _)| *i == index) {
            assert_eq!(error_to_sqlstate(&err).1, *state, "{err:?}");
            seen += 1;
        }
    }
    assert_eq!(seen, expected.len(), "a pinned variant has no sample");
}

/// A variant with a dedicated public code renders that code's class on the
/// numeric table too, so native and remote renderings agree with pgwire.
#[test]
fn dedicated_codes_render_the_class_of_their_variant() {
    use nodedb_types::error::ErrorCode as Ec;

    assert_eq!(
        numeric_code_to_sqlstate(Ec::QUOTA_OVERCOMMIT),
        sqlstate::QUOTA_OVERCOMMIT
    );
    assert_eq!(
        numeric_code_to_sqlstate(Ec::TENANT_VECTOR_DIM_EXCEEDED),
        sqlstate::QUOTA_EXCEEDED
    );
    assert_eq!(
        numeric_code_to_sqlstate(Ec::TENANT_GRAPH_DEPTH_EXCEEDED),
        sqlstate::QUOTA_EXCEEDED
    );
    assert_eq!(
        numeric_code_to_sqlstate(Ec::STORAGE_QUOTA_EXCEEDED),
        sqlstate::DISK_FULL
    );
    assert_eq!(
        numeric_code_to_sqlstate(Ec::MIRROR_READ_ONLY),
        sqlstate::READ_ONLY_SQL_TRANSACTION
    );
    assert_eq!(
        numeric_code_to_sqlstate(Ec::STALE_READ_NOT_LEADER),
        sqlstate::STALE_READ_NOT_LEADER
    );
    assert_eq!(
        numeric_code_to_sqlstate(Ec::TRANSACTION_ROLLBACK),
        sqlstate::TRANSACTION_ROLLBACK
    );
    assert_eq!(
        numeric_code_to_sqlstate(Ec::ACTIVE_SQL_TRANSACTION),
        sqlstate::ACTIVE_SQL_TRANSACTION
    );
    assert_eq!(
        numeric_code_to_sqlstate(Ec::DEPENDENT_OBJECTS_EXIST),
        sqlstate::DEPENDENT_OBJECTS_STILL_EXIST
    );
}
