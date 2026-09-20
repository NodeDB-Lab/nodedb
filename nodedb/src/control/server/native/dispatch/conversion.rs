// SPDX-License-Identifier: BUSL-1.1

//! Shared conversion helpers for native protocol dispatch.

use nodedb_types::Value;
use nodedb_types::protocol::NativeResponse;

use crate::bridge::envelope::Response;
use crate::control::server::native::sqlstate_code::sqlstate_error;
use crate::control::server::response_shape::types::{
    DmlFoldError, DmlOutcome, FoldedTag, ShapedRows,
};
use crate::control::server::shared::ddl::sqlstate::error_code_to_sqlstate;
use crate::control::server::shared::ddl::{DdlError, DdlResult};

/// Convert a Control-Plane error into a native error frame.
///
/// The stable numeric NodeDB code travels alongside the SQLSTATE, taken from
/// the one internal-to-public mapping table the crate owns. Without it the
/// frame reaches the client with `ndb_code == 0`, which the client can only
/// rebuild as a generic internal failure — so a planner's "collection does not
/// exist", an authorization refusal and a rate-limit rejection would all
/// answer `false` to `is_not_found()` / `is_auth_denied()` / `is_rate_exceeded()`.
/// The SQLSTATE is chosen here because it is a protocol-level rendering, while
/// the numeric code is the classification itself.
pub(crate) fn error_to_native(seq: u64, e: &crate::Error) -> NativeResponse {
    let (code, message) = match e {
        crate::Error::BadRequest { detail } => ("42601", detail.clone()),
        crate::Error::RejectedAuthz { resource, .. } => ("42501", resource.clone()),
        crate::Error::RateExceeded { .. } => (
            nodedb_types::error::sqlstate::TOO_MANY_CONNECTIONS,
            format!("{e}"),
        ),
        crate::Error::DeadlineExceeded { .. } => ("57014", "query cancelled due to timeout".into()),
        crate::Error::CollectionNotFound { collection, .. } => {
            ("42P01", format!("collection '{collection}' not found"))
        }
        // Same SQLSTATE as the "not authenticated" responses in
        // `session::request`: the client's stored bearer token expired
        // mid-connection and it must re-authenticate with a fresh Auth frame.
        crate::Error::SessionTokenExpired => (
            "28000",
            "OIDC bearer token expired; re-authenticate with a fresh Auth request".into(),
        ),
        // A cross-shard Calvin OCC abort is a serialization failure (40001) —
        // the client should retry the whole transaction.
        crate::Error::CalvinSerializationConflict => (
            nodedb_types::error::sqlstate::SERIALIZATION_FAILURE,
            format!("{e}"),
        ),
        // A participant error aborted the txn before any read-set was validated:
        // retryable class 40, but never 40001.
        crate::Error::CalvinParticipantError => (
            nodedb_types::error::sqlstate::TRANSACTION_ROLLBACK,
            format!("{e}"),
        ),
        // A shard verdict that rode back as a typed code keeps the exact
        // SQLSTATE and message the response-frame path would have given, from
        // the same protocol-neutral table pgwire reads. Rendering `XX000` here
        // made one condition answer two SQLSTATEs depending on whether its
        // response was inspected in place or collapsed into a typed `Err`.
        crate::Error::DataPlane(code) => {
            let (_severity, sqlstate, message) = error_code_to_sqlstate(code);
            (sqlstate, message)
        }
        crate::Error::Shaping(e) => (
            crate::control::server::pgwire::types::error_map::numeric_code_to_sqlstate(e.code()),
            e.message().to_string(),
        ),
        other => ("XX000", format!("{other}")),
    };
    let ndb_code = crate::error_classify::classify(e).code().0;
    NativeResponse::error_with_code(seq, code, message, ndb_code)
}

/// Convert a Control-Plane error into a native error frame under a SQLSTATE
/// the call site chooses.
///
/// Same classification as [`error_to_native`] — the numeric code comes from
/// the one `Error` mapping table — but for the guards that render a more
/// specific SQLSTATE than the error's own variant implies: a plan that cannot
/// be built is `42601` to a SQL client whatever its internal cause, and an
/// RLS injection failure is `42501`. Those sites still hold the classified
/// error, so the code must come from it rather than be inferred back out of
/// the SQLSTATE they just chose.
pub(crate) fn error_to_native_with_sqlstate(
    seq: u64,
    sqlstate: impl Into<String>,
    e: &crate::Error,
) -> NativeResponse {
    NativeResponse::error_with_code(
        seq,
        sqlstate,
        e.to_string(),
        crate::error_classify::classify(e).code().0,
    )
}

/// Convert a `NodeDbError` produced while shaping a response into a
/// NativeResponse error frame.
///
/// The numeric code travels alongside the SQLSTATE: the error is already
/// classified here, and rendering only `XX000` would make the client rebuild
/// it as a generic internal failure.
pub(crate) fn shape_error_to_native(seq: u64, e: &nodedb_types::NodeDbError) -> NativeResponse {
    NativeResponse::error_with_code(seq, "XX000", e.message().to_string(), e.code().0)
}

/// Render a statement-tag fold refusal as a native error frame. Two tasks of
/// one statement disagreeing on their verb is a planner bug, so it is an
/// internal error, the same class pgwire's `dml_fold_error_to_pg` renders.
pub(crate) fn dml_fold_error_to_native(seq: u64, e: &DmlFoldError) -> NativeResponse {
    sqlstate_error(seq, "XX000", e.to_string())
}

/// Render an error [`Response`] from the Data Plane as a native error frame.
///
/// The Data Plane already classified the failure into a deterministic
/// `ErrorCode`; both the SQLSTATE and the message come from the same
/// protocol-neutral mapping pgwire uses (`error_code_to_sqlstate`), and the
/// stable numeric code comes from the one `ErrorCode` → `NodeDbError`
/// conversion the crate owns. Formatting the code with `{:?}` and stamping
/// `XX000` instead would discard that classification, leaving a native
/// client unable to tell a duplicate key from a crashed database.
pub(crate) fn error_response_to_native(seq: u64, response: &Response) -> NativeResponse {
    let mut native = error_code_to_native(seq, response.error_code.as_deref());
    // A non-empty payload on an error response is a handler-rendered message
    // and is more specific than the mapping's generic rendering.
    if !response.payload.is_empty()
        && let Some(payload) = native.error.as_mut()
    {
        payload.message = String::from_utf8_lossy(&response.payload).into_owned();
    }
    native
}

/// Render a bare Data-Plane `ErrorCode` as a native error frame, for the
/// paths that carry the code without a full [`Response`] (a staging-gate
/// rejection). `None` means the Data Plane refused without classifying, which
/// is the only case that legitimately reaches the client as `XX000`.
pub(crate) fn error_code_to_native(
    seq: u64,
    code: Option<&crate::bridge::envelope::ErrorCode>,
) -> NativeResponse {
    let Some(code) = code else {
        return sqlstate_error(seq, "XX000", "unknown data plane error");
    };
    let (_, sqlstate, message) = error_code_to_sqlstate(code);
    let public = nodedb_types::NodeDbError::from(crate::Error::DataPlane(code.clone()));
    NativeResponse::error_with_code(seq, sqlstate, message, public.code().0)
}

/// Encode a protocol-neutral DDL dispatch result into a single
/// `NativeResponse`.
///
/// Reduction mirrors the pgwire→native bridge: on error, an error
/// frame carrying the neutral SQLSTATE + message; otherwise the first
/// row-returning / status / empty result determines the response (a status tag
/// becomes a single-column status row, a row result becomes a columns+rows
/// frame, an empty result or an empty vec becomes a bare OK).
///
/// `DdlError` carries its own numeric `code` (see
/// `crate::control::server::shared::ddl::result`), so the frame's `ndb_code`
/// comes directly from it rather than a bare-SQLSTATE re-derivation. Without
/// it every DDL refusal — a `DROP TABLE` naming a collection that does not
/// exist, a denied `GRANT` — reaches the client as a generic internal
/// failure.
pub(crate) fn ddl_result_to_native(
    seq: u64,
    result: Result<Vec<DdlResult>, DdlError>,
) -> NativeResponse {
    match result {
        Err(DdlError {
            sqlstate,
            code,
            message,
        }) => NativeResponse::error_with_code(seq, sqlstate, message, code.0),
        // Unknown pgwire response variants are dropped during translation, so
        // the first element is the first meaningful result — the bridge
        // returns on the first known variant.
        Ok(results) => match results.into_iter().next() {
            Some(DdlResult::Status {
                command,
                rows_affected,
            }) => match rows_affected {
                // A count-bearing status is a DML answer (the `{ ... }`
                // document INSERT / UPSERT, the graph edge and label DSL):
                // its verb and count, rendered by the same rule the dispatch
                // loop's folded tag renders with. pgwire renders it as the
                // `<verb> <n>` command tag.
                Some(affected) => {
                    let mut r = NativeResponse::ok(seq);
                    apply_dml_status(&mut r, &command, affected);
                    r
                }
                // A count-less DDL keeps the status row, whose `Some(1)`
                // `rows_affected` sentinel means "one command ran".
                None => NativeResponse::status_row(seq, command),
            },
            Some(DdlResult::Rows(shaped)) => {
                let (columns, rows) = to_native_columns_rows(&shaped);
                NativeResponse {
                    seq,
                    status: nodedb_types::protocol::ResponseStatus::Ok,
                    columns: Some(columns),
                    rows: Some(rows),
                    rows_affected: None,
                    command: None,
                    watermark_lsn: 0,
                    error: None,
                    auth: None,
                    warnings: Vec::new(),
                }
            }
            Some(DdlResult::Empty) | None => NativeResponse::ok(seq),
        },
    }
}

/// Build the native response for a completed Calvin transaction: its
/// RETURNING rows, when a task carried them, and the statement's one folded
/// tag as `(rows_affected, command)`.
///
/// `apply_result` is the applied Data-Plane response drained from the sidecar
/// and `plans` is the completed batch's plans, in dispatch order. The fold is
/// the protocol-neutral `response_shape::calvin_fold` pgwire renders too, so
/// a three-row implicit-edge insert answers `(3, INSERT)` here and
/// `INSERT 0 3` there.
///
/// There is deliberately no per-statement fallback count. The number of
/// dispatched tasks is not the number of affected rows — a single-row delete
/// dual-homed with its implicit edge cleanup dispatches two tasks and may affect
/// zero rows — so a batch whose count-bearing write reported nothing surfaces an
/// error rather than a plausible number.
pub(crate) fn calvin_native_response(
    seq: u64,
    apply_result: Option<crate::bridge::envelope::Response>,
    plans: &[crate::bridge::envelope::PhysicalPlan],
    state: &crate::control::state::SharedState,
    database_id: nodedb_types::DatabaseId,
    tenant_id: nodedb_types::TenantId,
    auth: &crate::control::security::auth_context::AuthContext,
) -> NativeResponse {
    use crate::control::server::response_shape::calvin_fold::{
        CalvinFoldCtx, CalvinFoldError, fold_calvin_batch,
    };

    let plan_refs: Vec<&crate::bridge::envelope::PhysicalPlan> = plans.iter().collect();
    let fold = match fold_calvin_batch(
        &plan_refs,
        apply_result.as_ref(),
        &CalvinFoldCtx {
            // The native protocol announces no output columns ahead of the rows.
            projection: None,
            state,
            tenant_id,
            database_id,
            auth,
        },
    ) {
        Ok(fold) => fold,
        Err(CalvinFoldError::Verb(e)) => return dml_fold_error_to_native(seq, &e),
        Err(CalvinFoldError::Shape(e)) => return error_to_native(seq, &e),
    };

    let mut r = NativeResponse::ok(seq);
    if let Some(resp) = &apply_result {
        r.watermark_lsn = resp.watermark_lsn.as_u64();
    }
    if let Some(shaped) = fold.rows {
        let (cols, rows) = to_native_columns_rows(&shaped);
        if !cols.is_empty() {
            r.columns = Some(cols);
        }
        r.rows = Some(rows);
    }
    // A batch with no count-bearing plan (vector / DDL work) has no row count
    // or verb to report, and says so by leaving both unset rather than
    // inventing a count from the task count.
    match fold.tag {
        Some(FoldedTag::Dml(outcome)) => apply_dml_outcome(&mut r, outcome),
        Some(FoldedTag::Opaque) | None => {}
    }
    r
}

/// Write a statement's count-bearing outcome onto a native response: the
/// verb always, the count only when the verb carries one (`TRUNCATE` does
/// not, matching the bare tag pgwire answers with).
pub(crate) fn apply_dml_outcome(r: &mut NativeResponse, outcome: DmlOutcome) {
    apply_dml_status(r, outcome.verb, outcome.affected);
}

/// The one rendering rule behind [`apply_dml_outcome`], for a verb that is
/// not a `'static` tag name (a DDL-router `DdlResult::Status` command).
fn apply_dml_status(r: &mut NativeResponse, verb: &str, affected: u64) {
    r.rows_affected = DmlOutcome::verb_carries_count(verb).then_some(affected);
    r.command = Some(verb.to_owned());
}

/// Convert protocol-neutral `ShapedRows` (produced by
/// `response_shape::compose::shape_response_materialized`) into native wire
/// columns/rows: each typed cell is carried as-is; a column absent from a
/// given row's map becomes `Value::Null`.
///
/// Structure is preserved all the way down, including nested objects and
/// arrays. The native protocol is MessagePack and `Value` has `Object` and
/// `Array` variants, so there is no format-level reason to render a nested
/// value as text — and doing so is lossy in a way the client cannot undo
/// reliably: a document field holding an object comes back as a `String` of
/// its JSON, so deserializing the row into the struct it was written from
/// fails with a type error, while a field that genuinely holds a JSON string
/// is indistinguishable from one that was flattened. Text rendering belongs
/// to pgwire, whose wire format is textual and which converts each cell to
/// JSON at its own edge for exactly that.
pub(crate) fn to_native_columns_rows(shaped: &ShapedRows) -> (Vec<String>, Vec<Vec<Value>>) {
    // Cells live in the row maps under per-column keys (display names may
    // repeat across columns, e.g. `SELECT w.id, b.id`), so read through the
    // shared accessor rather than by display name.
    let cell_keys = shaped.cell_keys();
    let rows = shaped
        .rows
        .iter()
        .map(|row| {
            cell_keys
                .iter()
                .map(|key| row.get(key.as_str()).cloned().unwrap_or(Value::Null))
                .collect()
        })
        .collect();
    (shaped.columns.clone(), rows)
}

#[cfg(test)]
mod tests {
    use super::*;
    use nodedb_types::protocol::ResponseStatus;

    /// A native session with a stored, now-expired OIDC token must surface
    /// an authentication-shaped error (SQLSTATE `28000`, the same class as
    /// "not authenticated") — not an internal error (`XX000`) — so the
    /// client can tell a stale token from a server fault and knows to
    /// re-authenticate rather than retry as-is.
    #[test]
    fn session_token_expired_maps_to_authentication_sqlstate() {
        let response = error_to_native(1, &crate::Error::SessionTokenExpired);

        assert_eq!(response.status, ResponseStatus::Error);
        let error = response
            .error
            .expect("error responses must carry a payload");
        assert_eq!(error.code, "28000");
    }

    /// One statement running out of time answers ONE SQLSTATE, whichever half
    /// of the race reported it: the Control-Plane timer, which raises
    /// `DeadlineExceeded` directly, or a shard refusing an already-expired
    /// task, whose verdict arrives as a Data-Plane code. Rendering the second
    /// as `XX000` made the code a client sees depend on machine load.
    #[test]
    fn both_halves_of_a_deadline_render_one_sqlstate() {
        let from_timer = error_to_native(
            1,
            &crate::Error::DeadlineExceeded {
                request_id: crate::types::RequestId::new(7),
            },
        );
        let from_shard = error_to_native(
            2,
            &crate::Error::DataPlane(crate::bridge::envelope::ErrorCode::DeadlineExceeded),
        );

        for response in [from_timer, from_shard] {
            let error = response.error.expect("error responses carry a payload");
            assert_eq!(error.code, "57014", "{error:?}");
            assert_eq!(
                error.ndb_code,
                nodedb_types::error::ErrorCode::DEADLINE_EXCEEDED.0,
                "{error:?}"
            );
        }
    }

    /// Every other shard verdict keeps its own SQLSTATE too, from the same
    /// protocol-neutral table pgwire reads.
    #[test]
    fn a_shard_verdict_keeps_its_sqlstate() {
        let response = error_to_native(
            1,
            &crate::Error::DataPlane(crate::bridge::envelope::ErrorCode::DivisionByZero),
        );

        let error = response.error.expect("error responses carry a payload");
        assert_eq!(error.code, "22012");
    }

    /// A Control-Plane classification must ride the frame as the numeric code,
    /// not just as a SQLSTATE: the client rebuilds the typed error from the
    /// number, and a zero there collapses every planner / catalog refusal into
    /// a generic internal failure.
    #[test]
    fn control_plane_errors_carry_their_numeric_code() {
        let response = error_to_native(
            1,
            &crate::Error::CollectionNotFound {
                tenant_id: crate::types::TenantId::new(0),
                collection: "missing".to_owned(),
            },
        );

        let error = response
            .error
            .expect("error responses must carry a payload");
        assert_eq!(error.code, "42P01");
        assert_eq!(
            error.ndb_code,
            nodedb_types::error::ErrorCode::COLLECTION_NOT_FOUND.0
        );
    }

    /// A DDL refusal is authored as a SQLSTATE with no `Error` behind it, so
    /// its numeric code comes from the SQLSTATE table. Without it the frame
    /// ships `ndb_code == 0` and a `DROP TABLE` naming an absent collection
    /// arrives as a generic internal failure while the identical `SELECT`
    /// arrives typed.
    ///
    /// Round-trips through actual msgpack bytes and `NodeDbError::from_wire`
    /// — proving the code reaches the client, not just that the server set
    /// it.
    #[test]
    fn ddl_refusals_carry_their_numeric_code() {
        let response = ddl_result_to_native(
            1,
            Err(DdlError::new(
                "42P01",
                "collection 'missing' does not exist",
            )),
        );

        let bytes = zerompk::to_msgpack_vec(&response).expect("encode native response");
        let decoded: NativeResponse =
            zerompk::from_msgpack(&bytes).expect("decode native response");
        let error = decoded.error.expect("error responses must carry a payload");
        assert_eq!(error.code, "42P01");
        assert_eq!(
            error.ndb_code,
            nodedb_types::error::ErrorCode::COLLECTION_NOT_FOUND.0
        );
        assert_eq!(error.message, "collection 'missing' does not exist");

        let client_err = nodedb_types::NodeDbError::from_wire(
            nodedb_types::error::ErrorCode(error.ndb_code),
            error.message,
        );
        assert_eq!(
            client_err.code(),
            nodedb_types::error::ErrorCode::COLLECTION_NOT_FOUND
        );
    }

    /// A count-bearing DDL-router status (the `{ ... }` document INSERT) is
    /// a DML answer: `(rows_affected, command)` exactly as the dispatch
    /// loop's folded tag reports, never a status row with no verb.
    #[test]
    fn count_bearing_ddl_status_reports_verb_and_count() {
        let response = ddl_result_to_native(
            1,
            Ok(vec![DdlResult::Status {
                command: "INSERT".to_owned(),
                rows_affected: Some(1),
            }]),
        );
        assert_eq!(response.rows_affected, Some(1));
        assert_eq!(response.command.as_deref(), Some("INSERT"));
        assert!(response.rows.is_none());
        assert!(response.columns.is_none());
    }

    /// `TRUNCATE` carries no count on any protocol: the verb alone.
    #[test]
    fn count_less_verb_status_reports_verb_only() {
        let response = ddl_result_to_native(
            1,
            Ok(vec![DdlResult::Status {
                command: "TRUNCATE".to_owned(),
                rows_affected: Some(9),
            }]),
        );
        assert_eq!(response.rows_affected, None);
        assert_eq!(response.command.as_deref(), Some("TRUNCATE"));
    }

    /// A count-less DDL keeps the status row.
    #[test]
    fn count_less_ddl_status_keeps_the_status_row() {
        let response = ddl_result_to_native(
            1,
            Ok(vec![DdlResult::Status {
                command: "CREATE COLLECTION".to_owned(),
                rows_affected: None,
            }]),
        );
        assert_eq!(response.command, None);
        assert_eq!(
            response.rows,
            Some(vec![vec![Value::String("CREATE COLLECTION".to_owned())]])
        );
    }

    /// A site that renders a more specific SQLSTATE than the error implies
    /// must still take the classification from the error rather than from the
    /// SQLSTATE it just chose: `42601` is shared by several conditions, while
    /// the error in hand names exactly one.
    #[test]
    fn a_site_chosen_sqlstate_keeps_the_errors_classification() {
        let response = error_to_native_with_sqlstate(
            1,
            "42601",
            &crate::Error::PlanError {
                detail: "no such column".to_owned(),
            },
        );

        let error = response
            .error
            .expect("error responses must carry a payload");
        assert_eq!(error.code, "42601");
        assert_eq!(
            error.ndb_code,
            nodedb_types::error::ErrorCode::PLAN_ERROR.0,
            "the classification must come from the error, not from the SQLSTATE"
        );
    }

    /// The numeric code is populated for every variant, including the ones
    /// whose SQLSTATE falls through to `XX000` — otherwise the fix would be a
    /// per-variant special case rather than one classification.
    #[test]
    fn errors_without_a_dedicated_sqlstate_still_carry_a_code() {
        let response = error_to_native(
            1,
            &crate::Error::PlanError {
                detail: "no such column".to_owned(),
            },
        );

        let error = response
            .error
            .expect("error responses must carry a payload");
        assert_eq!(error.code, "XX000");
        assert_eq!(
            error.ndb_code,
            nodedb_types::error::ErrorCode::PLAN_ERROR.0,
            "an unmapped SQLSTATE must not also erase the numeric classification"
        );
    }
}
