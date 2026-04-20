use pgwire::api::Type;
use pgwire::api::results::FieldFormat;
use pgwire::api::results::FieldInfo;
use pgwire::error::{ErrorInfo, PgWireError, PgWireResult};

use crate::bridge::envelope::{ErrorCode, Status};
use crate::control::security::identity::{AuthenticatedIdentity, Role};

/// Create a pgwire ErrorResponse with a SQLSTATE code.
pub fn sqlstate_error(code: &str, message: &str) -> PgWireError {
    PgWireError::UserError(Box::new(ErrorInfo::new(
        "ERROR".to_owned(),
        code.to_owned(),
        message.to_owned(),
    )))
}

/// Map a NodeDB `Error` to a PostgreSQL SQLSTATE code + message.
pub fn error_to_sqlstate(err: &crate::Error) -> (&'static str, &'static str, String) {
    match err {
        crate::Error::BadRequest { detail } => ("ERROR", "42601", detail.clone()),
        crate::Error::PlanError { detail } => ("ERROR", "42601", detail.clone()),
        crate::Error::CollectionNotFound { collection, .. } => (
            "ERROR",
            "42P01",
            format!("collection \"{collection}\" does not exist"),
        ),
        crate::Error::CollectionDeactivated { collection, .. } => (
            "ERROR",
            // 42P01 (undefined_table) is the canonical pg code; the
            // distinct message carries the UNDROP hint so client UX
            // can surface a restore button without a custom sqlstate.
            "42P01",
            format!(
                "collection \"{collection}\" was dropped and is within its retention \
                 window; restore it with `UNDROP COLLECTION {collection}` before \
                 it is hard-deleted"
            ),
        ),
        crate::Error::DocumentNotFound {
            collection,
            document_id,
        } => (
            "ERROR",
            "02000",
            format!("document \"{document_id}\" not found in \"{collection}\""),
        ),
        crate::Error::RejectedConstraint { detail, .. } => ("ERROR", "23505", detail.clone()),
        crate::Error::DeadlineExceeded { .. } => ("ERROR", "57014", err.to_string()),
        crate::Error::ConflictRetry { .. } => ("ERROR", "40001", err.to_string()),
        crate::Error::RejectedAuthz { .. } => ("ERROR", "42501", err.to_string()),
        crate::Error::MemoryExhausted { .. } => ("ERROR", "53200", err.to_string()),
        crate::Error::FanOutExceeded { .. } => ("ERROR", "54001", err.to_string()),
        crate::Error::NoLeader { .. } => ("ERROR", "55P03", err.to_string()),
        // SQLSTATE 57P04: admin_shutdown — the closest Postgres
        // canonical code for "try again later, different node".
        // Client libraries that recognise the 57P* family treat
        // this as retryable transient unavailability, which is
        // exactly the semantics we want. The message carries the
        // hinted leader address so an operator inspecting logs
        // can see the redirect target.
        crate::Error::NotLeader { leader_addr, .. } => (
            "ERROR",
            "57P04",
            format!("cluster in leader election; leader hint: {leader_addr}"),
        ),
        _ => ("ERROR", "XX000", err.to_string()),
    }
}

/// Map a Data Plane `ErrorCode` to SQLSTATE.
pub fn error_code_to_sqlstate(code: &ErrorCode) -> (&'static str, &'static str, String) {
    match code {
        ErrorCode::DeadlineExceeded => ("ERROR", "57014", "query cancelled due to deadline".into()),
        ErrorCode::RejectedConstraint { constraint, detail } => (
            "ERROR",
            "23505",
            if detail.is_empty() {
                format!("constraint violation: {constraint}")
            } else {
                format!("constraint violation: {constraint}: {detail}")
            },
        ),
        ErrorCode::RejectedPrevalidation { reason } => (
            "ERROR",
            "23514",
            format!("pre-validation rejected: {reason}"),
        ),
        ErrorCode::NotFound => ("ERROR", "02000", "not found".into()),
        ErrorCode::RejectedAuthz => ("ERROR", "42501", "authorization denied".into()),
        ErrorCode::ConflictRetry => ("ERROR", "40001", "write conflict, retry".into()),
        ErrorCode::FanOutExceeded => ("ERROR", "54001", "fan-out limit exceeded".into()),
        ErrorCode::ResourcesExhausted => ("ERROR", "53200", "resources exhausted".into()),
        ErrorCode::RejectedDanglingEdge { missing_node } => (
            "ERROR",
            "23503",
            format!("edge rejected: node \"{missing_node}\" does not exist"),
        ),
        ErrorCode::DuplicateWrite => (
            "ERROR",
            "23505",
            "duplicate write detected via idempotency key".into(),
        ),
        ErrorCode::AppendOnlyViolation { collection } => (
            "ERROR",
            "23601",
            format!("append-only violation: UPDATE/DELETE not allowed on {collection}"),
        ),
        ErrorCode::BalanceViolation { collection, detail } => (
            "ERROR",
            "23602",
            format!("balance violation on {collection}: {detail}"),
        ),
        ErrorCode::PeriodLocked { collection } => (
            "ERROR",
            "23603",
            format!("period locked: writes rejected on {collection}"),
        ),
        ErrorCode::RetentionViolation { collection } => (
            "ERROR",
            "23606",
            format!("retention violation: cannot delete from {collection}"),
        ),
        ErrorCode::LegalHoldActive { collection } => (
            "ERROR",
            "23607",
            format!("legal hold active: cannot delete from {collection}"),
        ),
        ErrorCode::StateTransitionViolation { collection, detail } => (
            "ERROR",
            "23604",
            format!("state transition violation on {collection}: {detail}"),
        ),
        ErrorCode::TransitionCheckViolation { collection } => (
            "ERROR",
            "23605",
            format!("transition check violation on {collection}"),
        ),
        ErrorCode::TypeGuardViolation { collection, detail } => (
            "ERROR",
            "23608",
            format!("type guard violation on {collection}: {detail}"),
        ),
        ErrorCode::TypeMismatch { collection, detail } => (
            "ERROR",
            "42846",
            format!("type mismatch on {collection}: {detail}"),
        ),
        ErrorCode::OverflowError { collection } => (
            "ERROR",
            "22003",
            format!("arithmetic overflow on {collection}"),
        ),
        ErrorCode::InsufficientBalance { collection, detail } => (
            "ERROR",
            "23514",
            format!("insufficient balance on {collection}: {detail}"),
        ),
        ErrorCode::RateExceeded {
            gate,
            retry_after_ms,
        } => (
            "ERROR",
            "54001",
            format!("rate limit exceeded for {gate}, retry after {retry_after_ms}ms"),
        ),
        ErrorCode::Internal { detail } => ("ERROR", "XX000", detail.clone()),
    }
}

/// Build a FieldInfo for a text column in query results.
pub fn text_field(name: &str) -> FieldInfo {
    FieldInfo::new(name.to_owned(), None, None, Type::TEXT, FieldFormat::Text)
}

/// Build a FieldInfo for an int8 column.
pub fn int8_field(name: &str) -> FieldInfo {
    FieldInfo::new(name.to_owned(), None, None, Type::INT8, FieldFormat::Text)
}

/// Build a FieldInfo for a float8 column.
pub fn float8_field(name: &str) -> FieldInfo {
    FieldInfo::new(name.to_owned(), None, None, Type::FLOAT8, FieldFormat::Text)
}

/// Build a FieldInfo for a float4 column.
pub fn float4_field(name: &str) -> FieldInfo {
    FieldInfo::new(name.to_owned(), None, None, Type::FLOAT4, FieldFormat::Text)
}

/// Build a FieldInfo for an int4 column.
pub fn int4_field(name: &str) -> FieldInfo {
    FieldInfo::new(name.to_owned(), None, None, Type::INT4, FieldFormat::Text)
}

/// Build a FieldInfo for an int2 column.
pub fn int2_field(name: &str) -> FieldInfo {
    FieldInfo::new(name.to_owned(), None, None, Type::INT2, FieldFormat::Text)
}

/// Build a FieldInfo for a bool column.
pub fn bool_field(name: &str) -> FieldInfo {
    FieldInfo::new(name.to_owned(), None, None, Type::BOOL, FieldFormat::Text)
}

/// Build a FieldInfo for a bytea column.
pub fn bytea_field(name: &str) -> FieldInfo {
    FieldInfo::new(name.to_owned(), None, None, Type::BYTEA, FieldFormat::Text)
}

/// Build a FieldInfo for a JSON column.
pub fn json_field(name: &str) -> FieldInfo {
    FieldInfo::new(name.to_owned(), None, None, Type::JSON, FieldFormat::Text)
}

/// Build a FieldInfo for a JSONB column.
pub fn jsonb_field(name: &str) -> FieldInfo {
    FieldInfo::new(name.to_owned(), None, None, Type::JSONB, FieldFormat::Text)
}

/// Build a FieldInfo for a timestamptz column.
pub fn timestamptz_field(name: &str) -> FieldInfo {
    FieldInfo::new(
        name.to_owned(),
        None,
        None,
        Type::TIMESTAMPTZ,
        FieldFormat::Text,
    )
}

/// Build a FieldInfo for a timestamp column.
pub fn timestamp_field(name: &str) -> FieldInfo {
    FieldInfo::new(
        name.to_owned(),
        None,
        None,
        Type::TIMESTAMP,
        FieldFormat::Text,
    )
}

/// Build a FieldInfo for a varchar column.
pub fn varchar_field(name: &str) -> FieldInfo {
    FieldInfo::new(
        name.to_owned(),
        None,
        None,
        Type::VARCHAR,
        FieldFormat::Text,
    )
}

/// Build a FieldInfo for a float4 array column (vector embeddings).
pub fn float4_array_field(name: &str) -> FieldInfo {
    FieldInfo::new(
        name.to_owned(),
        None,
        None,
        Type::FLOAT4_ARRAY,
        FieldFormat::Text,
    )
}

/// Build a FieldInfo for a float8 array column.
pub fn float8_array_field(name: &str) -> FieldInfo {
    FieldInfo::new(
        name.to_owned(),
        None,
        None,
        Type::FLOAT8_ARRAY,
        FieldFormat::Text,
    )
}

/// Map a NodeDB/DataFusion field type name to a pgwire Type.
///
/// Used when constructing RowDescription from collection schemas.
pub fn type_name_to_pgwire(type_name: &str) -> Type {
    match type_name.to_lowercase().as_str() {
        "int" | "int4" | "integer" => Type::INT4,
        "int2" | "smallint" => Type::INT2,
        "int8" | "bigint" => Type::INT8,
        "float4" | "real" => Type::FLOAT4,
        "float8" | "double" | "double precision" => Type::FLOAT8,
        "text" | "string" => Type::TEXT,
        "varchar" => Type::VARCHAR,
        "bool" | "boolean" => Type::BOOL,
        "bytea" | "bytes" => Type::BYTEA,
        "json" => Type::JSON,
        "jsonb" => Type::JSONB,
        "timestamp" => Type::TIMESTAMP,
        "timestamptz" => Type::TIMESTAMPTZ,
        s if s.starts_with("vector") || s.starts_with("float4[]") => Type::FLOAT4_ARRAY,
        "float8[]" => Type::FLOAT8_ARRAY,
        _ => Type::TEXT, // Default fallback for unknown types.
    }
}

/// Create a notice response (WARNING level).
pub fn notice_warning(message: &str) -> pgwire::messages::response::NoticeResponse {
    pgwire::messages::response::NoticeResponse::from(pgwire::error::ErrorInfo::new(
        "WARNING".to_owned(),
        "01000".to_owned(),
        message.to_owned(),
    ))
}

/// Require that the identity is superuser or tenant_admin.
pub fn require_admin(identity: &AuthenticatedIdentity, action: &str) -> PgWireResult<()> {
    if identity.is_superuser || identity.has_role(&Role::TenantAdmin) {
        Ok(())
    } else {
        Err(sqlstate_error(
            "42501",
            &format!("permission denied: only superuser or tenant_admin can {action}"),
        ))
    }
}

/// Parse a role name string into a `Role`.
///
/// Known roles map to their enum variants; unknown names become `Role::Custom`.
pub fn parse_role(name: &str) -> Role {
    // Role::from_str is Infallible — unwrap is safe on Infallible.
    match name.parse() {
        Ok(role) => role,
        Err(e) => match e {},
    }
}

/// Decode a hex string into bytes.
///
/// Returns `None` if the input has an odd number of characters or contains
/// characters that are not valid hexadecimal digits.
pub fn hex_decode(s: &str) -> Option<Vec<u8>> {
    if !s.len().is_multiple_of(2) {
        return None;
    }
    let mut bytes = Vec::with_capacity(s.len() / 2);
    for i in (0..s.len()).step_by(2) {
        let byte = u8::from_str_radix(&s[i..i + 2], 16).ok()?;
        bytes.push(byte);
    }
    Some(bytes)
}

/// Map a Data Plane response status + error code to a SQLSTATE triple.
pub fn response_status_to_sqlstate(
    status: Status,
    error_code: &Option<ErrorCode>,
) -> Option<(&'static str, &'static str, String)> {
    match status {
        Status::Ok | Status::Partial => None,
        Status::Error => {
            if let Some(code) = error_code {
                Some(error_code_to_sqlstate(code))
            } else {
                Some(("ERROR", "XX000", "unknown data plane error".into()))
            }
        }
    }
}
