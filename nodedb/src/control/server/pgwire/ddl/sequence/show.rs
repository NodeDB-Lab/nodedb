//! `SHOW SEQUENCES` and `DESCRIBE SEQUENCE` handlers.

use std::sync::Arc;

use pgwire::api::results::{DataRowEncoder, QueryResponse, Response};
use pgwire::error::{ErrorInfo, PgWireError, PgWireResult};

use crate::control::security::identity::AuthenticatedIdentity;
use crate::control::state::SharedState;

use super::super::super::types::text_field;

pub fn show_sequences(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
) -> PgWireResult<Vec<Response>> {
    let tenant_id = identity.tenant_id.as_u32();
    let sequences = state.sequence_registry.list(tenant_id);

    let schema = Arc::new(vec![
        text_field("name"),
        text_field("current_value"),
        text_field("called"),
    ]);

    let mut rows = Vec::with_capacity(sequences.len());
    for (name, current_value, is_called) in &sequences {
        let mut encoder = DataRowEncoder::new(schema.clone());
        encoder.encode_field(name)?;
        encoder.encode_field(&current_value.to_string())?;
        encoder.encode_field(&is_called.to_string())?;
        rows.push(Ok(encoder.take_row()));
    }

    Ok(vec![Response::Query(QueryResponse::new(
        schema,
        futures::stream::iter(rows),
    ))])
}

pub fn describe_sequence(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
    name: &str,
) -> PgWireResult<Vec<Response>> {
    let tenant_id = identity.tenant_id.as_u32();
    let name = name.to_lowercase();

    let def = state
        .sequence_registry
        .get_def(tenant_id, &name)
        .ok_or_else(|| {
            PgWireError::UserError(Box::new(ErrorInfo::new(
                "ERROR".to_owned(),
                "42P01".to_owned(),
                format!("sequence \"{name}\" does not exist"),
            )))
        })?;

    let schema = Arc::new(vec![text_field("property"), text_field("value")]);

    let format_str = def
        .format_template
        .as_ref()
        .map(|_| "(defined)")
        .unwrap_or("(none)");

    let reset_str = match def.reset_scope {
        crate::control::sequence::ResetScope::Never => "NEVER",
        crate::control::sequence::ResetScope::Yearly => "YEARLY",
        crate::control::sequence::ResetScope::Monthly => "MONTHLY",
        crate::control::sequence::ResetScope::Quarterly => "QUARTERLY",
        crate::control::sequence::ResetScope::Daily => "DAILY",
    };

    let props = [
        ("name", def.name.as_str()),
        ("start_value", &def.start_value.to_string()),
        ("increment", &def.increment.to_string()),
        ("min_value", &def.min_value.to_string()),
        ("max_value", &def.max_value.to_string()),
        ("cycle", &def.cycle.to_string()),
        ("cache_size", &def.cache_size.to_string()),
        ("format", format_str),
        ("reset_scope", reset_str),
        ("gap_free", &def.gap_free.to_string()),
    ];

    let mut rows = Vec::with_capacity(props.len());
    for (k, v) in &props {
        let mut encoder = DataRowEncoder::new(schema.clone());
        encoder.encode_field(&k.to_string())?;
        encoder.encode_field(&v.to_string())?;
        rows.push(Ok(encoder.take_row()));
    }

    Ok(vec![Response::Query(QueryResponse::new(
        schema,
        futures::stream::iter(rows),
    ))])
}
