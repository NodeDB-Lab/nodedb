//! `SHOW CHANGE STREAMS` DDL handler.

use std::sync::Arc;

use futures::stream;
use pgwire::api::results::{DataRowEncoder, QueryResponse, Response};
use pgwire::error::PgWireResult;

use crate::control::security::identity::AuthenticatedIdentity;
use crate::control::state::SharedState;

use super::super::super::types::text_field;

/// Handle `SHOW CHANGE STREAMS`
pub fn show_change_streams(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
) -> PgWireResult<Vec<Response>> {
    let tenant_id = identity.tenant_id.as_u32();

    let schema = Arc::new(vec![
        text_field("name"),
        text_field("collection"),
        text_field("include"),
        text_field("format"),
        text_field("owner"),
        text_field("created_at"),
    ]);

    let streams = state.stream_registry.list_for_tenant(tenant_id);

    let mut rows = Vec::new();
    for s in &streams {
        let mut encoder = DataRowEncoder::new(schema.clone());
        let _ = encoder.encode_field(&s.name);
        let _ = encoder.encode_field(&s.collection);
        let _ = encoder.encode_field(&s.op_filter.display());
        let _ = encoder.encode_field(&s.format.as_str().to_string());
        let _ = encoder.encode_field(&s.owner);
        let _ = encoder.encode_field(&s.created_at.to_string());
        rows.push(Ok(encoder.take_row()));
    }

    Ok(vec![Response::Query(QueryResponse::new(
        schema,
        stream::iter(rows),
    ))])
}
