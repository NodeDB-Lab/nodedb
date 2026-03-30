//! `SHOW PROCEDURES` DDL handler.

use std::sync::Arc;

use futures::stream;
use pgwire::api::results::{DataRowEncoder, QueryResponse, Response};
use pgwire::error::PgWireResult;

use crate::control::security::identity::AuthenticatedIdentity;
use crate::control::state::SharedState;

use super::super::super::types::text_field;

/// Handle `SHOW PROCEDURES`
pub fn show_procedures(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
) -> PgWireResult<Vec<Response>> {
    let tenant_id = identity.tenant_id.as_u32();

    let schema = Arc::new(vec![
        text_field("name"),
        text_field("parameters"),
        text_field("max_iterations"),
        text_field("timeout_secs"),
        text_field("owner"),
    ]);

    let mut rows = Vec::new();
    if let Some(catalog) = state.credentials.catalog().as_ref()
        && let Ok(procs) = catalog.load_procedures_for_tenant(tenant_id)
    {
        for p in &procs {
            let params_str = p
                .parameters
                .iter()
                .map(|param| {
                    format!(
                        "{} {} {}",
                        param.direction.as_str(),
                        param.name,
                        param.data_type
                    )
                })
                .collect::<Vec<_>>()
                .join(", ");

            let mut encoder = DataRowEncoder::new(schema.clone());
            let _ = encoder.encode_field(&p.name);
            let _ = encoder.encode_field(&params_str);
            let _ = encoder.encode_field(&p.max_iterations.to_string());
            let _ = encoder.encode_field(&p.timeout_secs.to_string());
            let _ = encoder.encode_field(&p.owner);
            rows.push(Ok(encoder.take_row()));
        }
    }

    Ok(vec![Response::Query(QueryResponse::new(
        schema,
        stream::iter(rows),
    ))])
}
