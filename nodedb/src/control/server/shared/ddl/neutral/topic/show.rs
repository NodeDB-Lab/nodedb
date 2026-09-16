// SPDX-License-Identifier: BUSL-1.1

//! Protocol-neutral `SHOW TOPICS` DDL handler.
//!
//! Ported from the pgwire `ddl::topic::show` handler. The tenant scoping, the
//! per-topic buffered-event counting, and the column layout are preserved
//! verbatim; only the result construction changed from a pgwire `QueryResponse`
//! to the protocol-neutral [`DdlResult::Rows`].

use serde_json::{Map, Value as JsonValue};

use crate::control::security::identity::AuthenticatedIdentity;
use crate::control::server::response_shape::types::ShapedRows;
use crate::control::state::SharedState;
use crate::types::DatabaseId;

use super::super::super::result::{DdlError, DdlResult};

pub fn show_topics(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
    database_id: DatabaseId,
) -> Result<Vec<DdlResult>, DdlError> {
    let tenant_id = identity.tenant_id.as_u64();

    let columns = vec![
        "name".to_string(),
        "retention_secs".to_string(),
        "buffered_events".to_string(),
        "owner".to_string(),
    ];

    let topics = state
        .ep_topic_registry
        .list_for_database_tenant(database_id, tenant_id);
    let mut rows = Vec::with_capacity(topics.len());

    for t in &topics {
        let buffer_key = format!("topic:{}", t.name);
        let buffered = state
            .cdc_router
            .get_buffer(database_id, tenant_id, &buffer_key)
            .map(|b| b.len())
            .unwrap_or(0);

        let mut row = Map::new();
        row.insert("name".to_string(), JsonValue::String(t.name.clone()));
        row.insert(
            "retention_secs".to_string(),
            JsonValue::String(t.retention.max_age_secs.to_string()),
        );
        row.insert(
            "buffered_events".to_string(),
            JsonValue::String(buffered.to_string()),
        );
        row.insert("owner".to_string(), JsonValue::String(t.owner.clone()));
        rows.push(row);
    }

    Ok(vec![DdlResult::Rows(ShapedRows::text_rows(columns, rows))])
}
