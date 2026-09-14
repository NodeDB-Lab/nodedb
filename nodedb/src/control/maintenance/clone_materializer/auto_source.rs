// SPDX-License-Identifier: BUSL-1.1

//! Engine-aware source scan for `INSERT ... SELECT`.
//!
//! The document materializer reads the document store; a kv collection keeps
//! nothing there, so scanning a kv source with it materializes zero rows and
//! the statement reports `INSERT 0 0`. Route on the source collection's own
//! engine and normalize every page to the document entry shape
//! `(doc_id, source_surrogate, value_bytes)`: the copy pipeline ignores the
//! ids and shapes the body through its column map, and for kv the body is the
//! stored msgpack row, so expression cells evaluate exactly as they do over a
//! document source.

use nodedb_sql::types::EngineType;

use super::{document, kv};

pub(crate) async fn scan_source_page(
    state: &crate::control::state::SharedState,
    tenant_id: nodedb_types::TenantId,
    database_id: nodedb_types::DatabaseId,
    source_qualified: &str,
    cursor: &[u8],
    system_as_of_ms: Option<i64>,
    txn_id: Option<crate::types::TxnId>,
) -> crate::Result<(Vec<(String, u32, Vec<u8>)>, Vec<u8>)> {
    let catalog = state.credentials.catalog();
    let stored = catalog
        .get_collection(
            database_id,
            tenant_id.as_u64(),
            &crate::control::target_identity::bare_collection_name(database_id, source_qualified),
        )?
        .ok_or_else(|| crate::Error::CollectionNotFound {
            tenant_id,
            collection: source_qualified.to_string(),
        })?;
    let (engine, _, _) =
        crate::control::planner::catalog_adapter::type_convert::convert_collection_type(&stored);

    match engine {
        EngineType::DocumentSchemaless | EngineType::DocumentStrict => {
            document::scan_source_page(
                state,
                tenant_id,
                database_id,
                source_qualified,
                cursor,
                system_as_of_ms,
                txn_id,
            )
            .await
        }
        EngineType::KeyValue => {
            let (pairs, next) =
                kv::scan_source_page(state, tenant_id, database_id, source_qualified, cursor)
                    .await?;
            let entries = pairs
                .into_iter()
                .map(|(key, value)| (String::from_utf8_lossy(&key).into_owned(), 0, value))
                .collect();
            Ok((entries, next))
        }
        // Refused by name instead of silently copied as nothing; these
        // engines have their own materializers that are not wired here yet.
        EngineType::Columnar | EngineType::Timeseries | EngineType::Spatial | EngineType::Array => {
            Err(crate::Error::PlanError {
                detail: format!("INSERT ... SELECT from {engine:?} sources is not supported yet"),
            })
        }
    }
}
