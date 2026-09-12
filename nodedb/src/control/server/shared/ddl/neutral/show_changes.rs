// SPDX-License-Identifier: BUSL-1.1

//! Protocol-neutral `SHOW CHANGES FOR <collection>` change-stream query.
//!
//! `SHOW CHANGES FOR <collection> [SINCE <timestamp>] [LIMIT <n>]` reads the
//! change stream for a collection and returns one row per recorded change. This
//! is the *query* surface over recorded changes — distinct from the change-stream
//! *DDL* (`CREATE/ALTER/DROP/SHOW CHANGE STREAM`) served by [`super::change_stream`].
//!
//! The handler builds [`DdlResult`] directly and carries no pgwire types.

use nodedb_sql::parser::preprocess::lex::find_ascii_case_insensitive;
use serde_json::{Map, Value as JsonValue};

use crate::control::change_stream::ReplayStart;
use crate::control::security::audit::ArcAuditEmitter;
use crate::control::security::identity::{AuthenticatedIdentity, Permission};
use crate::control::server::response_shape::types::{DdlColType, ShapedRows};
use crate::control::server::shared::authorization::authorize_collection;
use crate::control::state::SharedState;
use crate::types::DatabaseId;

use super::super::result::{DdlError, DdlResult};

/// Execute `SHOW CHANGES FOR <collection> [SINCE <timestamp>] [LIMIT <n>]`.
pub fn show_changes(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
    database_id: DatabaseId,
    sql: &str,
) -> Result<Vec<DdlResult>, DdlError> {
    if let Some(coll_token) =
        crate::control::server::shared::ddl::sql_parse::extract_collection_token_after(sql, " FOR ")
    {
        let coll_name =
            crate::control::server::shared::ddl::sql_parse::parse_ident_token(coll_token)?;
        let since_ms: u64 = if let Some(since_pos) = find_ascii_case_insensitive(sql, " SINCE ") {
            let since_str = sql[since_pos + 7..]
                .split_whitespace()
                .next()
                .unwrap_or("0");
            match crate::control::server::shared::ddl::sql_parse::parse_since_timestamp(since_str) {
                Ok(ms) => ms,
                Err(msg) => {
                    return Err(DdlError::new("22007", msg.to_string()));
                }
            }
        } else {
            // Default: last 24 hours of changes.
            let now_ms = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map(|d| d.as_millis() as u64)
                .unwrap_or(0);
            now_ms.saturating_sub(86_400 * 1000)
        };

        let limit = find_ascii_case_insensitive(sql, " LIMIT ")
            .and_then(|pos| sql[pos + 7..].split_whitespace().next())
            .and_then(|s| s.parse::<usize>().ok())
            .unwrap_or(1000)
            .min(10_000);

        let audit = ArcAuditEmitter(std::sync::Arc::clone(&state.audit));
        authorize_collection(
            identity,
            database_id,
            &coll_name,
            Permission::Read,
            &state.permissions,
            &state.roles,
            &audit,
        )
        .map_err(|error| {
            DdlError::new("42501", format!("permission denied: {}", error.resource()))
        })?;

        let changes = state
            .change_stream
            .query_changes_in_database(
                identity.tenant_id,
                database_id,
                Some(&coll_name),
                ReplayStart::Timestamp(since_ms),
                limit,
            )
            .map_err(|_| {
                DdlError::new("55000", "change stream replay cursor unexpectedly expired")
            })?
            .events;

        let columns = vec![
            "collection".to_string(),
            "operation".to_string(),
            "document_id".to_string(),
            "timestamp_ms".to_string(),
            "lsn".to_string(),
        ];
        let column_types = vec![
            DdlColType::Text,
            DdlColType::Text,
            DdlColType::Text,
            DdlColType::Text,
            DdlColType::Text,
        ];

        let mut rows = Vec::with_capacity(changes.len());
        for change in &changes {
            let mut row = Map::new();
            row.insert(
                "collection".to_string(),
                JsonValue::String(change.collection.clone()),
            );
            row.insert(
                "operation".to_string(),
                JsonValue::String(change.operation.as_str().to_string()),
            );
            row.insert(
                "document_id".to_string(),
                JsonValue::String(change.document_id.to_string()),
            );
            row.insert(
                "timestamp_ms".to_string(),
                JsonValue::String(change.timestamp_ms.to_string()),
            );
            row.insert(
                "lsn".to_string(),
                JsonValue::String(change.lsn.as_u64().to_string()),
            );
            rows.push(row);
        }

        return Ok(vec![DdlResult::Rows(ShapedRows {
            columns,
            column_types,
            rows,
            notice: None,
        })]);
    }

    Err(DdlError::new(
        "42601",
        "syntax: SHOW CHANGES FOR <collection> [SINCE <timestamp>]",
    ))
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use super::*;
    use crate::bridge::dispatch::Dispatcher;
    use crate::control::change_stream::{ChangeEvent, ChangeOperation};
    use crate::control::security::identity::{AuthMethod, DatabaseSet, Role};
    use crate::types::{Lsn, TenantId};
    use crate::wal::WalManager;
    use nodedb_types::RowIdentity;

    fn test_state() -> (tempfile::TempDir, Arc<SharedState>) {
        let dir = tempfile::tempdir().expect("create test directory");
        let wal = Arc::new(
            WalManager::open_for_testing(&dir.path().join("show-changes.wal"))
                .expect("open test WAL"),
        );
        let (dispatcher, _data_sides) = Dispatcher::new(1, 64);
        let state = SharedState::new(dispatcher, wal).expect("construct shared state");
        (dir, state)
    }

    fn identity(tenant_id: TenantId, roles: Vec<Role>) -> AuthenticatedIdentity {
        AuthenticatedIdentity::new_regular(
            42,
            "show-changes-reader",
            tenant_id,
            AuthMethod::Trust,
            roles,
            None,
            AuthenticatedIdentity::default_database_set(false),
        )
    }

    #[tokio::test]
    async fn show_changes_denies_custom_role_without_collection_read_grant() {
        let (_dir, state) = test_state();
        state.change_stream.publish(ChangeEvent {
            lsn: Lsn::new(1),
            tenant_id: TenantId::new(1),
            collection: "orders".into(),
            document_id: RowIdentity::from_user_key("hidden-order"),
            operation: ChangeOperation::Insert,
            timestamp_ms: 1,
            after: None,
        });
        let identity = identity(TenantId::new(1), vec![Role::Custom("auditor".into())]);

        let error = show_changes(
            &state,
            &identity,
            DatabaseId::DEFAULT,
            "SHOW CHANGES FOR orders SINCE 0",
        )
        .expect_err("custom role without a READ grant must be denied");

        assert_eq!(error.sqlstate, "42501");
    }

    #[tokio::test]
    async fn show_changes_isolates_events_by_selected_database() {
        let (_dir, state) = test_state();
        let second_database = DatabaseId::new(9);
        for (database_id, lsn, document_id) in [
            (DatabaseId::DEFAULT, Lsn::new(1), "default-database-order"),
            (second_database, Lsn::new(2), "second-database-order"),
        ] {
            state.change_stream.publish_in_database(
                database_id,
                ChangeEvent {
                    lsn,
                    tenant_id: TenantId::new(1),
                    collection: "orders".into(),
                    document_id: RowIdentity::from_user_key(document_id),
                    operation: ChangeOperation::Insert,
                    timestamp_ms: 1,
                    after: None,
                },
            );
        }
        let mut identity = identity(TenantId::new(1), vec![Role::ReadOnly]);
        identity.accessible_databases =
            DatabaseSet::Some(smallvec::smallvec![DatabaseId::DEFAULT, second_database]);

        for (database_id, expected_document_id) in [
            (DatabaseId::DEFAULT, "default-database-order"),
            (second_database, "second-database-order"),
        ] {
            let mut results = show_changes(
                &state,
                &identity,
                database_id,
                "SHOW CHANGES FOR orders SINCE 0",
            )
            .expect("readonly tenant identity may read changes from its selected database");
            let DdlResult::Rows(rows) = results.pop().expect("one rows result") else {
                panic!("SHOW CHANGES must return rows");
            };
            assert_eq!(
                rows.rows
                    .iter()
                    .map(|row| {
                        row.get("document_id")
                            .and_then(JsonValue::as_str)
                            .expect("document id column")
                    })
                    .collect::<Vec<_>>(),
                vec![expected_document_id],
                "SHOW CHANGES must return events only from the selected database"
            );
        }
    }

    #[tokio::test]
    async fn show_changes_returns_only_callers_tenant_events_from_shared_ring_buffer() {
        let (_dir, state) = test_state();
        state.change_stream.publish(ChangeEvent {
            lsn: Lsn::new(1),
            tenant_id: TenantId::new(1),
            collection: "orders".into(),
            document_id: RowIdentity::from_user_key("tenant-1-order"),
            operation: ChangeOperation::Insert,
            timestamp_ms: 1,
            after: None,
        });
        state.change_stream.publish(ChangeEvent {
            lsn: Lsn::new(2),
            tenant_id: TenantId::new(2),
            collection: "orders".into(),
            document_id: RowIdentity::from_user_key("tenant-2-order"),
            operation: ChangeOperation::Insert,
            timestamp_ms: 1,
            after: None,
        });
        let identity = identity(TenantId::new(1), vec![Role::ReadOnly]);

        let mut results = show_changes(
            &state,
            &identity,
            DatabaseId::DEFAULT,
            "SHOW CHANGES FOR orders SINCE 0",
        )
        .expect("readonly tenant identity may read changes");
        let DdlResult::Rows(rows) = results.pop().expect("one rows result") else {
            panic!("SHOW CHANGES must return rows");
        };
        let document_ids: Vec<_> = rows
            .rows
            .iter()
            .map(|row| {
                row.get("document_id")
                    .and_then(JsonValue::as_str)
                    .expect("document id column")
            })
            .collect();

        assert!(document_ids.contains(&"tenant-1-order"));
        assert!(!document_ids.contains(&"tenant-2-order"));
    }
}
