// SPDX-License-Identifier: BUSL-1.1

//! Typed DDL arms for `StreamViewStmt`: change streams, consumer groups,
//! materialized views, and continuous aggregates.

use nodedb_sql::ddl_ast::statement::{NodedbStatement, StreamViewStmt};

use crate::control::security::identity::AuthenticatedIdentity;
use crate::control::state::SharedState;
use crate::types::DatabaseId;

use super::super::super::result::{DdlError, DdlResult};
use super::super::change_stream;
use super::super::consumer_group;
use super::super::continuous_agg;
use super::super::materialized_view;

pub(super) async fn try_typed(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
    sql: &str,
    database_id: DatabaseId,
    stmt: &NodedbStatement,
) -> Option<Result<Vec<DdlResult>, DdlError>> {
    match stmt {
        NodedbStatement::StreamView(StreamViewStmt::CreateChangeStream {
            name,
            collection,
            with_clause_raw,
        }) => Some(change_stream::create_change_stream(
            state,
            identity,
            name,
            collection,
            with_clause_raw,
        )),

        NodedbStatement::StreamView(StreamViewStmt::AlterChangeStream { name, action }) => Some(
            change_stream::alter_change_stream(state, identity, name, action),
        ),

        NodedbStatement::StreamView(StreamViewStmt::DropChangeStream { name, if_exists }) => {
            // IF EXISTS short-circuit folded from the pgwire guard: a DROP of a
            // non-existing change stream returns the tag before the token
            // handler runs. The `if_exists: false` case and the existing-stream
            // case fall through to `drop_change_stream`, which re-derives the
            // name / IF EXISTS from `parts` exactly as the pgwire streaming
            // string dispatch did.
            if *if_exists && !change_stream::change_stream_exists(state, identity, name) {
                return Some(Ok(vec![DdlResult::Status {
                    command: "DROP CHANGE STREAM".to_string(),
                    rows_affected: None,
                }]));
            }
            let parts: Vec<&str> = sql.split_whitespace().collect();
            Some(change_stream::drop_change_stream(state, identity, &parts))
        }

        NodedbStatement::StreamView(StreamViewStmt::CreateConsumerGroup {
            group_name,
            stream_name,
        }) => Some(consumer_group::create_consumer_group(
            state,
            identity,
            group_name,
            stream_name,
        )),

        NodedbStatement::StreamView(StreamViewStmt::DropConsumerGroup {
            name,
            stream,
            if_exists,
        }) => {
            // IF EXISTS short-circuit folded from the pgwire guard: a DROP of a
            // non-existing consumer group returns the tag before the token
            // handler runs. The `if_exists: false` case and the existing-group
            // case fall through to `drop_consumer_group`, which re-derives the
            // name / stream from `parts` exactly as the pgwire streaming string
            // dispatch did. The guard checks the in-memory group registry for the
            // identity tenant using the parsed name / stream verbatim.
            let tid = identity.tenant_id.as_u64();
            if *if_exists && state.group_registry.get(tid, stream, name).is_none() {
                return Some(Ok(vec![DdlResult::Status {
                    command: "DROP CONSUMER GROUP".to_string(),
                    rows_affected: None,
                }]));
            }
            let parts: Vec<&str> = sql.split_whitespace().collect();
            Some(consumer_group::drop_consumer_group(state, identity, &parts))
        }

        NodedbStatement::StreamView(StreamViewStmt::CreateMaterializedView {
            name,
            source,
            query_sql,
            refresh_mode,
        }) => Some(
            materialized_view::create_materialized_view(
                state,
                identity,
                name,
                source,
                query_sql,
                refresh_mode,
            )
            .await,
        ),

        NodedbStatement::StreamView(StreamViewStmt::DropMaterializedView { name, if_exists }) => {
            // IF EXISTS short-circuit folded from the pgwire guard: a DROP of a
            // non-existing materialized view returns the tag before the token
            // handler runs. The existence check reads the in-memory registry
            // (`mv_registry`) for the identity tenant exactly as the pgwire guard
            // did. The `if_exists: false` case and the existing-view case fall
            // through to `drop_materialized_view`, which re-derives the name / IF
            // EXISTS from `parts` (and runs its own catalog-based existence check)
            // exactly as the pgwire admin string dispatch did.
            if *if_exists && !materialized_view::materialized_view_exists(state, identity, name) {
                return Some(Ok(vec![DdlResult::Status {
                    command: "DROP MATERIALIZED VIEW".to_string(),
                    rows_affected: None,
                }]));
            }
            let parts: Vec<&str> = sql.split_whitespace().collect();
            Some(materialized_view::drop_materialized_view(
                state, identity, &parts,
            ))
        }

        NodedbStatement::StreamView(StreamViewStmt::CreateContinuousAggregate {
            name,
            source,
            bucket_raw,
            aggregate_exprs_raw,
            group_by,
            with_clause_raw,
        }) => Some(
            continuous_agg::create_continuous_aggregate(
                state,
                identity,
                &continuous_agg::CreateContinuousAggregateRequest {
                    name,
                    source,
                    bucket_raw,
                    aggregate_exprs_raw,
                    group_by,
                    with_clause_raw,
                    database_id,
                },
            )
            .await,
        ),

        NodedbStatement::StreamView(StreamViewStmt::DropContinuousAggregate {
            name,
            if_exists,
        }) => {
            // IF EXISTS short-circuit folded from the pgwire guard: a DROP of a
            // non-existing continuous aggregate returns the tag before the token
            // handler runs. The existence check reads the in-memory registry
            // (`mv_registry`) for the identity tenant exactly as the pgwire guard
            // did. The `if_exists: false` case and the existing-aggregate case
            // fall through to `drop_continuous_aggregate`, which re-derives the
            // name from `parts[3]` exactly as the pgwire admin string dispatch
            // did.
            if *if_exists
                && !continuous_agg::continuous_aggregate_exists(state, identity, database_id, name)
            {
                return Some(Ok(vec![DdlResult::Status {
                    command: "DROP CONTINUOUS AGGREGATE".to_string(),
                    rows_affected: None,
                }]));
            }
            let parts: Vec<&str> = sql.split_whitespace().collect();
            Some(
                continuous_agg::drop_continuous_aggregate(state, identity, database_id, &parts)
                    .await,
            )
        }

        _ => None,
    }
}
