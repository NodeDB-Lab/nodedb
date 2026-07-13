// SPDX-License-Identifier: BUSL-1.1

//! `LIVE SELECT` — create a change-stream subscription bound to the
//! current pgwire session.

use std::sync::Arc;

use pgwire::api::results::{DataRowEncoder, QueryResponse, Response};
use pgwire::error::{ErrorInfo, PgWireError, PgWireResult};

use crate::control::security::audit::ArcAuditEmitter;
use crate::control::security::identity::{AuthenticatedIdentity, Permission};
use crate::control::server::shared::authorization::authorize_collection;

use super::super::types::text_field;
use super::auth::pgwire_authorization_error;
use super::core::NodeDbPgHandler;

impl NodeDbPgHandler {
    /// Handle `LIVE SELECT`: subscribe to the collection's change stream
    /// and return a single-row description of the new subscription.
    pub(super) fn handle_live_select(
        &self,
        identity: &AuthenticatedIdentity,
        addr: &std::net::SocketAddr,
        sql: &str,
    ) -> PgWireResult<Vec<Response>> {
        let coll_name =
            crate::control::server::shared::ddl::sql_parse::extract_collection_after(sql, " FROM ")
                .ok_or_else(|| {
                    PgWireError::UserError(Box::new(ErrorInfo::new(
                        "ERROR".to_owned(),
                        "42601".to_owned(),
                        "syntax: LIVE SELECT [*|fields] FROM <collection> [WHERE ...]".to_owned(),
                    )))
                })?;

        let database_id = self
            .sessions
            .get_current_database(addr)
            .unwrap_or(crate::types::DatabaseId::DEFAULT);
        let emitter = ArcAuditEmitter(Arc::clone(&self.state.audit));
        authorize_collection(
            identity,
            database_id,
            &coll_name,
            Permission::Read,
            &self.state.permissions,
            &self.state.roles,
            &emitter,
        )
        .map_err(pgwire_authorization_error)?;

        let tenant_id = identity.tenant_id;
        let sub = self
            .state
            .change_stream
            .subscribe(Some(coll_name.clone()), Some(tenant_id));
        let sub_id = sub.id;
        let channel = format!("live_{coll_name}");

        self.sessions
            .add_live_subscription(addr, channel.clone(), sub);

        tracing::info!(
            sub_id,
            collection = coll_name,
            channel,
            "LIVE SELECT subscription created"
        );

        use futures::stream;
        let schema = Arc::new(vec![
            text_field("subscription_id"),
            text_field("channel"),
            text_field("collection"),
            text_field("status"),
        ]);
        let mut encoder = DataRowEncoder::new(schema.clone());
        let _ = encoder.encode_field(&sub_id.to_string());
        let _ = encoder.encode_field(&channel);
        let _ = encoder.encode_field(&coll_name);
        let _ = encoder.encode_field(&"active");
        let row = encoder.take_row();
        Ok(vec![Response::Query(QueryResponse::new(
            schema,
            stream::iter(vec![Ok(row)]),
        ))])
    }
}
