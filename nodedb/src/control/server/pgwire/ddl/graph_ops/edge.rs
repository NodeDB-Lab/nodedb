//! Edge mutation handlers: GRAPH INSERT EDGE, GRAPH DELETE EDGE,
//! GRAPH LABEL / GRAPH UNLABEL.
//!
//! Each function receives already-parsed typed fields from
//! `nodedb_sql::ddl_ast::NodedbStatement`. Raw-SQL tokenising lives
//! in the AST parser — handlers never touch `&str` parse paths.

use pgwire::api::results::{Response, Tag};
use pgwire::error::PgWireResult;

use nodedb_sql::ddl_ast::GraphProperties;

use crate::bridge::envelope::PhysicalPlan;
use crate::bridge::physical_plan::GraphOp;
use crate::control::security::identity::AuthenticatedIdentity;
use crate::control::server::dispatch_utils;
use crate::control::server::pgwire::types::sqlstate_error;
use crate::control::state::SharedState;
use crate::types::VShardId;

/// `GRAPH INSERT EDGE FROM '<src>' TO '<dst>' TYPE '<label>' [PROPERTIES '<json>' | { ... }]`
pub async fn insert_edge(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
    src: String,
    dst: String,
    label: String,
    properties: GraphProperties,
) -> PgWireResult<Vec<Response>> {
    if src.is_empty() || dst.is_empty() || label.is_empty() {
        return Err(sqlstate_error(
            "42601",
            "GRAPH INSERT EDGE requires FROM, TO, and TYPE",
        ));
    }
    let properties_json = properties_to_json(properties)?;
    let tenant_id = identity.tenant_id;
    let vshard_id = VShardId::from_key(src.as_bytes());

    let plan = PhysicalPlan::Graph(GraphOp::EdgePut {
        src_id: src,
        label,
        dst_id: dst,
        properties: properties_json.into_bytes(),
    });

    dispatch_utils::wal_append_if_write(&state.wal, tenant_id, vshard_id, &plan)
        .map_err(|e| sqlstate_error("XX000", &e.to_string()))?;

    match dispatch_utils::dispatch_to_data_plane(state, tenant_id, vshard_id, plan, 0).await {
        Ok(_) => Ok(vec![Response::Execution(Tag::new("INSERT EDGE"))]),
        Err(e) => Err(sqlstate_error("XX000", &e.to_string())),
    }
}

/// `GRAPH DELETE EDGE FROM '<src>' TO '<dst>' TYPE '<label>'`
pub async fn delete_edge(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
    src: String,
    dst: String,
    label: String,
) -> PgWireResult<Vec<Response>> {
    if src.is_empty() || dst.is_empty() || label.is_empty() {
        return Err(sqlstate_error(
            "42601",
            "GRAPH DELETE EDGE requires FROM, TO, and TYPE",
        ));
    }
    let tenant_id = identity.tenant_id;
    let vshard_id = VShardId::from_key(src.as_bytes());

    let plan = PhysicalPlan::Graph(GraphOp::EdgeDelete {
        src_id: src,
        label,
        dst_id: dst,
    });

    dispatch_utils::wal_append_if_write(&state.wal, tenant_id, vshard_id, &plan)
        .map_err(|e| sqlstate_error("XX000", &e.to_string()))?;

    match dispatch_utils::dispatch_to_data_plane(state, tenant_id, vshard_id, plan, 0).await {
        Ok(_) => Ok(vec![Response::Execution(Tag::new("DELETE EDGE"))]),
        Err(e) => Err(sqlstate_error("XX000", &e.to_string())),
    }
}

/// `GRAPH LABEL '<node_id>' AS '<label>' [, '<label2>']`
/// `GRAPH UNLABEL '<node_id>' AS '<label>'`
pub async fn set_node_labels(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
    node_id: String,
    labels: Vec<String>,
    remove: bool,
) -> PgWireResult<Vec<Response>> {
    if node_id.is_empty() {
        return Err(sqlstate_error(
            "42601",
            "GRAPH LABEL/UNLABEL requires a quoted node id",
        ));
    }
    if labels.is_empty() {
        return Err(sqlstate_error(
            "42601",
            "missing AS '<label>' [, '<label2>']",
        ));
    }

    let tenant_id = identity.tenant_id;
    let vshard_id = VShardId::from_key(node_id.as_bytes());

    let plan = if remove {
        PhysicalPlan::Graph(GraphOp::RemoveNodeLabels { node_id, labels })
    } else {
        PhysicalPlan::Graph(GraphOp::SetNodeLabels { node_id, labels })
    };

    dispatch_utils::wal_append_if_write(&state.wal, tenant_id, vshard_id, &plan)
        .map_err(|e| sqlstate_error("XX000", &e.to_string()))?;

    match dispatch_utils::dispatch_to_data_plane(state, tenant_id, vshard_id, plan, 0).await {
        Ok(_) => {
            let tag = if remove { "UNLABEL" } else { "LABEL" };
            Ok(vec![Response::Execution(Tag::new(tag))])
        }
        Err(e) => Err(sqlstate_error("XX000", &e.to_string())),
    }
}

/// Convert a parsed `PROPERTIES` clause to the JSON string stored
/// in `GraphOp::EdgePut`. Object-literal forms go through the
/// existing `nodedb_sql::parser::object_literal::parse_object_literal`
/// so the type coercions (numbers, bools, nested objects) match
/// every other object-literal ingress (INSERT { ... }, UPSERT).
fn properties_to_json(properties: GraphProperties) -> PgWireResult<String> {
    match properties {
        GraphProperties::None => Ok(String::new()),
        GraphProperties::Quoted(s) => Ok(s),
        GraphProperties::Object(obj_str) => {
            match nodedb_sql::parser::object_literal::parse_object_literal(&obj_str) {
                Some(Ok(fields)) => Ok(sonic_rs::to_string(&nodedb_types::Value::Object(fields))
                    .unwrap_or_else(|_| "{}".to_string())),
                Some(Err(msg)) => Err(sqlstate_error(
                    "42601",
                    &format!("PROPERTIES object literal error: {msg}"),
                )),
                None => Ok(String::new()),
            }
        }
    }
}
