// SPDX-License-Identifier: BUSL-1.1

//! Engine routing for the row-shaped write converters.

use nodedb_sql::types::EngineType;

/// The lowering a statement's rows take.
pub(super) enum WriteRoute {
    /// One task per row, carrying a `DocumentOp` or a `CrdtOp`.
    Document,
    /// One batched `ColumnarOp` task for the whole statement.
    ColumnarFamily,
}

/// Resolve an INSERT's route, refusing engines that lower elsewhere.
///
/// Routing runs once per statement, ahead of DEFAULT materialization, so a
/// refused engine never allocates a sequence value it discards.
pub(super) fn insert_route(engine: &EngineType, collection: &str) -> crate::Result<WriteRoute> {
    match engine {
        EngineType::DocumentSchemaless | EngineType::DocumentStrict => Ok(WriteRoute::Document),
        EngineType::Columnar | EngineType::Spatial => Ok(WriteRoute::ColumnarFamily),
        EngineType::KeyValue => Err(crate::Error::PlanError {
            detail: "KV INSERT must use SqlPlan::KvInsert path".into(),
        }),
        EngineType::Timeseries => Err(crate::Error::PlanError {
            detail: format!(
                "INSERT into '{collection}': timeseries collections use TimeseriesIngest, not Insert"
            ),
        }),
        EngineType::Array => Err(crate::Error::PlanError {
            detail: format!(
                "INSERT into '{collection}': array engine uses INSERT INTO ARRAY syntax"
            ),
        }),
    }
}

/// Resolve an UPSERT's route, refusing engines with no upsert lowering.
///
/// Runs ahead of DEFAULT materialization for the same reason as
/// [`insert_route`].
pub(super) fn upsert_route(engine: &EngineType, collection: &str) -> crate::Result<WriteRoute> {
    match engine {
        EngineType::DocumentSchemaless | EngineType::DocumentStrict => Ok(WriteRoute::Document),
        EngineType::Columnar | EngineType::Spatial => Ok(WriteRoute::ColumnarFamily),
        EngineType::Timeseries | EngineType::KeyValue | EngineType::Array => {
            Err(crate::Error::PlanError {
                detail: format!(
                    "UPSERT into '{collection}': engine type {engine:?} does not support upsert"
                ),
            })
        }
    }
}
