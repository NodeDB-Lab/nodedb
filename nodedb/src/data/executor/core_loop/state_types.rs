// SPDX-License-Identifier: BUSL-1.1

//! Map type aliases for the `CoreLoop` fields whose spelled-out types run long.

use std::collections::{HashMap, HashSet};

use nodedb_types::DatabaseId;

use super::doc_config_key::DocConfigKey;
use crate::data::executor::handlers::aggregate::AggregateCacheEntry;
use crate::data::executor::handlers::transaction::overlay::{
    ArrayTxnOverlay, GraphTxnOverlay, TxnOverlay,
};
use crate::data::executor::handlers::transaction::savepoint_marks::TxnSavepoints;
use crate::engine::document::store::CollectionConfig;
use crate::engine::timeseries::columnar_memtable::ColumnarMemtable;
use crate::engine::timeseries::last_value_cache::LastValueCache;
use crate::engine::timeseries::partition_registry::PartitionRegistry;
use crate::types::{TenantId, TxnId};

/// Reverse map from an indexed document to the HNSW vector ID it produced.
pub(in crate::data::executor) type VectorDocMap = HashMap<
    (
        DatabaseId,
        TenantId,
        String,
        String,
        nodedb_types::StorageKey,
    ),
    u32,
>;

/// Nodes whose rows a delete removed, per `(database, tenant)`, then by collection.
pub(in crate::data::executor) type DeletedNodes =
    HashMap<(DatabaseId, TenantId), HashMap<String, HashSet<String>>>;

/// Incremental aggregate cache entries, per `(database, tenant, key)`.
pub(in crate::data::executor) type AggregateCache =
    HashMap<(DatabaseId, TenantId, String), AggregateCacheEntry>;

/// Columnar timeseries memtables, per `(database, tenant, collection)`.
pub(in crate::data::executor) type ColumnarMemtables =
    HashMap<(DatabaseId, TenantId, String), ColumnarMemtable>;

/// Last-value caches, per `(database, tenant, collection)`.
pub(in crate::data::executor) type TsLastValueCaches =
    HashMap<(DatabaseId, TenantId, String), LastValueCache>;

/// Timeseries partition registries, per `(database, tenant, collection)`.
pub(in crate::data::executor) type TsRegistries =
    HashMap<(DatabaseId, TenantId, String), PartitionRegistry>;

/// Document index configs, keyed by `DocConfigKey`.
pub(in crate::data::executor) type DocConfigs = hashbrown::HashMap<DocConfigKey, CollectionConfig>;

/// Staging overlay of each in-flight transaction.
pub(in crate::data::executor) type TxnOverlays = HashMap<TxnId, TxnOverlay>;

/// Graph staging overlay of each in-flight transaction.
pub(in crate::data::executor) type GraphTxnOverlays = HashMap<TxnId, GraphTxnOverlay>;

/// Array staging overlay of each in-flight transaction.
pub(in crate::data::executor) type ArrayTxnOverlays = HashMap<TxnId, ArrayTxnOverlay>;

/// Savepoint records of each in-flight transaction.
pub(in crate::data::executor) type TxnSavepointsMap = HashMap<TxnId, TxnSavepoints>;
