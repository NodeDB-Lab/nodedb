//! Wire-format encode/decode helpers for PhysicalPlan.
//!
//! MessagePack encoding via zerompk. Used by the cluster layer to ship
//! physical plans over the wire as part of `ExecuteRequest` RPC.

use super::PhysicalPlan;
use crate::Error;

/// Encode a `PhysicalPlan` to MessagePack bytes.
pub fn encode(plan: &PhysicalPlan) -> Result<Vec<u8>, Error> {
    zerompk::to_msgpack_vec(plan).map_err(|e| Error::Internal {
        detail: format!("plan encode: {e}"),
    })
}

/// Decode a `PhysicalPlan` from MessagePack bytes.
pub fn decode(bytes: &[u8]) -> Result<PhysicalPlan, Error> {
    zerompk::from_msgpack(bytes).map_err(|e| Error::Internal {
        detail: format!("plan decode: {e}"),
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::bridge::physical_plan::{
        AggregateSpec, BalancedDef, ColumnarOp, CrdtOp, DocumentOp, EnforcementOptions, GraphOp,
        JoinProjection, KvOp, MetaOp, QueryOp, SpatialOp, SpatialPredicate, TextOp, TimeseriesOp,
        VectorOp,
    };
    use crate::engine::graph::algo::params::{AlgoParams, GraphAlgorithm};
    use crate::engine::graph::edge_store::Direction;
    use crate::engine::graph::traversal_options::GraphTraversalOptions;
    use crate::engine::timeseries::continuous_agg::{
        AggFunction, AggregateExpr, ContinuousAggregateDef, RefreshPolicy,
    };
    use crate::types::RequestId;

    fn roundtrip(plan: PhysicalPlan) {
        let encoded = encode(&plan).expect("encode failed");
        let decoded = decode(&encoded).expect("decode failed");
        assert_eq!(plan, decoded, "roundtrip mismatch");
    }

    #[test]
    fn roundtrip_vector() {
        roundtrip(PhysicalPlan::Vector(VectorOp::Search {
            collection: "embeddings".into(),
            query_vector: vec![0.1, 0.2, 0.3],
            top_k: 10,
            ef_search: 40,
            filter_bitmap: Some(vec![0x01, 0x02]),
            field_name: "vec".into(),
            rls_filters: vec![],
        }));
    }

    #[test]
    fn roundtrip_graph() {
        roundtrip(PhysicalPlan::Graph(GraphOp::Hop {
            start_nodes: vec!["alice".into()],
            edge_label: Some("follows".into()),
            direction: Direction::Out,
            depth: 2,
            options: GraphTraversalOptions::default(),
            rls_filters: vec![],
        }));
    }

    #[test]
    fn roundtrip_graph_algo() {
        roundtrip(PhysicalPlan::Graph(GraphOp::Algo {
            algorithm: GraphAlgorithm::PageRank,
            params: AlgoParams {
                collection: "social".into(),
                damping: Some(0.85),
                max_iterations: Some(20),
                ..Default::default()
            },
        }));
    }

    #[test]
    fn roundtrip_document() {
        roundtrip(PhysicalPlan::Document(DocumentOp::PointGet {
            collection: "users".into(),
            document_id: "user-1".into(),
            rls_filters: vec![],
        }));
    }

    #[test]
    fn roundtrip_document_register() {
        roundtrip(PhysicalPlan::Document(DocumentOp::Register {
            collection: "users".into(),
            indexes: vec![crate::bridge::physical_plan::RegisteredIndex {
                name: "email".into(),
                path: "$.email".into(),
                unique: false,
                case_insensitive: false,
                state: crate::bridge::physical_plan::RegisteredIndexState::Ready,
            }],
            crdt_enabled: false,
            storage_mode: crate::bridge::physical_plan::StorageMode::Schemaless,
            enforcement: Box::new(EnforcementOptions {
                append_only: true,
                balanced: Some(BalancedDef {
                    group_key_column: "journal_id".into(),
                    entry_type_column: "type".into(),
                    debit_value: "D".into(),
                    credit_value: "C".into(),
                    amount_column: "amount".into(),
                }),
                ..Default::default()
            }),
        }));
    }

    #[test]
    fn roundtrip_kv() {
        roundtrip(PhysicalPlan::Kv(KvOp::Put {
            collection: "sessions".into(),
            key: b"sess:abc".to_vec(),
            value: b"\x81\xa3foo\xa3bar".to_vec(),
            ttl_ms: 3_600_000,
        }));
    }

    #[test]
    fn roundtrip_text() {
        roundtrip(PhysicalPlan::Text(TextOp::Search {
            collection: "docs".into(),
            query: "hello world".into(),
            top_k: 5,
            fuzzy: true,
            rls_filters: vec![],
        }));
    }

    #[test]
    fn roundtrip_columnar() {
        roundtrip(PhysicalPlan::Columnar(ColumnarOp::Scan {
            collection: "metrics".into(),
            projection: vec!["cpu".into(), "mem".into()],
            limit: 1000,
            filters: vec![],
            rls_filters: vec![],
        }));
    }

    #[test]
    fn roundtrip_timeseries() {
        roundtrip(PhysicalPlan::Timeseries(TimeseriesOp::Scan {
            collection: "cpu_metrics".into(),
            time_range: (0, i64::MAX),
            projection: vec!["cpu".into()],
            limit: 500,
            filters: vec![],
            bucket_interval_ms: 60_000,
            group_by: vec!["host".into()],
            aggregates: vec![("avg".into(), "cpu".into())],
            gap_fill: "null".into(),
            computed_columns: vec![],
            rls_filters: vec![],
        }));
    }

    #[test]
    fn roundtrip_spatial() {
        roundtrip(PhysicalPlan::Spatial(SpatialOp::Scan {
            collection: "places".into(),
            field: "location".into(),
            predicate: SpatialPredicate::DWithin,
            query_geometry: b"{}".to_vec(),
            distance_meters: 500.0,
            attribute_filters: vec![],
            limit: 20,
            projection: vec!["name".into()],
            rls_filters: vec![],
        }));
    }

    #[test]
    fn roundtrip_crdt() {
        roundtrip(PhysicalPlan::Crdt(CrdtOp::Read {
            collection: "notes".into(),
            document_id: "note-1".into(),
        }));
    }

    #[test]
    fn roundtrip_query() {
        roundtrip(PhysicalPlan::Query(QueryOp::Aggregate {
            collection: "orders".into(),
            group_by: vec!["status".into()],
            aggregates: vec![AggregateSpec {
                function: "count".into(),
                alias: "cnt".into(),
                user_alias: None,
                field: "*".into(),
                expr: None,
            }],
            filters: vec![],
            having: vec![],
            limit: 100,
            sub_group_by: vec![],
            sub_aggregates: vec![],
        }));
    }

    #[test]
    fn roundtrip_query_hashjoin() {
        roundtrip(PhysicalPlan::Query(QueryOp::HashJoin {
            left_collection: "orders".into(),
            right_collection: "customers".into(),
            left_alias: None,
            right_alias: None,
            on: vec![("customer_id".into(), "id".into())],
            join_type: "inner".into(),
            limit: 50,
            post_group_by: vec![],
            post_aggregates: vec![],
            projection: vec![JoinProjection {
                source: "orders.id".into(),
                output: "order_id".into(),
            }],
            post_filters: vec![],
            inline_left: None,
            inline_right: None,
        }));
    }

    #[test]
    fn roundtrip_meta() {
        roundtrip(PhysicalPlan::Meta(MetaOp::Cancel {
            target_request_id: RequestId::new(42),
        }));
    }

    #[test]
    fn roundtrip_meta_continuous_agg() {
        roundtrip(PhysicalPlan::Meta(MetaOp::RegisterContinuousAggregate {
            def: ContinuousAggregateDef {
                name: "metrics_1m".into(),
                source: "raw_metrics".into(),
                bucket_interval: "1m".into(),
                bucket_interval_ms: 60_000,
                group_by: vec!["host".into()],
                aggregates: vec![AggregateExpr {
                    function: AggFunction::Avg,
                    source_column: "cpu".into(),
                    output_column: "cpu_avg".into(),
                }],
                refresh_policy: RefreshPolicy::OnFlush,
                retention_period_ms: 0,
                stale: false,
            },
        }));
    }
}
