// SPDX-License-Identifier: BUSL-1.1

//! Wire-enum stability snapshot tests.
//!
//! Each test asserts the exact JSON form for every known variant.
//! Because all enums here are `#[non_exhaustive]`, a `_ => panic!` wildcard
//! arm ensures a new variant causes a test failure at runtime until a
//! rename attribute and snapshot are added here.
//!
//! These tests cover Category B enums: types that cross the SPSC bridge as
//! MessagePack but also have serde JSON impls used for introspection,
//! catalog export, or SQL DDL round-trips.

use serde_json::json;

// ─── CollectionType ───────────────────────────────────────────────────────────

#[test]
fn collection_type_wire_forms() {
    use nodedb_types::collection::CollectionType;
    use nodedb_types::columnar::{ColumnarProfile, DocumentMode};

    let doc = CollectionType::Document(DocumentMode::Schemaless);
    let col = CollectionType::Columnar(ColumnarProfile::Plain);

    for v in [&doc, &col] {
        match v {
            CollectionType::Document(_) => {}
            CollectionType::Columnar(_) => {}
            CollectionType::KeyValue(_) => {}
        }
        serde_json::to_value(v).expect("CollectionType must serialize");
    }

    let v = serde_json::to_value(&doc).expect("serialize Document");
    assert_eq!(
        v["storage"], "Document",
        "CollectionType::Document wire tag"
    );

    let v = serde_json::to_value(&col).expect("serialize Columnar");
    assert_eq!(
        v["storage"], "Columnar",
        "CollectionType::Columnar wire tag"
    );
}

// ─── ColumnarProfile ──────────────────────────────────────────────────────────

#[test]
fn columnar_profile_wire_forms() {
    use nodedb_types::columnar::ColumnarProfile;

    let plain = ColumnarProfile::Plain;
    match &plain {
        ColumnarProfile::Plain => {}
        ColumnarProfile::Timeseries { .. } => {}
        ColumnarProfile::Spatial { .. } => {}
    }
    let v = serde_json::to_value(&plain).expect("serialize");
    assert_eq!(v["profile"], "Plain", "ColumnarProfile::Plain wire tag");

    let ts = ColumnarProfile::Timeseries {
        time_key: "ts".into(),
        interval: "1h".into(),
    };
    let v = serde_json::to_value(&ts).expect("serialize");
    assert_eq!(
        v["profile"], "Timeseries",
        "ColumnarProfile::Timeseries wire tag"
    );

    let sp = ColumnarProfile::Spatial {
        geometry_column: "geom".into(),
        auto_rtree: true,
        auto_geohash: false,
    };
    let v = serde_json::to_value(&sp).expect("serialize");
    assert_eq!(v["profile"], "Spatial", "ColumnarProfile::Spatial wire tag");
}

// ─── DocumentMode ─────────────────────────────────────────────────────────────

#[test]
fn document_mode_wire_forms() {
    use nodedb_types::columnar::{ColumnDef, ColumnType, DocumentMode, StrictSchema};

    let s = DocumentMode::Schemaless;
    match &s {
        DocumentMode::Schemaless => {}
        DocumentMode::Strict(_) => {}
    }
    let v = serde_json::to_value(&s).expect("serialize");
    assert_eq!(v["mode"], "Schemaless", "DocumentMode::Schemaless wire tag");

    let schema =
        StrictSchema::new(vec![ColumnDef::required("id", ColumnType::Int64)]).expect("schema");
    let strict = DocumentMode::Strict(schema);
    let v = serde_json::to_value(&strict).expect("serialize");
    assert_eq!(v["mode"], "Strict", "DocumentMode::Strict wire tag");
}

// ─── ColumnType ───────────────────────────────────────────────────────────────

#[test]
fn column_type_wire_forms() {
    use nodedb_types::columnar::ColumnType;

    let unit_cases: &[(ColumnType, &str)] = &[
        (ColumnType::Int64, "Int64"),
        (ColumnType::Float64, "Float64"),
        (ColumnType::String, "String"),
        (ColumnType::Bool, "Bool"),
        (ColumnType::Bytes, "Bytes"),
        (ColumnType::Timestamp, "Timestamp"),
        (ColumnType::Timestamptz, "Timestamptz"),
        (ColumnType::SystemTimestamp, "SystemTimestamp"),
        (ColumnType::Geometry, "Geometry"),
        (ColumnType::Uuid, "Uuid"),
        (ColumnType::Json, "Json"),
        (ColumnType::Ulid, "Ulid"),
        (ColumnType::Duration, "Duration"),
        (ColumnType::Array, "Array"),
        (ColumnType::Set, "Set"),
        (ColumnType::Regex, "Regex"),
        (ColumnType::Range, "Range"),
        (ColumnType::Record, "Record"),
        (ColumnType::SparseVector, "SparseVector"),
    ];

    for (ct, expected_tag) in unit_cases {
        // Wildcard arm required because ColumnType is #[non_exhaustive].
        match ct {
            ColumnType::Int64 => {}
            ColumnType::Float64 => {}
            ColumnType::String => {}
            ColumnType::Bool => {}
            ColumnType::Bytes => {}
            ColumnType::Timestamp => {}
            ColumnType::Timestamptz => {}
            ColumnType::SystemTimestamp => {}
            ColumnType::Decimal { .. } => {}
            ColumnType::Geometry => {}
            ColumnType::Vector(_) => {}
            ColumnType::Uuid => {}
            ColumnType::Json => {}
            ColumnType::Ulid => {}
            ColumnType::Duration => {}
            ColumnType::Array => {}
            ColumnType::Set => {}
            ColumnType::Regex => {}
            ColumnType::Range => {}
            ColumnType::Record => {}
            ColumnType::SparseVector => {}
            _ => panic!("unrecognized ColumnType — update wire_enum_lock.rs"),
        }
        let v = serde_json::to_value(ct).expect("serialize");
        assert_eq!(
            v["type"], *expected_tag,
            "ColumnType::{expected_tag} has wrong wire tag"
        );
    }

    // Parametric variants.
    let dec = ColumnType::Decimal {
        precision: 10,
        scale: 2,
    };
    let v = serde_json::to_value(dec).expect("serialize");
    assert_eq!(v["type"], "Decimal", "ColumnType::Decimal wire tag");

    let vec = ColumnType::Vector(128);
    let v = serde_json::to_value(vec).expect("serialize");
    assert_eq!(v["type"], "Vector", "ColumnType::Vector wire tag");
}

// ─── KvTtlPolicy ──────────────────────────────────────────────────────────────

#[test]
fn kv_ttl_policy_wire_forms() {
    use nodedb_types::kv::KvTtlPolicy;

    let fd = KvTtlPolicy::FixedDuration {
        duration_ms: 60_000,
    };
    match &fd {
        KvTtlPolicy::FixedDuration { .. } => {}
        KvTtlPolicy::FieldBased { .. } => {}
        _ => panic!("unrecognized KvTtlPolicy — update wire_enum_lock.rs"),
    }
    let v = serde_json::to_value(&fd).expect("serialize");
    assert_eq!(
        v["kind"], "FixedDuration",
        "KvTtlPolicy::FixedDuration wire tag"
    );

    let fb = KvTtlPolicy::FieldBased {
        field: "expires_at".into(),
        offset_ms: 0,
    };
    let v = serde_json::to_value(&fb).expect("serialize");
    assert_eq!(v["kind"], "FieldBased", "KvTtlPolicy::FieldBased wire tag");
}

// ─── PartitionState ───────────────────────────────────────────────────────────

#[test]
fn partition_state_wire_forms() {
    use nodedb_types::timeseries::partition::PartitionState;

    let cases: &[(PartitionState, &str)] = &[
        (PartitionState::Active, "Active"),
        (PartitionState::Sealed, "Sealed"),
        (PartitionState::Merging, "Merging"),
        (PartitionState::Merged, "Merged"),
        (PartitionState::Deleted, "Deleted"),
        (PartitionState::Archived, "Archived"),
    ];

    for (ps, expected) in cases {
        match ps {
            PartitionState::Active => {}
            PartitionState::Sealed => {}
            PartitionState::Merging => {}
            PartitionState::Merged => {}
            PartitionState::Deleted => {}
            PartitionState::Archived => {}
            _ => panic!("unrecognized PartitionState — update wire_enum_lock.rs"),
        }
        let v = serde_json::to_value(ps).expect("serialize");
        assert_eq!(v, json!(expected), "PartitionState::{expected} wire form");
    }
}

// ─── PartitionInterval ────────────────────────────────────────────────────────

#[test]
fn partition_interval_wire_forms() {
    use nodedb_types::timeseries::partition::PartitionInterval;

    let cases: Vec<(PartitionInterval, serde_json::Value)> = vec![
        (PartitionInterval::Month, json!("Month")),
        (PartitionInterval::Year, json!("Year")),
        (PartitionInterval::Unbounded, json!("Unbounded")),
        (PartitionInterval::Auto, json!("Auto")),
    ];

    for (pi, expected) in &cases {
        match pi {
            PartitionInterval::Duration(_) => {}
            PartitionInterval::Month => {}
            PartitionInterval::Year => {}
            PartitionInterval::Unbounded => {}
            PartitionInterval::Auto => {}
            _ => panic!("unrecognized PartitionInterval — update wire_enum_lock.rs"),
        }
        let v = serde_json::to_value(pi).expect("serialize");
        assert_eq!(v, *expected, "PartitionInterval wire form");
    }

    let dur = PartitionInterval::Duration(3_600_000);
    serde_json::to_value(&dur).expect("PartitionInterval::Duration must serialize");
}

// ─── DistanceMetric ───────────────────────────────────────────────────────────

#[test]
fn distance_metric_wire_forms() {
    use nodedb_types::vector_distance::DistanceMetric;

    let cases: &[(DistanceMetric, &str)] = &[
        (DistanceMetric::L2, "L2"),
        (DistanceMetric::Cosine, "Cosine"),
        (DistanceMetric::InnerProduct, "InnerProduct"),
        (DistanceMetric::Manhattan, "Manhattan"),
        (DistanceMetric::Chebyshev, "Chebyshev"),
        (DistanceMetric::Hamming, "Hamming"),
        (DistanceMetric::Jaccard, "Jaccard"),
        (DistanceMetric::Pearson, "Pearson"),
    ];

    for (dm, expected) in cases {
        match dm {
            DistanceMetric::L2 => {}
            DistanceMetric::Cosine => {}
            DistanceMetric::InnerProduct => {}
            DistanceMetric::Manhattan => {}
            DistanceMetric::Chebyshev => {}
            DistanceMetric::Hamming => {}
            DistanceMetric::Jaccard => {}
            DistanceMetric::Pearson => {}
            _ => panic!("unrecognized DistanceMetric — update wire_enum_lock.rs"),
        }
        let v = serde_json::to_value(dm).expect("serialize");
        assert_eq!(v, json!(expected), "DistanceMetric::{expected} wire form");
    }
}

// ─── VectorQuantization ───────────────────────────────────────────────────────

#[test]
fn vector_quantization_wire_forms() {
    use nodedb_types::vector_ann::VectorQuantization;

    let cases: &[(VectorQuantization, &str)] = &[
        (VectorQuantization::None, "None"),
        (VectorQuantization::Sq8, "Sq8"),
        (VectorQuantization::Pq, "Pq"),
        (VectorQuantization::RaBitQ, "RaBitQ"),
        (VectorQuantization::Bbq, "Bbq"),
        (VectorQuantization::Binary, "Binary"),
        (VectorQuantization::Ternary, "Ternary"),
        (VectorQuantization::Opq, "Opq"),
    ];

    for (vq, expected) in cases {
        match vq {
            VectorQuantization::None => {}
            VectorQuantization::Sq8 => {}
            VectorQuantization::Pq => {}
            VectorQuantization::RaBitQ => {}
            VectorQuantization::Bbq => {}
            VectorQuantization::Binary => {}
            VectorQuantization::Ternary => {}
            VectorQuantization::Opq => {}
            _ => panic!("unrecognized VectorQuantization — update wire_enum_lock.rs"),
        }
        let v = serde_json::to_value(vq).expect("serialize");
        assert_eq!(
            v,
            json!(expected),
            "VectorQuantization::{expected} wire form"
        );
    }
}

// ─── BatteryState ─────────────────────────────────────────────────────────────

#[test]
fn battery_state_wire_forms() {
    use nodedb_types::timeseries::series::BatteryState;

    let cases: &[(BatteryState, &str)] = &[
        (BatteryState::Normal, "Normal"),
        (BatteryState::Low, "Low"),
        (BatteryState::Charging, "Charging"),
        (BatteryState::Unknown, "Unknown"),
    ];

    for (bs, expected) in cases {
        match bs {
            BatteryState::Normal => {}
            BatteryState::Low => {}
            BatteryState::Charging => {}
            BatteryState::Unknown => {}
            _ => panic!("unrecognized BatteryState — update wire_enum_lock.rs"),
        }
        let v = serde_json::to_value(bs).expect("serialize");
        assert_eq!(v, json!(expected), "BatteryState::{expected} wire form");
    }
}

// ─── MultiVectorScoreMode ─────────────────────────────────────────────────────

#[test]
fn multi_vector_score_mode_wire_forms() {
    use nodedb_types::multi_vector::MultiVectorScoreMode;

    let cases: &[(MultiVectorScoreMode, &str)] = &[
        (MultiVectorScoreMode::MaxSim, "MaxSim"),
        (MultiVectorScoreMode::AvgSim, "AvgSim"),
        (MultiVectorScoreMode::SumSim, "SumSim"),
    ];

    for (m, expected) in cases {
        match m {
            MultiVectorScoreMode::MaxSim => {}
            MultiVectorScoreMode::AvgSim => {}
            MultiVectorScoreMode::SumSim => {}
            _ => panic!("unrecognized MultiVectorScoreMode — update wire_enum_lock.rs"),
        }
        let v = serde_json::to_value(m).expect("serialize");
        assert_eq!(
            v,
            json!(expected),
            "MultiVectorScoreMode::{expected} wire form"
        );
    }
}

// ─── PrimaryEngine ────────────────────────────────────────────────────────────

#[test]
fn primary_engine_wire_forms() {
    use nodedb_types::collection_config::PrimaryEngine;

    let cases: &[(PrimaryEngine, &str)] = &[
        (PrimaryEngine::Document, "Document"),
        (PrimaryEngine::Strict, "Strict"),
        (PrimaryEngine::KeyValue, "KeyValue"),
        (PrimaryEngine::Columnar, "Columnar"),
        (PrimaryEngine::Spatial, "Spatial"),
        (PrimaryEngine::Vector, "Vector"),
    ];

    for (pe, expected) in cases {
        match pe {
            PrimaryEngine::Document => {}
            PrimaryEngine::Strict => {}
            PrimaryEngine::KeyValue => {}
            PrimaryEngine::Columnar => {}
            PrimaryEngine::Spatial => {}
            PrimaryEngine::Vector => {}
            _ => panic!("unrecognized PrimaryEngine — update wire_enum_lock.rs"),
        }
        let v = serde_json::to_value(pe).expect("serialize");
        assert_eq!(v, json!(expected), "PrimaryEngine::{expected} wire form");
    }
}

// ─── PayloadIndexKind ─────────────────────────────────────────────────────────

#[test]
fn payload_index_kind_wire_forms() {
    use nodedb_types::collection_config::PayloadIndexKind;

    let cases: &[(PayloadIndexKind, &str)] = &[
        (PayloadIndexKind::Equality, "Equality"),
        (PayloadIndexKind::Range, "Range"),
        (PayloadIndexKind::Boolean, "Boolean"),
    ];

    for (pik, expected) in cases {
        match pik {
            PayloadIndexKind::Equality => {}
            PayloadIndexKind::Range => {}
            PayloadIndexKind::Boolean => {}
            _ => panic!("unrecognized PayloadIndexKind — update wire_enum_lock.rs"),
        }
        let v = serde_json::to_value(pik).expect("serialize");
        assert_eq!(v, json!(expected), "PayloadIndexKind::{expected} wire form");
    }
}

// ─── QueryMode (text_search) ──────────────────────────────────────────────────

#[test]
fn query_mode_wire_forms() {
    use nodedb_types::text_search::QueryMode;

    let cases: &[(QueryMode, &str)] = &[(QueryMode::Or, "Or"), (QueryMode::And, "And")];

    for (qm, expected) in cases {
        match qm {
            QueryMode::Or => {}
            QueryMode::And => {}
        }
        let v = serde_json::to_value(qm).expect("serialize");
        assert_eq!(v, json!(expected), "QueryMode::{expected} wire form");
    }
}

// ─── RefreshPolicy ────────────────────────────────────────────────────────────

#[test]
fn refresh_policy_wire_forms() {
    use nodedb_types::timeseries::continuous_agg::RefreshPolicy;

    let cases: Vec<(RefreshPolicy, serde_json::Value)> = vec![
        (RefreshPolicy::OnFlush, json!("on_flush")),
        (RefreshPolicy::OnSeal, json!("on_seal")),
        (RefreshPolicy::Manual, json!("manual")),
    ];

    for (rp, expected) in &cases {
        match rp {
            RefreshPolicy::OnFlush => {}
            RefreshPolicy::OnSeal => {}
            RefreshPolicy::Periodic(_) => {}
            RefreshPolicy::Manual => {}
            _ => panic!("unrecognized RefreshPolicy — update wire_enum_lock.rs"),
        }
        let v = serde_json::to_value(rp).expect("serialize");
        assert_eq!(v, *expected, "RefreshPolicy wire form");
    }

    let periodic = RefreshPolicy::Periodic(60_000);
    serde_json::to_value(&periodic).expect("RefreshPolicy::Periodic must serialize");
}
