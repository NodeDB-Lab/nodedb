//! Collection type enum shared between Origin and Lite.
//!
//! Determines routing, storage format, and query execution strategy.

use serde::{Deserialize, Serialize};

use crate::columnar::{ColumnarProfile, DocumentMode, StrictSchema};
use crate::kv::{KV_DEFAULT_INLINE_THRESHOLD, KvConfig, KvTtlPolicy};

/// The type of a collection, determining its storage engine and query behavior.
///
/// Three top-level modes:
/// - `Document`: B-tree storage in redb (schemaless MessagePack or strict Binary Tuples).
/// - `Columnar`: Compressed segment files with profile specialization (plain, timeseries, spatial).
/// - `KeyValue`: Hash-indexed O(1) point lookups with typed value fields (Binary Tuples).
#[derive(
    Debug,
    Clone,
    PartialEq,
    Eq,
    Serialize,
    Deserialize,
    zerompk::ToMessagePack,
    zerompk::FromMessagePack,
)]
#[serde(tag = "storage")]
pub enum CollectionType {
    /// Document storage in redb B-tree.
    /// Schemaless (MessagePack) or strict (Binary Tuples).
    Document(DocumentMode),
    /// Columnar storage in compressed segment files.
    /// Profile determines constraints and specialized behavior.
    Columnar(ColumnarProfile),
    /// Key-Value storage with hash-indexed primary key.
    /// O(1) point lookups, optional TTL, optional secondary indexes.
    /// Value fields use Binary Tuple codec (same as strict mode) for O(1) field extraction.
    KeyValue(KvConfig),
}

impl Default for CollectionType {
    fn default() -> Self {
        Self::Document(DocumentMode::default())
    }
}

impl CollectionType {
    /// Schemaless document (default, backward compatible).
    pub fn document() -> Self {
        Self::Document(DocumentMode::Schemaless)
    }

    /// Strict document with schema.
    pub fn strict(schema: StrictSchema) -> Self {
        Self::Document(DocumentMode::Strict(schema))
    }

    /// Plain columnar (general analytics).
    pub fn columnar() -> Self {
        Self::Columnar(ColumnarProfile::Plain)
    }

    /// Columnar with timeseries profile.
    pub fn timeseries(time_key: impl Into<String>, interval: impl Into<String>) -> Self {
        Self::Columnar(ColumnarProfile::Timeseries {
            time_key: time_key.into(),
            interval: interval.into(),
        })
    }

    /// Columnar with spatial profile.
    pub fn spatial(geometry_column: impl Into<String>) -> Self {
        Self::Columnar(ColumnarProfile::Spatial {
            geometry_column: geometry_column.into(),
            auto_rtree: true,
            auto_geohash: true,
        })
    }

    /// Key-Value collection with typed schema and optional TTL.
    ///
    /// The schema MUST contain exactly one PRIMARY KEY column (the hash key).
    /// Remaining columns are value fields encoded as Binary Tuples.
    pub fn kv(schema: StrictSchema) -> Self {
        Self::KeyValue(KvConfig {
            schema,
            ttl: None,
            capacity_hint: 0,
            inline_threshold: KV_DEFAULT_INLINE_THRESHOLD,
        })
    }

    /// Key-Value collection with TTL policy.
    pub fn kv_with_ttl(schema: StrictSchema, ttl: KvTtlPolicy) -> Self {
        Self::KeyValue(KvConfig {
            schema,
            ttl: Some(ttl),
            capacity_hint: 0,
            inline_threshold: KV_DEFAULT_INLINE_THRESHOLD,
        })
    }

    pub fn is_document(&self) -> bool {
        matches!(self, Self::Document(_))
    }

    /// Returns `true` for any columnar-family type (Plain, Timeseries, Spatial).
    /// Use `is_plain_columnar()` to check for plain columnar only.
    pub fn is_columnar_family(&self) -> bool {
        matches!(self, Self::Columnar(_))
    }

    pub fn is_plain_columnar(&self) -> bool {
        matches!(self, Self::Columnar(ColumnarProfile::Plain))
    }

    pub fn is_timeseries(&self) -> bool {
        matches!(self, Self::Columnar(ColumnarProfile::Timeseries { .. }))
    }

    pub fn is_spatial(&self) -> bool {
        matches!(self, Self::Columnar(ColumnarProfile::Spatial { .. }))
    }

    pub fn is_strict(&self) -> bool {
        matches!(self, Self::Document(DocumentMode::Strict(_)))
    }

    pub fn is_schemaless(&self) -> bool {
        matches!(self, Self::Document(DocumentMode::Schemaless))
    }

    pub fn is_kv(&self) -> bool {
        matches!(self, Self::KeyValue(_))
    }

    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Document(DocumentMode::Schemaless) => "document",
            Self::Document(DocumentMode::Strict(_)) => "strict",
            Self::Columnar(ColumnarProfile::Plain) => "columnar",
            Self::Columnar(ColumnarProfile::Timeseries { .. }) => "timeseries",
            Self::Columnar(ColumnarProfile::Spatial { .. }) => "columnar:spatial",
            Self::KeyValue(_) => "kv",
        }
    }

    /// Get the document mode, if this is a document collection.
    pub fn document_mode(&self) -> Option<&DocumentMode> {
        match self {
            Self::Document(mode) => Some(mode),
            _ => None,
        }
    }

    /// Get the columnar profile, if this is a columnar collection.
    pub fn columnar_profile(&self) -> Option<&ColumnarProfile> {
        match self {
            Self::Columnar(profile) => Some(profile),
            _ => None,
        }
    }

    /// Get the KV config, if this is a key-value collection.
    pub fn kv_config(&self) -> Option<&KvConfig> {
        match self {
            Self::KeyValue(config) => Some(config),
            _ => None,
        }
    }
}

impl std::fmt::Display for CollectionType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

impl std::str::FromStr for CollectionType {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_lowercase().as_str() {
            "document" | "doc" => Ok(Self::document()),
            "strict" => Ok(Self::Document(DocumentMode::Strict(
                // Placeholder — real schema comes from DDL parsing, not FromStr.
                // FromStr only resolves the storage mode; schema is attached separately.
                StrictSchema {
                    columns: vec![],
                    version: 1,
                    dropped_columns: Vec::new(),
                },
            ))),
            "columnar" => Ok(Self::columnar()),
            "timeseries" | "ts" => Ok(Self::timeseries("time", "1h")),
            "kv" | "key_value" | "keyvalue" => Ok(Self::KeyValue(KvConfig {
                // Placeholder — real schema comes from DDL parsing, not FromStr.
                schema: StrictSchema {
                    columns: vec![],
                    version: 1,
                    dropped_columns: Vec::new(),
                },
                ttl: None,
                capacity_hint: 0,
                inline_threshold: KV_DEFAULT_INLINE_THRESHOLD,
            })),
            other => Err(format!("unknown collection type: '{other}'")),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::columnar::{ColumnDef, ColumnType};

    #[test]
    fn default_is_schemaless_document() {
        let ct = CollectionType::default();
        assert!(ct.is_document());
        assert!(ct.is_schemaless());
        assert!(!ct.is_columnar_family());
        assert!(!ct.is_timeseries());
        assert!(!ct.is_kv());
    }

    #[test]
    fn factory_methods() {
        assert!(CollectionType::document().is_schemaless());
        assert!(CollectionType::columnar().is_columnar_family());
        assert!(CollectionType::timeseries("time", "1h").is_timeseries());
        assert!(CollectionType::spatial("geom").is_columnar_family());
        assert!(CollectionType::spatial("geom").is_spatial());

        let schema = StrictSchema::new(vec![
            ColumnDef::required("key", ColumnType::String).with_primary_key(),
            ColumnDef::nullable("value", ColumnType::Bytes),
        ])
        .unwrap();
        let kv = CollectionType::kv(schema);
        assert!(kv.is_kv());
        assert!(!kv.is_document());
        assert!(!kv.is_columnar_family());
    }

    #[test]
    fn kv_with_ttl_factory() {
        let schema = StrictSchema::new(vec![
            ColumnDef::required("ip", ColumnType::String).with_primary_key(),
            ColumnDef::required("hits", ColumnType::Int64),
        ])
        .unwrap();
        let ttl = KvTtlPolicy::FixedDuration {
            duration_ms: 60_000,
        };
        let ct = CollectionType::kv_with_ttl(schema, ttl);
        assert!(ct.is_kv());
        let config = ct.kv_config().unwrap();
        assert!(config.has_ttl());
        match config.ttl.as_ref().unwrap() {
            KvTtlPolicy::FixedDuration { duration_ms } => assert_eq!(*duration_ms, 60_000),
            _ => panic!("expected FixedDuration"),
        }
    }

    #[test]
    fn serde_roundtrip_document() {
        let ct = CollectionType::document();
        let json = sonic_rs::to_string(&ct).unwrap();
        let back: CollectionType = sonic_rs::from_str(&json).unwrap();
        assert_eq!(back, ct);
    }

    #[test]
    fn serde_roundtrip_columnar() {
        let ct = CollectionType::columnar();
        let json = sonic_rs::to_string(&ct).unwrap();
        let back: CollectionType = sonic_rs::from_str(&json).unwrap();
        assert_eq!(back, ct);
    }

    #[test]
    fn serde_roundtrip_timeseries() {
        let ct = CollectionType::timeseries("ts", "1h");
        let json = sonic_rs::to_string(&ct).unwrap();
        let back: CollectionType = sonic_rs::from_str(&json).unwrap();
        assert_eq!(back, ct);
    }

    #[test]
    fn serde_roundtrip_kv_no_ttl() {
        let schema = StrictSchema::new(vec![
            ColumnDef::required("k", ColumnType::String).with_primary_key(),
            ColumnDef::nullable("v", ColumnType::Bytes),
        ])
        .unwrap();
        let ct = CollectionType::kv(schema);
        let json = sonic_rs::to_string(&ct).unwrap();
        let back: CollectionType = sonic_rs::from_str(&json).unwrap();
        assert_eq!(back, ct);
    }

    #[test]
    fn serde_roundtrip_kv_fixed_ttl() {
        let schema = StrictSchema::new(vec![
            ColumnDef::required("k", ColumnType::String).with_primary_key(),
            ColumnDef::required("v", ColumnType::Bytes),
        ])
        .unwrap();
        let ttl = KvTtlPolicy::FixedDuration {
            duration_ms: 900_000,
        };
        let ct = CollectionType::kv_with_ttl(schema, ttl);
        let json = sonic_rs::to_string(&ct).unwrap();
        let back: CollectionType = sonic_rs::from_str(&json).unwrap();
        assert_eq!(back, ct);
    }

    #[test]
    fn serde_roundtrip_kv_field_ttl() {
        let schema = StrictSchema::new(vec![
            ColumnDef::required("k", ColumnType::String).with_primary_key(),
            ColumnDef::required("last_active", ColumnType::Timestamp),
        ])
        .unwrap();
        let ttl = KvTtlPolicy::FieldBased {
            field: "last_active".into(),
            offset_ms: 3_600_000,
        };
        let ct = CollectionType::kv_with_ttl(schema, ttl);
        let json = sonic_rs::to_string(&ct).unwrap();
        let back: CollectionType = sonic_rs::from_str(&json).unwrap();
        assert_eq!(back, ct);
    }

    #[test]
    fn display() {
        assert_eq!(CollectionType::document().to_string(), "document");
        assert_eq!(CollectionType::columnar().to_string(), "columnar");
        assert_eq!(
            CollectionType::timeseries("time", "1h").to_string(),
            "timeseries"
        );

        let schema = StrictSchema::new(vec![
            ColumnDef::required("k", ColumnType::String).with_primary_key(),
        ])
        .unwrap();
        assert_eq!(CollectionType::kv(schema).to_string(), "kv");
    }

    #[test]
    fn from_str() {
        assert!("document".parse::<CollectionType>().unwrap().is_document());
        assert!(
            "columnar"
                .parse::<CollectionType>()
                .unwrap()
                .is_columnar_family()
        );
        assert!(
            "timeseries"
                .parse::<CollectionType>()
                .unwrap()
                .is_timeseries()
        );
        assert!("ts".parse::<CollectionType>().unwrap().is_timeseries());
        assert!("kv".parse::<CollectionType>().unwrap().is_kv());
        assert!("key_value".parse::<CollectionType>().unwrap().is_kv());
        assert!("keyvalue".parse::<CollectionType>().unwrap().is_kv());
        assert!("unknown".parse::<CollectionType>().is_err());
    }

    #[test]
    fn accessors() {
        let ct = CollectionType::timeseries("time", "1h");
        assert!(ct.columnar_profile().is_some());
        assert!(ct.document_mode().is_none());
        assert!(ct.kv_config().is_none());

        let doc = CollectionType::document();
        assert!(doc.document_mode().is_some());
        assert!(doc.columnar_profile().is_none());
        assert!(doc.kv_config().is_none());

        let schema = StrictSchema::new(vec![
            ColumnDef::required("k", ColumnType::String).with_primary_key(),
        ])
        .unwrap();
        let kv = CollectionType::kv(schema);
        assert!(kv.kv_config().is_some());
        assert!(kv.document_mode().is_none());
        assert!(kv.columnar_profile().is_none());
    }
}
