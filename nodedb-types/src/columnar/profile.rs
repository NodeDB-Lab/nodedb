//! ColumnarProfile and DocumentMode — collection storage specializations.

use std::fmt;

use serde::{Deserialize, Serialize};

use super::schema::StrictSchema;

/// Specialization profile for columnar collections.
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
#[serde(tag = "profile")]
pub enum ColumnarProfile {
    /// General analytics. No special constraints.
    Plain,
    /// Time-partitioned append-only storage for metrics and logs.
    Timeseries { time_key: String, interval: String },
    /// Geometry-optimized storage with automatic spatial indexing.
    Spatial {
        geometry_column: String,
        auto_rtree: bool,
        auto_geohash: bool,
    },
}

impl ColumnarProfile {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Plain => "plain",
            Self::Timeseries { .. } => "timeseries",
            Self::Spatial { .. } => "spatial",
        }
    }

    pub fn is_timeseries(&self) -> bool {
        matches!(self, Self::Timeseries { .. })
    }

    pub fn is_spatial(&self) -> bool {
        matches!(self, Self::Spatial { .. })
    }
}

impl fmt::Display for ColumnarProfile {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

/// Storage mode for document collections.
#[derive(
    Debug,
    Clone,
    PartialEq,
    Eq,
    Serialize,
    Deserialize,
    Default,
    zerompk::ToMessagePack,
    zerompk::FromMessagePack,
)]
#[serde(tag = "mode")]
pub enum DocumentMode {
    /// Schemaless MessagePack documents. Default. CRDT-friendly.
    #[default]
    Schemaless,
    /// Schema-enforced Binary Tuple documents.
    Strict(StrictSchema),
}

impl DocumentMode {
    pub fn is_schemaless(&self) -> bool {
        matches!(self, Self::Schemaless)
    }

    pub fn is_strict(&self) -> bool {
        matches!(self, Self::Strict(_))
    }

    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Schemaless => "schemaless",
            Self::Strict(_) => "strict",
        }
    }

    pub fn schema(&self) -> Option<&StrictSchema> {
        match self {
            Self::Strict(s) => Some(s),
            Self::Schemaless => None,
        }
    }
}

impl fmt::Display for DocumentMode {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::columnar::{ColumnDef, ColumnType};

    #[test]
    fn columnar_profile_serde_roundtrip() {
        let profiles = vec![
            ColumnarProfile::Plain,
            ColumnarProfile::Timeseries {
                time_key: "time".into(),
                interval: "1h".into(),
            },
            ColumnarProfile::Spatial {
                geometry_column: "geom".into(),
                auto_rtree: true,
                auto_geohash: true,
            },
        ];
        for p in profiles {
            let json = sonic_rs::to_string(&p).unwrap();
            let back: ColumnarProfile = sonic_rs::from_str(&json).unwrap();
            assert_eq!(back, p);
        }
    }

    #[test]
    fn document_mode_default_is_schemaless() {
        assert!(DocumentMode::default().is_schemaless());
    }

    #[test]
    fn document_mode_strict_has_schema() {
        let schema = StrictSchema::new(vec![ColumnDef::required("id", ColumnType::Int64)]).unwrap();
        let mode = DocumentMode::Strict(schema.clone());
        assert!(mode.is_strict());
        assert_eq!(mode.schema().unwrap(), &schema);
    }

    #[test]
    fn document_mode_serde_roundtrip() {
        let modes = vec![
            DocumentMode::Schemaless,
            DocumentMode::Strict(
                StrictSchema::new(vec![
                    ColumnDef::required("id", ColumnType::Int64).with_primary_key(),
                    ColumnDef::nullable("name", ColumnType::String),
                ])
                .unwrap(),
            ),
        ];
        for m in modes {
            let json = sonic_rs::to_string(&m).unwrap();
            let back: DocumentMode = sonic_rs::from_str(&json).unwrap();
            assert_eq!(back, m);
        }
    }
}
