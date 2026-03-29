//! Prometheus-compatible PromQL HTTP API at `/obsv/api/v1/*`.
//!
//! Grafana data source URL: `http://nodedb:6480/obsv/api`

mod handlers;
mod helpers;

pub use handlers::{
    buildinfo, instant_query, label_names, label_values, metadata, range_query, series_query,
};

/// Query parameters for `/query`.
#[derive(Debug, serde::Deserialize)]
pub struct InstantQueryParams {
    pub query: String,
    pub time: Option<f64>,
}

/// Query parameters for `/query_range`.
#[derive(Debug, serde::Deserialize)]
pub struct RangeQueryParams {
    pub query: String,
    pub start: f64,
    pub end: f64,
    pub step: String,
}

/// Query parameters for `/series`.
#[derive(Debug, serde::Deserialize)]
pub struct SeriesParams {
    #[serde(rename = "match[]", default)]
    pub matchers: Vec<String>,
    pub start: Option<f64>,
    pub end: Option<f64>,
}

/// Query parameters for `/labels`.
#[derive(Debug, serde::Deserialize)]
pub struct LabelsParams {
    pub start: Option<f64>,
    pub end: Option<f64>,
}
