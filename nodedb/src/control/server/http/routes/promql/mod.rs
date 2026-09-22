// SPDX-License-Identifier: BUSL-1.1

//! Prometheus-compatible PromQL HTTP API at `/v1/obsv/api/v1/*`.
//!
//! Grafana data source URL: `http://nodedb:6480/v1/obsv/api`

pub mod buildinfo;
pub mod handlers;
pub(crate) mod helpers;
mod remote;
pub mod types;

pub use buildinfo::buildinfo;
pub use handlers::{
    annotations, instant_query, label_names, label_values, metadata, range_query, series_query,
};
pub use remote::{remote_read, remote_write};
pub use types::{InstantQueryParams, LabelsParams, RangeQueryParams, SeriesParams};
