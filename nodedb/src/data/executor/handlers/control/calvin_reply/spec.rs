// SPDX-License-Identifier: BUSL-1.1

//! A staged reply as the redo entry carries it, and back.
//!
//! The leader's resolve turns the reply its stage decided into a
//! [`CalvinReplySpec`]. Every replica's install turns that spec back into a
//! [`CalvinReply`] and renders it.

use nodedb_physical::physical_plan::{
    CalvinInstalledTimeseriesSpec, CalvinPostImagesSpec, CalvinReplyRow, CalvinReplySpec,
    CalvinRowEngine,
};
use nodedb_types::RowIdentity;

use super::reply::{CalvinReply, InstalledTimeseries, PostImages};
use super::target::RowEngine;

impl From<RowEngine> for CalvinRowEngine {
    fn from(engine: RowEngine) -> Self {
        match engine {
            RowEngine::Document => Self::Document,
            RowEngine::Crdt => Self::Crdt,
            RowEngine::Kv => Self::Kv,
            RowEngine::Vector => Self::Vector,
            RowEngine::Columnar => Self::Columnar,
            RowEngine::Timeseries => Self::Timeseries,
        }
    }
}

impl From<CalvinRowEngine> for RowEngine {
    fn from(engine: CalvinRowEngine) -> Self {
        match engine {
            CalvinRowEngine::Document => Self::Document,
            CalvinRowEngine::Crdt => Self::Crdt,
            CalvinRowEngine::Kv => Self::Kv,
            CalvinRowEngine::Vector => Self::Vector,
            CalvinRowEngine::Columnar => Self::Columnar,
            CalvinRowEngine::Timeseries => Self::Timeseries,
        }
    }
}

impl From<&CalvinReply> for CalvinReplySpec {
    fn from(reply: &CalvinReply) -> Self {
        match reply {
            CalvinReply::Count(payload) => Self::Count(payload.clone()),
            CalvinReply::Rows(payload) => Self::Rows(payload.clone()),
            CalvinReply::PostImages(images) => Self::PostImages(CalvinPostImagesSpec {
                spec: images.spec.clone(),
                rls_filters: images.rls_filters.clone(),
                collection: images.collection.clone(),
                engine: images.engine.into(),
                rows: images
                    .rows
                    .iter()
                    .map(|(identity, surrogate)| CalvinReplyRow {
                        identity: identity.as_str().to_string(),
                        surrogate: *surrogate,
                    })
                    .collect(),
            }),
            CalvinReply::InstalledTimeseries(installed) => {
                Self::InstalledTimeseries(CalvinInstalledTimeseriesSpec {
                    spec: installed.spec.clone(),
                    rls_filters: installed.rls_filters.clone(),
                    collection: installed.collection.clone(),
                    ordinal: installed.ordinal as u64,
                })
            }
        }
    }
}

impl From<CalvinReplySpec> for CalvinReply {
    fn from(spec: CalvinReplySpec) -> Self {
        match spec {
            CalvinReplySpec::Count(payload) => Self::Count(payload),
            CalvinReplySpec::Rows(payload) => Self::Rows(payload),
            CalvinReplySpec::PostImages(images) => Self::PostImages(PostImages {
                spec: images.spec,
                rls_filters: images.rls_filters,
                collection: images.collection,
                engine: images.engine.into(),
                rows: images
                    .rows
                    .into_iter()
                    .map(|row| (RowIdentity::from_user_key(row.identity), row.surrogate))
                    .collect(),
            }),
            // An ordinal past the address space names no install, so the
            // render reports the install absent.
            CalvinReplySpec::InstalledTimeseries(installed) => {
                Self::InstalledTimeseries(InstalledTimeseries {
                    spec: installed.spec,
                    rls_filters: installed.rls_filters,
                    collection: installed.collection,
                    ordinal: usize::try_from(installed.ordinal).unwrap_or(usize::MAX),
                })
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use nodedb_physical::physical_plan::{ReturningColumns, ReturningSpec};
    use nodedb_types::Surrogate;

    fn returning() -> ReturningSpec {
        ReturningSpec {
            columns: ReturningColumns::Star,
        }
    }

    fn round_trip(spec: CalvinReplySpec) {
        let reply = CalvinReply::from(spec.clone());
        assert_eq!(CalvinReplySpec::from(&reply), spec);
    }

    #[test]
    fn every_reply_shape_round_trips_through_its_spec() {
        round_trip(CalvinReplySpec::Count(vec![1, 2]));
        round_trip(CalvinReplySpec::Rows(vec![3]));
        round_trip(CalvinReplySpec::PostImages(CalvinPostImagesSpec {
            spec: returning(),
            rls_filters: vec![4],
            collection: "orders".into(),
            engine: CalvinRowEngine::Vector,
            rows: vec![
                CalvinReplyRow {
                    identity: "o1".into(),
                    surrogate: Surrogate::new(1),
                },
                CalvinReplyRow {
                    identity: "o2".into(),
                    surrogate: Surrogate::new(2),
                },
            ],
        }));
        round_trip(CalvinReplySpec::InstalledTimeseries(
            CalvinInstalledTimeseriesSpec {
                spec: returning(),
                rls_filters: Vec::new(),
                collection: "metrics".into(),
                ordinal: 3,
            },
        ));
    }
}
