// SPDX-License-Identifier: BUSL-1.1

//! The retention decision one log entry carries.
//!
//! The leader reads its clock once. Every field here is that reading's result, so
//! a replica or a mirror that applies the entry reads no clock.

use nodedb_physical::physical_plan::{AggregateIncarnation, MetaOp, PhysicalPlan};
use nodedb_types::{DatabaseId, QualifiedCollection, TenantId};

/// One retention decision.
#[derive(
    Debug,
    Clone,
    PartialEq,
    Eq,
    serde::Serialize,
    serde::Deserialize,
    zerompk::ToMessagePack,
    zerompk::FromMessagePack,
)]
pub enum RetentionOp {
    /// Drop a timeseries collection's partitions past `decided_ms - max_age_ms`.
    /// Each partition folds into its aggregates first.
    TimeseriesPartitions {
        collection: String,
        max_age_ms: i64,
        decided_ms: i64,
    },
    /// Drop one tenant's continuous-aggregate buckets past each one's retention.
    ContinuousAggregates { decided_ms: i64 },
    /// Purge superseded edge versions below the cutoff.
    TemporalEdges {
        collection: String,
        cutoff_system_ms: i64,
    },
    /// Purge superseded strict-document versions below the cutoff.
    TemporalDocumentStrict {
        collection: String,
        cutoff_system_ms: i64,
    },
    /// Purge a columnar collection's history below the cutoff.
    TemporalColumnar {
        collection: String,
        cutoff_system_ms: i64,
        decided_ms: i64,
    },
    /// Purge a CRDT collection's archived versions below the cutoff.
    TemporalCrdt {
        collection: String,
        cutoff_system_ms: i64,
    },
    /// Purge an array's tile versions below the cutoff. A later compaction's
    /// window ends here too.
    TemporalArray {
        array: String,
        cutoff_system_ms: i64,
    },
}

impl RetentionOp {
    /// The collection or array the decision names. `None` for a tenant's whole
    /// aggregate set.
    pub fn object(&self) -> Option<&str> {
        match self {
            Self::TimeseriesPartitions { collection, .. }
            | Self::TemporalEdges { collection, .. }
            | Self::TemporalDocumentStrict { collection, .. }
            | Self::TemporalColumnar { collection, .. }
            | Self::TemporalCrdt { collection, .. } => Some(collection),
            Self::TemporalArray { array, .. } => Some(array),
            Self::ContinuousAggregates { .. } => None,
        }
    }

    /// Keep an audit purge's cutoff at or below `floor_ms`. An age or bucket
    /// decision reaches no version a snapshot read needs, so it stays.
    pub fn lower_cutoff(&mut self, floor_ms: i64) {
        match self {
            Self::TemporalEdges {
                cutoff_system_ms, ..
            }
            | Self::TemporalDocumentStrict {
                cutoff_system_ms, ..
            }
            | Self::TemporalColumnar {
                cutoff_system_ms, ..
            }
            | Self::TemporalCrdt {
                cutoff_system_ms, ..
            }
            | Self::TemporalArray {
                cutoff_system_ms, ..
            } => *cutoff_system_ms = (*cutoff_system_ms).min(floor_ms),
            Self::TimeseriesPartitions { .. } | Self::ContinuousAggregates { .. } => {}
        }
    }

    /// The plan every core runs. `aggregates` are those the catalog roots at the
    /// collection, as this node's catalog holds them.
    pub fn plan(
        &self,
        tenant_id: TenantId,
        database_id: DatabaseId,
        aggregates: Vec<AggregateIncarnation>,
    ) -> PhysicalPlan {
        let tenant_id = tenant_id.as_u64();
        let qualified = |collection: &str| QualifiedCollection::new(database_id, collection);
        PhysicalPlan::Meta(match self {
            Self::TimeseriesPartitions {
                collection,
                max_age_ms,
                decided_ms,
            } => MetaOp::EnforceTimeseriesRetention {
                collection: qualified(collection),
                max_age_ms: *max_age_ms,
                decided_ms: *decided_ms,
                aggregates,
            },
            Self::ContinuousAggregates { decided_ms } => MetaOp::ApplyContinuousAggRetention {
                decided_ms: *decided_ms,
            },
            Self::TemporalEdges {
                collection,
                cutoff_system_ms,
            } => MetaOp::TemporalPurgeEdgeStore {
                tenant_id,
                collection: qualified(collection),
                cutoff_system_ms: *cutoff_system_ms,
            },
            Self::TemporalDocumentStrict {
                collection,
                cutoff_system_ms,
            } => MetaOp::TemporalPurgeDocumentStrict {
                tenant_id,
                collection: qualified(collection),
                cutoff_system_ms: *cutoff_system_ms,
            },
            Self::TemporalColumnar {
                collection,
                cutoff_system_ms,
                decided_ms,
            } => MetaOp::TemporalPurgeColumnar {
                tenant_id,
                collection: qualified(collection),
                cutoff_system_ms: *cutoff_system_ms,
                decided_ms: *decided_ms,
                aggregates,
            },
            Self::TemporalCrdt {
                collection,
                cutoff_system_ms,
            } => MetaOp::TemporalPurgeCrdt {
                tenant_id,
                collection: qualified(collection),
                cutoff_system_ms: *cutoff_system_ms,
            },
            Self::TemporalArray {
                array,
                cutoff_system_ms,
            } => MetaOp::TemporalPurgeArray {
                tenant_id,
                array_id: array.clone(),
                cutoff_system_ms: *cutoff_system_ms,
            },
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_plan_carries_the_decision_as_read() {
        let op = RetentionOp::TimeseriesPartitions {
            collection: "metrics".to_string(),
            max_age_ms: 10,
            decided_ms: 1_000,
        };
        let PhysicalPlan::Meta(MetaOp::EnforceTimeseriesRetention {
            max_age_ms,
            decided_ms,
            ..
        }) = op.plan(TenantId::new(1), DatabaseId::new(2), Vec::new())
        else {
            panic!("a timeseries drop plans its enforcement");
        };
        assert_eq!((max_age_ms, decided_ms), (10, 1_000));
    }

    #[test]
    fn only_a_tenants_aggregate_pass_names_no_object() {
        assert_eq!(
            RetentionOp::ContinuousAggregates { decided_ms: 1 }.object(),
            None
        );
        let array = RetentionOp::TemporalArray {
            array: "cells".to_string(),
            cutoff_system_ms: 1,
        };
        assert_eq!(array.object(), Some("cells"));
    }
}
