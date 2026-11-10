// SPDX-License-Identifier: BUSL-1.1

//! Request, row, limit, and reply types of the precommit response.

use nodedb_cluster::calvin::types::ReturningContractLimits;
use nodedb_types::config::tuning::NetworkTuning;

use crate::control::security::redaction::snapshot_rules::RedactionSnapshot;
use crate::control::sequence::SequenceAccess;
use crate::control::server::response_shape::schema::OutputSchema;
use crate::control::server::response_shape::types::{ShapedRow, ShapedRows};
use crate::control::server::shared::returning::ReturningSpec;
use crate::control::server::shared::returning::transcript::SequenceEffect;

/// Source identity supplied by the checked preparation coordinator.
/// This identity never comes from user-visible source fields.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub(crate) struct PreparedRowIdentity {
    pub collection: String,
    pub stable_row_key: Vec<u8>,
    pub operation_index: u64,
}

/// One actual prepared source image and its independent stable identity.
#[derive(Debug, Clone, PartialEq)]
pub(crate) struct PreparedReturningRow {
    pub identity: PreparedRowIdentity,
    pub fields: ShapedRow,
}

/// Hard response limits checked before source commit.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct ReturningLimits {
    pub rows: usize,
    pub columns: usize,
    pub cells: usize,
    pub payload_bytes: usize,
}

impl ReturningLimits {
    /// The limits a node's tuning sets. The payload bytes follow
    /// `network.max_query_result_bytes`, the cap on every other result.
    pub(crate) fn from_tuning(network: &NetworkTuning) -> Self {
        Self {
            rows: to_usize(network.max_returning_rows),
            columns: to_usize(network.max_returning_columns),
            cells: to_usize(network.max_returning_cells),
            payload_bytes: to_usize(network.max_query_result_bytes),
        }
    }

    /// The limits a contract carries.
    pub(crate) fn from_contract(limits: &ReturningContractLimits) -> Self {
        Self {
            rows: to_usize(limits.rows),
            columns: to_usize(limits.columns),
            cells: to_usize(limits.cells),
            payload_bytes: to_usize(limits.payload_bytes),
        }
    }

    /// The contract form of these limits.
    pub(crate) fn to_contract(self) -> ReturningContractLimits {
        ReturningContractLimits {
            rows: to_u64(self.rows),
            columns: to_u64(self.columns),
            cells: to_u64(self.cells),
            payload_bytes: to_u64(self.payload_bytes),
        }
    }
}

fn to_usize(value: u64) -> usize {
    usize::try_from(value).unwrap_or(usize::MAX)
}

fn to_u64(value: usize) -> u64 {
    u64::try_from(value).unwrap_or(u64::MAX)
}

/// Immutable request scope and response contract for one prepared source.
pub(crate) struct PrecommitReturning<'a> {
    pub spec: &'a ReturningSpec,
    /// Source field order, including declared fields when no row matches.
    pub source_columns: &'a [String],
    pub redaction: &'a RedactionSnapshot,
    pub sequences: &'a dyn SequenceAccess,
    pub projection: Option<&'a OutputSchema>,
    pub crs_set: &'a nodedb_crs::CustomCrsSet,
    pub limits: ReturningLimits,
}

/// Frozen response bytes and their exact protocol-neutral shape.
/// Transport retries reuse this value without evaluating expressions again.
#[derive(Debug)]
pub(crate) struct FrozenReturningReply {
    pub payload: Vec<u8>,
    pub shaped: ShapedRows,
    /// The `nextval` and `setval` results the evaluation recorded, in
    /// invocation order.
    pub sequence_effects: Vec<SequenceEffect>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn limits_follow_the_tuning_and_the_result_byte_cap() {
        let network = NetworkTuning {
            max_returning_rows: 7,
            max_returning_columns: 5,
            max_returning_cells: 30,
            max_query_result_bytes: 4096,
            ..NetworkTuning::default()
        };
        let limits = ReturningLimits::from_tuning(&network);
        assert_eq!(
            limits,
            ReturningLimits {
                rows: 7,
                columns: 5,
                cells: 30,
                payload_bytes: 4096,
            }
        );
        assert_eq!(
            ReturningLimits::from_contract(&limits.to_contract()),
            limits
        );
    }

    #[test]
    fn the_default_tuning_sets_limits_below_the_result_byte_cap() {
        let network = NetworkTuning::default();
        let limits = ReturningLimits::from_tuning(&network);
        assert_eq!(limits.payload_bytes as u64, network.max_query_result_bytes);
        assert!(limits.rows > 0 && limits.columns > 0 && limits.cells > 0);
        assert!(limits.cells >= limits.rows);
    }
}
