// SPDX-License-Identifier: BUSL-1.1

//! The change metadata a `ClusterArrayOp` names.

use nodedb_physical::physical_plan::ClusterArrayOp;

use crate::control::change_stream::ChangeOperation;

use super::extract::{WriteChangeMeta, array_change_name, every_row};

/// Map a `ClusterArrayOp` to its CDC change metadata.
pub(super) fn cluster_array_change_meta(op: &ClusterArrayOp) -> Vec<WriteChangeMeta> {
    match op {
        ClusterArrayOp::Put { array_id, .. } => {
            vec![(
                array_change_name(array_id),
                every_row(),
                ChangeOperation::Insert,
            )]
        }
        ClusterArrayOp::Delete { array_id, .. } => {
            vec![(
                array_change_name(array_id),
                every_row(),
                ChangeOperation::Delete,
            )]
        }
        // Reads and maintenance change no row.
        ClusterArrayOp::Slice { .. }
        | ClusterArrayOp::Agg { .. }
        | ClusterArrayOp::Elementwise { .. }
        | ClusterArrayOp::Flush { .. }
        | ClusterArrayOp::Compact { .. } => Vec::new(),
    }
}
