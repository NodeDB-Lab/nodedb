// SPDX-License-Identifier: BUSL-1.1

//! Forensic payloads for a backup cut's data-group barriers.

use faultbox::DomainContext;
use faultbox::serde_json::{Value, json};

/// A cut's barrier driver stopped with a data group of this node that never
/// applied the cut's barrier.
pub(in crate::diag) struct CutBarrierNotPlaced {
    pub group_id: u64,
    /// The cut's watermark.
    pub hlc: u64,
    pub restore_point: u64,
    /// Whether this node led the group when the driver stopped.
    pub led_here: bool,
    /// The vShards of the group whose scheduler here had not passed the
    /// cut's marker.
    pub lagging: Vec<u32>,
}

impl DomainContext for CutBarrierNotPlaced {
    fn domain_kind(&self) -> &'static str {
        "nodedb.cut_barrier_not_placed"
    }

    fn grouping_key(&self) -> String {
        // One report per group: the watermark and the lagging vShards
        // change with every cut.
        format!("cut_barrier_not_placed:{}", self.group_id)
    }

    fn to_json(&self) -> Value {
        json!({
            "group_id": self.group_id,
            "hlc": self.hlc,
            "restore_point": self.restore_point,
            "led_here": self.led_here,
            "lagging_vshards": self.lagging,
            "effect": "the cut has no barrier in this group. The backup or restore point \
                       that took it failed. Calvin redo the leader held for the cut was \
                       released without a barrier",
            "operator_action": "check the group's leadership and the lagging vShards' \
                                Calvin schedulers: a halted scheduler never passes a cut \
                                marker. Retry the backup or restore point once they run",
        })
    }
}
