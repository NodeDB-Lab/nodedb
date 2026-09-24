// SPDX-License-Identifier: BUSL-1.1

//! Forensic payload for a write window that leaked from the outcome floor.

use faultbox::DomainContext;
use faultbox::serde_json::{Value, json};

/// A write window was dropped before its write's outcome was final.
pub(in crate::diag) struct WriteWindowLeaked {
    /// Ticket of the leaked window.
    pub ticket: u64,
    /// The window's horizon: the outcome floor stays below it.
    pub horizon: u64,
    /// How long the window had been open when it leaked.
    pub open_for_ms: u64,
}

impl DomainContext for WriteWindowLeaked {
    fn domain_kind(&self) -> &'static str {
        "nodedb.write_window_leaked"
    }

    fn grouping_key(&self) -> String {
        // One bug class: a mint site dropped its window on a path with no
        // settle. Ticket and horizon are the occurrence.
        "write_window_leaked".to_string()
    }

    fn to_json(&self) -> Value {
        json!({
            "ticket": self.ticket,
            "horizon": self.horizon,
            "open_for_ms": self.open_for_ms,
            "why_fatal": "the outcome floor bounds every engine watermark and every WAL \
                          truncation. A leaked window holds the floor below its horizon \
                          until the process restarts, so no checkpoint on this node \
                          advances past it and the WAL grows without bound",
            "operator_action": "restart the node to release the floor: restart replay \
                                 reaches the record the window held. Then find the mint \
                                 site whose early-return path drops its window without \
                                 settling or holding it; the backtrace names it",
        })
    }
}

/// A write window was held: its record has no final outcome in this process.
pub(in crate::diag) struct WriteWindowHeld {
    /// `file:line` of the hold call.
    pub site: String,
    /// Ticket of the held window.
    pub ticket: u64,
    /// The window's horizon: the outcome floor stays below it.
    pub horizon: u64,
    /// How long the window had been open when it was held.
    pub open_for_ms: u64,
}

impl DomainContext for WriteWindowHeld {
    fn domain_kind(&self) -> &'static str {
        "nodedb.write_window_held"
    }

    fn grouping_key(&self) -> String {
        // One report per hold site. Ticket and horizon are the occurrence.
        format!("write_window_held:{}", self.site)
    }

    fn to_json(&self) -> Value {
        json!({
            "site": self.site,
            "ticket": self.ticket,
            "horizon": self.horizon,
            "open_for_ms": self.open_for_ms,
            "impact": "the outcome floor bounds every engine watermark and every WAL \
                       truncation. A held window keeps the floor below its horizon \
                       until the process restarts, so no checkpoint on this node \
                       advances past it and the WAL grows until then",
            "operator_action": "restart the node once convenient: restart replay reaches \
                                 the held record and gives it a final outcome. The site \
                                 names the path that could not decide the outcome",
        })
    }
}
