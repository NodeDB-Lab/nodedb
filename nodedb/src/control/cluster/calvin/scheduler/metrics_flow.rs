// SPDX-License-Identifier: BUSL-1.1

//! Prometheus rendering of the scheduler's flow metrics: deferred dispatch,
//! the intake gate, the apply halt, and sequencer propose retries.

use std::fmt::Write as _;
use std::sync::atomic::Ordering;

use super::metrics::{
    SchedulerMetrics, apply_halt_reason, intake_closure_reason, sequencer_propose_kind,
};

impl SchedulerMetrics {
    /// Append the flow metrics for the vShard in `label` to `out`.
    pub(super) fn render_flow_prometheus(&self, out: &mut String, label: &str) {
        let _ = writeln!(
            out,
            "# HELP nodedb_calvin_dispatch_deferred_total \
             Scheduler dispatches refused at dispatcher capacity and parked for re-send."
        );
        let _ = writeln!(out, "# TYPE nodedb_calvin_dispatch_deferred_total counter");
        let _ = writeln!(
            out,
            "nodedb_calvin_dispatch_deferred_total{{{label}}} {}",
            self.dispatch_deferred_count.load(Ordering::Relaxed)
        );

        let _ = writeln!(
            out,
            "# HELP nodedb_calvin_dispatch_deferred_depth \
             Scheduler requests parked for re-send, waiting for dispatcher capacity."
        );
        let _ = writeln!(out, "# TYPE nodedb_calvin_dispatch_deferred_depth gauge");
        let _ = writeln!(
            out,
            "nodedb_calvin_dispatch_deferred_depth{{{label}}} {}",
            self.dispatch_deferred_depth.load(Ordering::Relaxed)
        );

        let _ = writeln!(
            out,
            "# HELP nodedb_calvin_intake_gate_closed \
             1 while the scheduler takes no new sequenced input."
        );
        let _ = writeln!(out, "# TYPE nodedb_calvin_intake_gate_closed gauge");
        let _ = writeln!(
            out,
            "nodedb_calvin_intake_gate_closed{{{label}}} {}",
            self.intake_gate_closed.load(Ordering::Relaxed)
        );

        let _ = writeln!(
            out,
            "# HELP nodedb_calvin_intake_backlog \
             Pending, blocked, and dependent-barrier txns in the scheduler."
        );
        let _ = writeln!(out, "# TYPE nodedb_calvin_intake_backlog gauge");
        let _ = writeln!(
            out,
            "nodedb_calvin_intake_backlog{{{label}}} {}",
            self.intake_backlog.load(Ordering::Relaxed)
        );

        let _ = writeln!(
            out,
            "# HELP nodedb_calvin_intake_gate_closed_total \
             Times the scheduler stopped taking new sequenced input, by reason."
        );
        let _ = writeln!(out, "# TYPE nodedb_calvin_intake_gate_closed_total counter");
        for (i, &reason_label) in intake_closure_reason::LABELS.iter().enumerate() {
            let _ = writeln!(
                out,
                "nodedb_calvin_intake_gate_closed_total{{{label},reason=\"{reason_label}\"}} {}",
                self.intake_gate_closed_counts[i].load(Ordering::Relaxed)
            );
        }

        let _ = writeln!(
            out,
            "# HELP nodedb_calvin_apply_halted \
             1 once the scheduler halted on an apply error it cannot mark applied, by reason."
        );
        let _ = writeln!(out, "# TYPE nodedb_calvin_apply_halted gauge");
        let halted = self.apply_halted.load(Ordering::Relaxed) == 1;
        let halt_reason = self.apply_halt_reason.load(Ordering::Relaxed);
        for (i, &reason_label) in apply_halt_reason::LABELS.iter().enumerate() {
            let value = u64::from(halted && halt_reason == i as u64);
            let _ = writeln!(
                out,
                "nodedb_calvin_apply_halted{{{label},reason=\"{reason_label}\"}} {value}"
            );
        }
        let _ = writeln!(
            out,
            "# HELP nodedb_calvin_sequencer_propose_retry_total \
             Owed sequencer entries re-proposed because their effect was not yet applied, by kind."
        );
        let _ = writeln!(
            out,
            "# TYPE nodedb_calvin_sequencer_propose_retry_total counter"
        );
        for (i, &kind_label) in sequencer_propose_kind::LABELS.iter().enumerate() {
            let _ = writeln!(
                out,
                "nodedb_calvin_sequencer_propose_retry_total{{{label},kind=\"{kind_label}\"}} {}",
                self.sequencer_propose_retry_counts[i].load(Ordering::Relaxed)
            );
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn propose_retry_counter_renders_per_kind() {
        let m = SchedulerMetrics::new();
        m.record_sequencer_propose_retry(sequencer_propose_kind::VOTE);
        m.record_sequencer_propose_retry(sequencer_propose_kind::VOTE);
        m.record_sequencer_propose_retry(sequencer_propose_kind::COMPLETION_ACK);
        let out = m.render_prometheus(3);
        assert!(
            out.contains(
                "nodedb_calvin_sequencer_propose_retry_total{vshard=\"3\",kind=\"vote\"} 2"
            )
        );
        assert!(out.contains(
            "nodedb_calvin_sequencer_propose_retry_total{vshard=\"3\",kind=\"completion_ack\"} 1"
        ));
        assert!(out.contains(
            "nodedb_calvin_sequencer_propose_retry_total{vshard=\"3\",kind=\"routing_failed\"} 0"
        ));
    }
}
