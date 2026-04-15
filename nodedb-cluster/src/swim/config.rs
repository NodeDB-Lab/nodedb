//! SWIM protocol configuration.
//!
//! Tunable parameters that govern failure-detection latency, bandwidth, and
//! false-positive rate. Defaults follow the Lifeguard recommendations for
//! a ≤ 256-node cluster and are safe for production without tuning.

use std::time::Duration;

use super::error::SwimError;
use super::incarnation::Incarnation;

/// Configuration for the SWIM failure detector.
///
/// All fields are validated at construction time via [`SwimConfig::validate`];
/// an invalid config is a programmer error and returns a typed
/// [`SwimError::InvalidConfig`] rather than panicking.
#[derive(Debug, Clone)]
pub struct SwimConfig {
    /// Time between probe rounds (T' in the SWIM paper). One randomly-chosen
    /// alive peer is pinged per interval.
    pub probe_interval: Duration,

    /// Round-trip deadline for a direct ping before falling back to k
    /// indirect pings. Must be strictly less than `probe_interval`.
    pub probe_timeout: Duration,

    /// Number of indirect probe helpers (`k` in the paper).
    pub indirect_probes: u8,

    /// Multiplier on `probe_interval` used to compute the suspicion timeout
    /// before a `Suspect` member is declared `Dead`. Lifeguard §3.1.
    pub suspicion_mult: u8,

    /// Minimum value for the suspicion timeout; protects small clusters from
    /// sub-second suspicion windows. The effective timeout is
    /// `max(min_suspicion, suspicion_mult * log2(n) * probe_interval)`.
    pub min_suspicion: Duration,

    /// Seed incarnation for a freshly-booted local node. Always `0` in
    /// production; exposed for deterministic unit tests.
    pub initial_incarnation: Incarnation,

    /// Maximum number of membership deltas to piggyback on a single
    /// outgoing SWIM datagram. Caps per-message bandwidth and bounds
    /// the encoded payload size below a UDP MTU.
    pub max_piggyback: usize,

    /// Gossip fanout multiplier (`lambda` in Das §4.3). The
    /// dissemination queue drops a rumour after it has been carried
    /// on `ceil(fanout_lambda * log2(n+1))` outgoing messages, which
    /// with high probability reaches every member.
    pub fanout_lambda: u32,
}

impl SwimConfig {
    /// Production defaults from Lifeguard, tuned for a ≤ 256-node cluster.
    pub fn production() -> Self {
        Self {
            probe_interval: Duration::from_millis(1000),
            probe_timeout: Duration::from_millis(500),
            indirect_probes: 3,
            suspicion_mult: 4,
            min_suspicion: Duration::from_secs(2),
            initial_incarnation: Incarnation::ZERO,
            max_piggyback: 6,
            fanout_lambda: 3,
        }
    }

    /// Validate the configuration. Returns `InvalidConfig` if any invariant
    /// fails. Callers should treat validation failure as a fatal startup
    /// error — SWIM cannot run with incoherent timing parameters.
    pub fn validate(&self) -> Result<(), SwimError> {
        if self.probe_interval.is_zero() {
            return Err(SwimError::InvalidConfig {
                field: "probe_interval",
                reason: "must be non-zero",
            });
        }
        if self.probe_timeout >= self.probe_interval {
            return Err(SwimError::InvalidConfig {
                field: "probe_timeout",
                reason: "must be strictly less than probe_interval",
            });
        }
        if self.indirect_probes == 0 {
            return Err(SwimError::InvalidConfig {
                field: "indirect_probes",
                reason: "must be at least 1",
            });
        }
        if self.suspicion_mult == 0 {
            return Err(SwimError::InvalidConfig {
                field: "suspicion_mult",
                reason: "must be at least 1",
            });
        }
        if self.min_suspicion.is_zero() {
            return Err(SwimError::InvalidConfig {
                field: "min_suspicion",
                reason: "must be non-zero",
            });
        }
        if self.max_piggyback == 0 {
            return Err(SwimError::InvalidConfig {
                field: "max_piggyback",
                reason: "must be at least 1",
            });
        }
        if self.fanout_lambda == 0 {
            return Err(SwimError::InvalidConfig {
                field: "fanout_lambda",
                reason: "must be at least 1",
            });
        }
        Ok(())
    }
}

impl Default for SwimConfig {
    fn default() -> Self {
        Self::production()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn production_defaults_are_valid() {
        SwimConfig::production().validate().expect("valid");
    }

    #[test]
    fn zero_probe_interval_rejected() {
        let mut cfg = SwimConfig::production();
        cfg.probe_interval = Duration::ZERO;
        assert!(matches!(
            cfg.validate(),
            Err(SwimError::InvalidConfig {
                field: "probe_interval",
                ..
            })
        ));
    }

    #[test]
    fn probe_timeout_must_be_less_than_interval() {
        let mut cfg = SwimConfig::production();
        cfg.probe_timeout = cfg.probe_interval;
        assert!(matches!(
            cfg.validate(),
            Err(SwimError::InvalidConfig {
                field: "probe_timeout",
                ..
            })
        ));
    }

    #[test]
    fn zero_indirect_probes_rejected() {
        let mut cfg = SwimConfig::production();
        cfg.indirect_probes = 0;
        assert!(matches!(
            cfg.validate(),
            Err(SwimError::InvalidConfig {
                field: "indirect_probes",
                ..
            })
        ));
    }

    #[test]
    fn zero_suspicion_mult_rejected() {
        let mut cfg = SwimConfig::production();
        cfg.suspicion_mult = 0;
        assert!(matches!(
            cfg.validate(),
            Err(SwimError::InvalidConfig {
                field: "suspicion_mult",
                ..
            })
        ));
    }

    #[test]
    fn zero_min_suspicion_rejected() {
        let mut cfg = SwimConfig::production();
        cfg.min_suspicion = Duration::ZERO;
        assert!(matches!(
            cfg.validate(),
            Err(SwimError::InvalidConfig {
                field: "min_suspicion",
                ..
            })
        ));
    }

    #[test]
    fn zero_max_piggyback_rejected() {
        let mut cfg = SwimConfig::production();
        cfg.max_piggyback = 0;
        assert!(matches!(
            cfg.validate(),
            Err(SwimError::InvalidConfig {
                field: "max_piggyback",
                ..
            })
        ));
    }

    #[test]
    fn zero_fanout_lambda_rejected() {
        let mut cfg = SwimConfig::production();
        cfg.fanout_lambda = 0;
        assert!(matches!(
            cfg.validate(),
            Err(SwimError::InvalidConfig {
                field: "fanout_lambda",
                ..
            })
        ));
    }
}
