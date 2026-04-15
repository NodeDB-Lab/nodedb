//! Single SWIM probe round.
//!
//! One probe round follows the Lifeguard sequence:
//!
//! 1. Pick a target via [`ProbeScheduler::next_target`].
//! 2. Send a `Ping` and wait `probe_timeout` for the matching `Ack`.
//! 3. If no `Ack` arrives, pick `k` helpers via
//!    [`ProbeScheduler::pick_helpers`] and send each a `PingReq`. Wait
//!    `probe_timeout` more for any forwarded `Ack`.
//! 4. On total failure, the target is reported back as [`ProbeOutcome::Suspect`]
//!    — the runner translates that into a suspicion-timer entry and a
//!    `Suspect` rumour applied to [`MembershipList`].
//!
//! The [`InflightProbes`] registry correlates outbound probe ids with
//! incoming `Ack`/`Nack` datagrams from the runner's recv loop.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use nodedb_types::NodeId;
use tokio::sync::{Mutex, oneshot};
use tokio::time::timeout;

use crate::swim::error::SwimError;
use crate::swim::incarnation::Incarnation;
use crate::swim::wire::{Ping, PingReq, ProbeId, SwimMessage};

use super::scheduler::ProbeScheduler;
use super::transport::Transport;
use crate::swim::dissemination::DisseminationQueue;
use crate::swim::membership::MembershipList;

/// Upper bound on concurrent inflight probes. The detector only issues a
/// handful per round so 4 096 is a safety ceiling, not a tuning knob.
const MAX_INFLIGHT: usize = 4096;

/// Registry of outstanding probe ids awaiting a response.
#[derive(Debug, Default)]
pub struct InflightProbes {
    map: Mutex<HashMap<ProbeId, oneshot::Sender<SwimMessage>>>,
}

impl InflightProbes {
    pub fn new() -> Self {
        Self::default()
    }

    /// Register a new probe id and return the receiver that fires when
    /// the matching `Ack`/`Nack` is routed via [`Self::resolve`].
    pub async fn register(
        &self,
        probe_id: ProbeId,
    ) -> Result<oneshot::Receiver<SwimMessage>, SwimError> {
        let (tx, rx) = oneshot::channel();
        let mut guard = self.map.lock().await;
        if guard.len() >= MAX_INFLIGHT {
            return Err(SwimError::ProbeInflightOverflow);
        }
        guard.insert(probe_id, tx);
        Ok(rx)
    }

    /// Drop a probe id without firing its receiver (timeouts).
    pub async fn forget(&self, probe_id: ProbeId) {
        self.map.lock().await.remove(&probe_id);
    }

    /// Route an incoming `Ack`/`Nack` to the probe that registered
    /// `probe_id`. Does nothing if no match is found — late responses
    /// are discarded silently.
    pub async fn resolve(&self, probe_id: ProbeId, msg: SwimMessage) {
        if let Some(tx) = self.map.lock().await.remove(&probe_id) {
            let _ = tx.send(msg);
        }
    }
}

/// Outcome of [`execute_round`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ProbeOutcome {
    /// Target acked either directly or via a helper; no action required.
    Acked {
        target: NodeId,
        incarnation: Incarnation,
    },
    /// No peers to probe — cluster is solo.
    Idle,
    /// Target failed to respond; runner should mark it `Suspect` and
    /// arm a suspicion timer.
    Suspect { target: NodeId },
}

/// All inputs to a single probe round. Passed to [`ProbeRound::execute`]
/// instead of a long argument list so the call sites stay readable and
/// the compiler can tell us at a glance what each field means.
pub struct ProbeRound<'a, F: Fn() -> ProbeId> {
    pub scheduler: &'a mut ProbeScheduler,
    pub membership: &'a MembershipList,
    pub transport: &'a Arc<dyn Transport>,
    pub inflight: &'a Arc<InflightProbes>,
    pub dissemination: &'a Arc<DisseminationQueue>,
    pub probe_timeout: Duration,
    pub k_indirect: usize,
    pub max_piggyback: usize,
    pub fanout_lambda: u32,
    pub next_probe_id: F,
    pub local_incarnation: Incarnation,
}

impl<'a, F: Fn() -> ProbeId> ProbeRound<'a, F> {
    /// Execute the round. See the module-level docs for the Lifeguard
    /// sequence this implements.
    pub async fn execute(self) -> Result<ProbeOutcome, SwimError> {
        let Self {
            scheduler,
            membership,
            transport,
            inflight,
            dissemination,
            probe_timeout,
            k_indirect,
            max_piggyback,
            fanout_lambda,
            next_probe_id,
            local_incarnation,
        } = self;

        let fanout = DisseminationQueue::fanout_threshold(membership.len(), fanout_lambda);

        let Some((target_id, target_addr)) = scheduler.next_target(membership) else {
            return Ok(ProbeOutcome::Idle);
        };

        // ── Direct probe ────────────────────────────────────────────────
        let direct_id = next_probe_id();
        let direct_rx = inflight.register(direct_id).await?;
        let local = membership.local_node_id().clone();
        transport
            .send(
                target_addr,
                SwimMessage::Ping(Ping {
                    probe_id: direct_id,
                    from: local.clone(),
                    incarnation: local_incarnation,
                    piggyback: dissemination.take_for_message(max_piggyback, fanout),
                }),
            )
            .await?;

        match timeout(probe_timeout, direct_rx).await {
            Ok(Ok(SwimMessage::Ack(ack))) => {
                return Ok(ProbeOutcome::Acked {
                    target: target_id,
                    incarnation: ack.incarnation,
                });
            }
            Ok(Ok(_)) | Ok(Err(_)) | Err(_) => {
                inflight.forget(direct_id).await;
            }
        }

        // ── Indirect probes ─────────────────────────────────────────────
        let helpers = scheduler.pick_helpers(membership, &target_id, k_indirect);
        if helpers.is_empty() {
            return Ok(ProbeOutcome::Suspect { target: target_id });
        }

        let mut indirect_rxs: Vec<(ProbeId, oneshot::Receiver<SwimMessage>)> = Vec::new();
        for (_, helper_addr) in &helpers {
            let pid = next_probe_id();
            let rx = inflight.register(pid).await?;
            indirect_rxs.push((pid, rx));
            transport
                .send(
                    *helper_addr,
                    SwimMessage::PingReq(PingReq {
                        probe_id: pid,
                        from: local.clone(),
                        target: target_id.clone(),
                        target_addr: target_addr.to_string(),
                        piggyback: dissemination.take_for_message(max_piggyback, fanout),
                    }),
                )
                .await?;
        }

        let indirect_ids: Vec<ProbeId> = indirect_rxs.iter().map(|(id, _)| *id).collect();
        let any_ack = wait_for_any_ack(indirect_rxs, probe_timeout).await;
        for id in indirect_ids {
            inflight.forget(id).await;
        }

        if let Some(ack_inc) = any_ack {
            Ok(ProbeOutcome::Acked {
                target: target_id,
                incarnation: ack_inc,
            })
        } else {
            Ok(ProbeOutcome::Suspect { target: target_id })
        }
    }
}

/// Wait until any of the indirect probe receivers yields an `Ack`, or
/// until the timeout elapses. Returns the responder's incarnation on
/// success.
async fn wait_for_any_ack(
    mut rxs: Vec<(ProbeId, oneshot::Receiver<SwimMessage>)>,
    deadline: Duration,
) -> Option<Incarnation> {
    if rxs.is_empty() {
        return None;
    }
    let futs = rxs
        .drain(..)
        .map(|(_, rx)| async move {
            match rx.await {
                Ok(SwimMessage::Ack(ack)) => Some(ack.incarnation),
                _ => None,
            }
        })
        .collect::<Vec<_>>();
    let any = async move {
        let mut set = tokio::task::JoinSet::new();
        for fut in futs {
            set.spawn(fut);
        }
        while let Some(res) = set.join_next().await {
            if let Ok(Some(inc)) = res {
                return Some(inc);
            }
        }
        None
    };
    timeout(deadline, any).await.unwrap_or_default()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::swim::config::SwimConfig;
    use crate::swim::detector::transport::TransportFabric;
    use crate::swim::incarnation::Incarnation;
    use crate::swim::member::MemberState;
    use crate::swim::member::record::MemberUpdate;
    use crate::swim::wire::Ack;
    use std::net::{IpAddr, Ipv4Addr, SocketAddr};
    use std::sync::atomic::{AtomicU64, Ordering};

    fn addr(p: u16) -> SocketAddr {
        SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), p)
    }

    fn cfg() -> SwimConfig {
        SwimConfig {
            probe_interval: Duration::from_millis(100),
            probe_timeout: Duration::from_millis(40),
            indirect_probes: 2,
            suspicion_mult: 4,
            min_suspicion: Duration::from_millis(500),
            initial_incarnation: Incarnation::ZERO,
            max_piggyback: 6,
            fanout_lambda: 3,
        }
    }

    async fn membership_with_peers(
        local: &str,
        local_port: u16,
        peers: &[(&str, u16, MemberState)],
    ) -> Arc<MembershipList> {
        let list = Arc::new(MembershipList::new_local(
            NodeId::new(local),
            addr(local_port),
            Incarnation::ZERO,
        ));
        for (id, port, state) in peers {
            list.apply(&MemberUpdate {
                node_id: NodeId::new(*id),
                addr: addr(*port).to_string(),
                state: *state,
                incarnation: Incarnation::new(1),
            });
        }
        list
    }

    fn pid_gen(start: u64) -> impl Fn() -> ProbeId {
        let counter = AtomicU64::new(start);
        move || ProbeId::new(counter.fetch_add(1, Ordering::Relaxed))
    }

    #[tokio::test]
    async fn idle_when_no_peers() {
        let fab = TransportFabric::new();
        let local = Arc::new(fab.bind(addr(7000)).await) as Arc<dyn Transport>;
        let list = membership_with_peers("local", 7000, &[]).await;
        let mut sched = ProbeScheduler::with_seed(1);
        let inflight = Arc::new(InflightProbes::new());
        let dissemination = Arc::new(DisseminationQueue::new());
        let outcome = ProbeRound {
            scheduler: &mut sched,
            membership: &list,
            transport: &local,
            inflight: &inflight,
            dissemination: &dissemination,
            probe_timeout: cfg().probe_timeout,
            k_indirect: 2,
            max_piggyback: 6,
            fanout_lambda: 3,
            next_probe_id: pid_gen(1),
            local_incarnation: Incarnation::ZERO,
        }
        .execute()
        .await
        .expect("run");
        assert_eq!(outcome, ProbeOutcome::Idle);
    }

    #[tokio::test(start_paused = true)]
    async fn suspect_when_target_silent_and_no_helpers() {
        let fab = TransportFabric::new();
        let local = Arc::new(fab.bind(addr(7000)).await) as Arc<dyn Transport>;
        // Bind target so the send() does not drop unbinded, but never
        // reply — the probe round will time out naturally. Paused
        // runtime auto-advances the timeout once no task is runnable.
        let _silent_target = fab.bind(addr(7001)).await;
        let list = membership_with_peers("local", 7000, &[("n1", 7001, MemberState::Alive)]).await;
        let mut sched = ProbeScheduler::with_seed(1);
        let inflight = Arc::new(InflightProbes::new());
        let dissemination = Arc::new(DisseminationQueue::new());
        let outcome = ProbeRound {
            scheduler: &mut sched,
            membership: &list,
            transport: &local,
            inflight: &inflight,
            dissemination: &dissemination,
            probe_timeout: cfg().probe_timeout,
            k_indirect: 2,
            max_piggyback: 6,
            fanout_lambda: 3,
            next_probe_id: pid_gen(1),
            local_incarnation: Incarnation::ZERO,
        }
        .execute()
        .await
        .expect("run");
        assert_eq!(
            outcome,
            ProbeOutcome::Suspect {
                target: NodeId::new("n1")
            }
        );
    }

    #[tokio::test(start_paused = true)]
    async fn direct_ack_succeeds() {
        let fab = TransportFabric::new();
        let local = Arc::new(fab.bind(addr(7000)).await) as Arc<dyn Transport>;
        let target = fab.bind(addr(7001)).await;
        let list = membership_with_peers("local", 7000, &[("n1", 7001, MemberState::Alive)]).await;
        let mut sched = ProbeScheduler::with_seed(1);
        let inflight = Arc::new(InflightProbes::new());

        // Responder task: read the Ping and route an Ack back via the
        // inflight registry (simulating the runner).
        let inflight_responder = Arc::clone(&inflight);
        let responder = tokio::spawn(async move {
            let (_from, msg) = target.recv().await.expect("recv");
            match msg {
                SwimMessage::Ping(p) => {
                    inflight_responder
                        .resolve(
                            p.probe_id,
                            SwimMessage::Ack(Ack {
                                probe_id: p.probe_id,
                                from: NodeId::new("n1"),
                                incarnation: Incarnation::new(3),
                                piggyback: vec![],
                            }),
                        )
                        .await;
                }
                _ => panic!("expected Ping"),
            }
        });

        let dissemination = Arc::new(DisseminationQueue::new());
        let outcome = ProbeRound {
            scheduler: &mut sched,
            membership: &list,
            transport: &local,
            inflight: &inflight,
            dissemination: &dissemination,
            probe_timeout: cfg().probe_timeout,
            k_indirect: 2,
            max_piggyback: 6,
            fanout_lambda: 3,
            next_probe_id: pid_gen(1),
            local_incarnation: Incarnation::ZERO,
        }
        .execute()
        .await
        .expect("run");
        responder.await.expect("responder");
        assert_eq!(
            outcome,
            ProbeOutcome::Acked {
                target: NodeId::new("n1"),
                incarnation: Incarnation::new(3),
            }
        );
    }

    #[tokio::test(start_paused = true)]
    async fn indirect_ack_saves_target() {
        let fab = TransportFabric::new();
        let local = Arc::new(fab.bind(addr(7000)).await) as Arc<dyn Transport>;
        // Target bound but silent on the direct channel.
        let _silent = fab.bind(addr(7001)).await;
        let helper = fab.bind(addr(7002)).await;
        let list = membership_with_peers(
            "local",
            7000,
            &[
                ("n1", 7001, MemberState::Alive),
                ("n2", 7002, MemberState::Alive),
            ],
        )
        .await;
        let mut sched = ProbeScheduler::with_seed(1);
        let inflight = Arc::new(InflightProbes::new());

        // Helper task: forwards any PingReq it sees into an Ack via the
        // inflight registry. Paused-runtime auto-advance drives the
        // direct-ping timeout on the main task.
        let inflight_helper = Arc::clone(&inflight);
        let responder = tokio::spawn(async move {
            loop {
                let (_from, msg) = match helper.recv().await {
                    Ok(v) => v,
                    Err(_) => return,
                };
                if let SwimMessage::PingReq(req) = msg {
                    inflight_helper
                        .resolve(
                            req.probe_id,
                            SwimMessage::Ack(Ack {
                                probe_id: req.probe_id,
                                from: req.target.clone(),
                                incarnation: Incarnation::new(9),
                                piggyback: vec![],
                            }),
                        )
                        .await;
                    return;
                }
            }
        });

        let dissemination = Arc::new(DisseminationQueue::new());
        let outcome = ProbeRound {
            scheduler: &mut sched,
            membership: &list,
            transport: &local,
            inflight: &inflight,
            dissemination: &dissemination,
            probe_timeout: cfg().probe_timeout,
            k_indirect: 2,
            max_piggyback: 6,
            fanout_lambda: 3,
            next_probe_id: pid_gen(1),
            local_incarnation: Incarnation::ZERO,
        }
        .execute()
        .await
        .expect("run");
        let _ = responder.await;
        // Either direct (unlikely — n1 is silent) or indirect ack via n2.
        // Whichever path fires, the outcome must be Acked.
        assert!(matches!(outcome, ProbeOutcome::Acked { .. }));
    }

    #[tokio::test]
    async fn inflight_overflow_is_reported() {
        let inflight = InflightProbes::new();
        // Force map to max by registering MAX_INFLIGHT ids.
        for i in 0..MAX_INFLIGHT as u64 {
            inflight.register(ProbeId::new(i)).await.expect("room");
        }
        let err = inflight
            .register(ProbeId::new(u64::MAX))
            .await
            .expect_err("full");
        assert!(matches!(err, SwimError::ProbeInflightOverflow));
    }
}
