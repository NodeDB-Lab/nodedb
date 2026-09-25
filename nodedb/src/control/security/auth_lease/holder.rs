// SPDX-License-Identifier: BUSL-1.1

//! The holder side of this node's authorization lease.
//!
//! A statement is planned against local authorization state only while the
//! lease is valid. The lease ends on this node's clock before it ends on the
//! leader's (see [`super::timing`]), so once the leader treats it as expired
//! no statement here can still plan under it.

use std::sync::Mutex;
use std::time::Instant;

/// The end of this node's lease, if it holds one.
#[derive(Debug, Default)]
pub struct LeaseHolder {
    valid_until: Mutex<Option<Instant>>,
}

impl LeaseHolder {
    /// Extend the lease to `until`. A grant never shortens a lease already
    /// held: the leader granted each one against the state it covers.
    pub fn install(&self, until: Instant) {
        let mut valid_until = self.valid_until.lock().unwrap_or_else(|p| p.into_inner());
        if valid_until.is_none_or(|current| current < until) {
            *valid_until = Some(until);
        }
    }

    /// Whether the lease is valid at `now`.
    pub fn is_valid_at(&self, now: Instant) -> bool {
        self.valid_until
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .is_some_and(|until| now < until)
    }

    /// When the lease ends, if one was granted.
    pub fn valid_until(&self) -> Option<Instant> {
        *self.valid_until.lock().unwrap_or_else(|p| p.into_inner())
    }

    /// Wait until a lease is valid, polling every `poll`, or refuse once
    /// `timeout` passes.
    pub async fn await_valid(
        &self,
        timeout: std::time::Duration,
        poll: std::time::Duration,
    ) -> crate::Result<()> {
        let deadline = Instant::now() + timeout;
        while !self.is_valid_at(Instant::now()) {
            if Instant::now() >= deadline {
                return Err(crate::Error::AuthorizationStateBehind {
                    detail: format!("no authorization lease was granted within {timeout:?}"),
                });
            }
            tokio::time::sleep(poll).await;
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use super::*;
    use crate::control::security::auth_lease::LeaseTiming;

    #[test]
    fn a_lease_is_valid_until_its_margin_adjusted_end() {
        let timing = LeaseTiming::from_raft(Duration::from_millis(150), Duration::from_millis(50))
            .expect("timing");
        let holder = LeaseHolder::default();
        let sent_at = Instant::now();
        assert!(!holder.is_valid_at(sent_at));

        holder.install(timing.holder_expiry(sent_at, timing.lease));
        assert!(holder.is_valid_at(sent_at + Duration::from_millis(99)));
        // The leader's lease still runs at 100ms, but the holder's has ended.
        assert!(!holder.is_valid_at(sent_at + Duration::from_millis(100)));
        assert!(!holder.is_valid_at(sent_at + timing.lease));
    }

    #[test]
    fn an_older_grant_never_shortens_the_lease() {
        let holder = LeaseHolder::default();
        let now = Instant::now();
        holder.install(now + Duration::from_millis(200));
        holder.install(now + Duration::from_millis(100));
        assert!(holder.is_valid_at(now + Duration::from_millis(150)));
    }
}
