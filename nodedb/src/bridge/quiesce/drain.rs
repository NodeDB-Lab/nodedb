// SPDX-License-Identifier: BUSL-1.1

//! Drain coordination: `begin_drain`, `wait_until_drained`, `clear_drain`.

use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use super::refcount::CollectionQuiesce;

impl CollectionQuiesce {
    /// Mark `(tenant_id, collection)` as draining. After this call:
    /// - `try_start_scan` returns `ScanStartError::Draining`.
    /// - `wait_until_drained` can be awaited to block until open scans
    ///   reach zero.
    ///
    /// Idempotent: calling twice is a no-op on the second call.
    pub fn begin_drain(&self, database_id: u64, tenant_id: u64, collection: &str) {
        let mut inner = self.inner_mut();
        let entry = inner
            .states
            .entry((database_id, tenant_id, collection.to_string()))
            .or_default();
        entry.draining = true;
    }

    /// Stop the drain marker, allowing new scans again. Only called when
    /// the purge is aborted or a recreate happens — on a normal purge
    /// the collection metadata is gone so new scans naturally return
    /// `collection_not_found` from that point on, and the drain entry
    /// is garbage-collected via `forget`.
    pub fn clear_drain(&self, database_id: u64, tenant_id: u64, collection: &str) {
        let mut inner = self.inner_mut();
        if let Some(state) = inner
            .states
            .get_mut(&(database_id, tenant_id, collection.to_string()))
        {
            state.draining = false;
        }
    }

    /// Drop the entry entirely once reclaim has completed. After this,
    /// `is_draining` returns false and `open_scans` is 0. Called by
    /// the purge handler right before emitting the reclaim ack.
    pub fn forget(&self, database_id: u64, tenant_id: u64, collection: &str) {
        let mut inner = self.inner_mut();
        inner
            .states
            .remove(&(database_id, tenant_id, collection.to_string()));
    }

    /// Returns a future that resolves once every open scan against
    /// `(tenant_id, collection)` has completed. Safe to await from the
    /// Control Plane (tokio) — internally uses [`tokio::sync::Notify`]
    /// for wake-up; no polling.
    ///
    /// `begin_drain` must be called before awaiting this future, or
    /// new scans could continue to bump the counter and the future
    /// would never resolve.
    pub fn wait_until_drained(
        self: &Arc<Self>,
        database_id: u64,
        tenant_id: u64,
        collection: &str,
    ) -> WaitDrain {
        WaitDrain {
            registry: Arc::clone(self),
            database_id,
            tenant_id,
            collection: collection.to_string(),
            notified: None,
        }
    }

    fn inner_mut(&self) -> std::sync::MutexGuard<'_, super::refcount::Inner> {
        self.inner.lock().expect("CollectionQuiesce mutex poisoned")
    }
}

/// Future returned by [`CollectionQuiesce::wait_until_drained`].
///
/// Completes when the `(tenant, collection)` open-scan count reaches 0.
/// Implementation detail: each poll takes a fresh `Notify::notified()`
/// future so we don't race against a notification that fires between
/// check and await.
pub struct WaitDrain {
    registry: Arc<CollectionQuiesce>,
    database_id: u64,
    tenant_id: u64,
    collection: String,
    notified: Option<Pin<Box<tokio::sync::futures::Notified<'static>>>>,
}

// Safety: Notified borrows from the Notify inside `registry` (Arc).
// We transmute the lifetime to `'static` because we own the Arc for the
// future's lifetime, guaranteeing the Notify outlives the Notified.
unsafe impl Send for WaitDrain {}

impl Future for WaitDrain {
    type Output = ();

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<()> {
        loop {
            if self
                .registry
                .open_scans(self.database_id, self.tenant_id, &self.collection)
                == 0
            {
                return Poll::Ready(());
            }
            // Arm a notification, then re-check. If a release fires
            // between the check and the arm we handled it on the next
            // iteration (open_scans would be 0 then).
            if self.notified.is_none() {
                let notify: &tokio::sync::Notify = &self.registry.notify;
                // SAFETY: we hold an Arc<CollectionQuiesce>; the Notify
                // inside it outlives `self`.
                let notified: tokio::sync::futures::Notified<'_> = notify.notified();
                let notified: tokio::sync::futures::Notified<'static> =
                    unsafe { std::mem::transmute(notified) };
                self.notified = Some(Box::pin(notified));
            }
            let fut = self.notified.as_mut().expect("just set");
            match fut.as_mut().poll(cx) {
                Poll::Ready(()) => {
                    self.notified = None;
                    // Loop: re-check open_scans.
                    continue;
                }
                Poll::Pending => {
                    // Re-check once before sleeping to close the race
                    // between arming the notified future and a release
                    // that just happened.
                    if self
                        .registry
                        .open_scans(self.database_id, self.tenant_id, &self.collection)
                        == 0
                    {
                        return Poll::Ready(());
                    }
                    return Poll::Pending;
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const DB: u64 = 0;

    #[tokio::test]
    async fn drain_resolves_immediately_when_no_open_scans() {
        let q = CollectionQuiesce::new();
        q.begin_drain(DB, 1, "c");
        q.wait_until_drained(DB, 1, "c").await;
    }

    #[tokio::test]
    async fn drain_waits_for_last_scan_to_release() {
        let q = CollectionQuiesce::new();
        let g1 = q.try_start_scan(DB, 1, "c").unwrap();
        let g2 = q.try_start_scan(DB, 1, "c").unwrap();
        q.begin_drain(DB, 1, "c");

        let q_clone = Arc::clone(&q);
        let drain_task = tokio::spawn(async move {
            q_clone.wait_until_drained(DB, 1, "c").await;
        });

        // Briefly yield so the drain task parks.
        tokio::task::yield_now().await;
        assert!(
            !drain_task.is_finished(),
            "drain must not resolve while scans open"
        );

        drop(g1);
        tokio::task::yield_now().await;
        assert!(
            !drain_task.is_finished(),
            "drain must not resolve with 1 scan still open"
        );

        drop(g2);
        drain_task.await.unwrap();
    }

    #[tokio::test]
    async fn forget_clears_state() {
        let q = CollectionQuiesce::new();
        q.begin_drain(DB, 1, "c");
        q.wait_until_drained(DB, 1, "c").await;
        q.forget(DB, 1, "c");
        assert!(!q.is_draining(DB, 1, "c"));
        assert!(q.try_start_scan(DB, 1, "c").is_ok());
    }
}
