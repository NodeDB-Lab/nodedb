// SPDX-License-Identifier: BUSL-1.1

//! Per-connection DDL transaction buffer.
//!
//! When a connection session is inside a `BEGIN` block and executes DDL
//! statements (CREATE, DROP, ALTER), the `propose_catalog_entry`
//! path checks this buffer. If the buffer is active (non-None), the
//! entry is pushed into it instead of being proposed immediately.
//!
//! On `COMMIT`, the `ddl_flush` module drains the buffer as one atomic
//! batch, so either all DDL in the transaction commits or none does.
//!
//! On `ROLLBACK`, the buffer is cleared without proposing.

use std::cell::RefCell;

use crate::control::catalog_entry::CatalogEntry;

use super::audit_context::AuditCtx;
use super::ddl_effect::DeferredDdlEffect;

/// One buffered DDL statement: the unstamped `CatalogEntry`
/// plus the optional audit context captured from
/// [`super::audit_context::current()`] at buffer time. The audit
/// context is stamped at *statement* time, not at COMMIT time, so
/// each sub-entry's audit record correctly names the DDL that
/// produced it (not just the COMMIT).
#[derive(Debug, Clone)]
pub struct BufferedDdl {
    pub entry: CatalogEntry,
    pub audit: Option<AuditCtx>,
    /// Engine side effects the statement that buffered this entry owes at
    /// COMMIT, in statement order.
    pub effects: Vec<DeferredDdlEffect>,
}

/// Unstamped DDL entries buffered during a transaction.
pub type DdlBuffer = Vec<BufferedDdl>;

/// Run `f` against this connection's DDL buffer slot, or return `default`
/// when the caller runs outside any connection scope (Event Plane system
/// transactions, bootstrap, background DDL).
fn with_slot<T>(default: T, f: impl FnOnce(&RefCell<Option<DdlBuffer>>) -> T) -> T {
    super::conn_scope::with_scope(default, |scope| f(&scope.ddl_buffer))
}

/// Activate the DDL buffer for the current connection. Any subsequent call to
/// `try_buffer` will push into this buffer instead of returning `false`.
pub fn activate() {
    with_slot((), |b| {
        let mut guard = b.borrow_mut();
        if guard.is_none() {
            *guard = Some(Vec::new());
        }
    });
}

/// Try to buffer an unstamped DDL entry. Returns `true` if the buffer is
/// active and the entry was pushed. Returns `false` if no buffer is active
/// (caller should prepare and propose normally).
pub fn try_buffer(entry: CatalogEntry) -> bool {
    with_slot(false, |b| {
        let mut guard = b.borrow_mut();
        if let Some(buf) = guard.as_mut() {
            buf.push(BufferedDdl {
                entry,
                audit: super::audit_context::current(),
                effects: Vec::new(),
            });
            true
        } else {
            false
        }
    })
}

/// Attach an engine side effect to the entry buffered last, to run at
/// COMMIT. Returns `false` when no buffer is active or it holds no entry: the
/// caller then runs the effect at once.
pub fn defer_effect(effect: DeferredDdlEffect) -> bool {
    with_slot(false, |b| {
        let mut guard = b.borrow_mut();
        match guard.as_mut().and_then(|buf| buf.last_mut()) {
            Some(last) => {
                last.effects.push(effect);
                true
            }
            None => false,
        }
    })
}

/// Remove every deferred engine side effect from the active buffer, in
/// statement order, leaving its entries in place. COMMIT calls this once,
/// before it takes the entries.
pub fn drain_effects() -> Vec<DeferredDdlEffect> {
    with_slot(Vec::new(), |b| {
        b.borrow_mut()
            .as_mut()
            .map(|buf| {
                buf.iter_mut()
                    .flat_map(|item| std::mem::take(&mut item.effects))
                    .collect()
            })
            .unwrap_or_default()
    })
}

/// Run `f` over the entries this connection's open transaction has buffered,
/// in statement order. Returns `None` when no buffer is active — outside a
/// transaction, and outside any connection scope, there is nothing to overlay.
pub fn with_buffered<T>(f: impl FnOnce(&[BufferedDdl]) -> T) -> Option<T> {
    with_slot(None, |b| b.borrow().as_ref().map(|buf| f(buf)))
}

/// Take the accumulated buffer contents and deactivate. Returns
/// `None` if the buffer was never activated.
pub fn take() -> Option<DdlBuffer> {
    let taken = with_slot(None, |b| b.borrow_mut().take());
    super::ephemeral_sequence::clear();
    taken
}

/// Deactivate and discard the buffer without returning its contents.
pub fn discard() {
    with_slot((), |b| {
        let _ = b.borrow_mut().take();
    });
    super::ephemeral_sequence::clear();
}

/// Returns `true` if a DDL buffer is currently active on this connection.
pub fn is_active() -> bool {
    with_slot(false, |b| b.borrow().is_some())
}

/// Number of DDL statements buffered in this connection's active
/// transaction. Returns 0 if no buffer is active.
pub fn buffer_len() -> usize {
    with_slot(0, |b| b.borrow().as_ref().map(|v| v.len()).unwrap_or(0))
}

/// Truncate the active buffer to `len` entries, discarding everything
/// buffered after that point. No-op if no buffer is active.
pub fn truncate(len: usize) {
    with_slot((), |b| {
        if let Some(buf) = b.borrow_mut().as_mut() {
            buf.truncate(len);
        }
    });
    super::ephemeral_sequence::clear();
}

#[cfg(test)]
mod tests {
    use super::super::conn_scope;
    use super::*;

    fn sample_entry(name: &str) -> CatalogEntry {
        CatalogEntry::DeleteSequence {
            database_id: 0,
            tenant_id: 1,
            name: name.to_string(),
        }
    }

    #[tokio::test]
    async fn inactive_buffer_does_not_capture() {
        conn_scope::scoped(async {
            assert!(!try_buffer(sample_entry("one")));
            assert!(!is_active());
        })
        .await;
    }

    #[tokio::test]
    async fn active_buffer_captures() {
        conn_scope::scoped(async {
            activate();
            assert!(is_active());
            assert!(try_buffer(sample_entry("one")));
            assert!(try_buffer(sample_entry("two")));
            assert_eq!(buffer_len(), 2);
            let buf = take().expect("buffer active");
            assert_eq!(buf.len(), 2);
            assert!(matches!(
                &buf[0].entry,
                CatalogEntry::DeleteSequence { name, .. } if name == "one"
            ));
            assert!(matches!(
                &buf[1].entry,
                CatalogEntry::DeleteSequence { name, .. } if name == "two"
            ));
            assert!(!is_active());
        })
        .await;
    }

    #[tokio::test]
    async fn a_deferred_effect_rides_the_last_entry_and_a_truncate_drops_it() {
        use super::super::ddl_effect::DeferredDdlEffect;
        let drop = |name: &str| DeferredDdlEffect::SortedIndexDrop {
            tenant_id: crate::types::TenantId::new(1),
            database_id: crate::types::DatabaseId::DEFAULT,
            collection: "scores".into(),
            index_name: name.into(),
        };
        conn_scope::scoped(async {
            activate();
            assert!(
                !defer_effect(drop("none")),
                "an empty buffer takes no effect"
            );
            try_buffer(sample_entry("one"));
            assert!(defer_effect(drop("kept")));
            try_buffer(sample_entry("two"));
            assert!(defer_effect(drop("dropped")));
            truncate(1);
            let effects = drain_effects();
            assert_eq!(effects.len(), 1);
            assert!(matches!(
                &effects[0],
                DeferredDdlEffect::SortedIndexDrop { index_name, .. } if index_name == "kept"
            ));
            assert!(drain_effects().is_empty(), "draining removes them");
            assert_eq!(buffer_len(), 1, "the entries stay");
        })
        .await;
    }

    #[tokio::test]
    async fn discard_clears_buffer() {
        conn_scope::scoped(async {
            activate();
            try_buffer(sample_entry("one"));
            discard();
            assert!(!is_active());
            assert!(take().is_none());
        })
        .await;
    }

    #[tokio::test]
    async fn take_on_inactive_returns_none() {
        conn_scope::scoped(async {
            assert!(take().is_none());
        })
        .await;
    }

    #[test]
    fn outside_any_scope_is_inert() {
        assert!(!is_active());
        assert!(!try_buffer(sample_entry("one")));
        assert!(take().is_none());
        assert_eq!(buffer_len(), 0);
        discard();
    }

    /// The buffer must survive a worker-thread hop mid-transaction: this is the
    /// exact failure a thread-local had, where post-`BEGIN` statements polled on
    /// another worker proposed DDL immediately instead of buffering it.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn buffer_survives_worker_thread_migration() {
        conn_scope::scoped(async {
            activate();
            assert!(try_buffer(sample_entry("one")));
            for _ in 0..64 {
                tokio::task::yield_now().await;
            }
            assert!(is_active());
            assert!(try_buffer(sample_entry("two")));
            let buf = take().expect("buffer survives the hop");
            assert_eq!(buf.len(), 2);
        })
        .await;
    }
}
