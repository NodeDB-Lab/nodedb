// SPDX-License-Identifier: BUSL-1.1

//! The read/allocate/bind operations on [`super::SurrogateAssigner`]:
//! `assign`, `assign_fresh`, `assign_anonymous`, `bind`, and `lookup`.

use nodedb_types::{DatabaseId, TenantId};

use nodedb_types::Surrogate;

use super::types::SurrogateAssigner;

impl SurrogateAssigner {
    /// Resolve `(collection, pk_bytes)` to a stable surrogate.
    ///
    /// - If the credential store has no catalog (in-memory test fixture),
    ///   returns `Surrogate::ZERO`. Production state always wires a
    ///   redb-backed `CredentialStore::open` so this branch never fires.
    /// - If a binding already exists, return it (no allocation, no flush).
    /// - Else: allocate one surrogate, persist the binding, and check
    ///   the registry's flush threshold; flush durably if tripped.
    ///
    /// Allocation + catalog write happen inside one critical section
    /// on the registry write-lock so the registry hwm and the
    /// persisted PK row cannot diverge under concurrent assigners.
    pub fn assign(
        &self,
        database_id: DatabaseId,
        tenant_id: TenantId,
        collection: &str,
        pk_bytes: &[u8],
    ) -> crate::Result<Surrogate> {
        let catalog = self.credential_store.catalog();

        // Fast-path: existing binding. Done under a read lock — most
        // production calls land here once the per-collection working
        // set has been observed.
        if let Some(s) =
            catalog.get_surrogate_for_pk(database_id, tenant_id, collection, pk_bytes)?
        {
            return Ok(s);
        }

        // Slow path: allocate + persist + maybe flush. The write lock
        // guards the (allocate, write-pk-row) pair so two concurrent
        // assigners can't both observe "missing", both allocate, and
        // both write — the second would silently overwrite the
        // first's binding with a different surrogate.
        //
        // In cluster mode the allocation source is the node's reserved
        // batch (`try_alloc_reserved`); when the batch is empty the
        // background refiller normally has the next batch ready, so the
        // lock-free draw simply succeeds. The synchronous `ensure_batch`
        // refill remains only as a rare safety net — see
        // `cluster_reserve` for the full hot-path contract. It MUST run
        // WITHOUT the registry write lock held (the applier installs the
        // batch under a read guard; holding the write lock across the
        // wait would deadlock), so we drop the lock, refill, and retry.
        loop {
            let registry = self.registry_write()?;
            // Re-check inside the lock: another assigner may have raced
            // us between the read above and the lock acquisition.
            if let Some(s) =
                catalog.get_surrogate_for_pk(database_id, tenant_id, collection, pk_bytes)?
            {
                return Ok(s);
            }
            let surrogate = match self.alloc_locked(&registry)? {
                Some(s) => {
                    // Proactive top-up: if the batch is running low, nudge
                    // the background refiller so the next reservation lands
                    // before the pool drains — keeping the blocking Raft
                    // round-trip OFF this latency-critical insert path.
                    self.nudge_refill_if_low(&registry);
                    s
                }
                None => {
                    // Cluster mode, empty batch: the background refiller
                    // hasn't caught up. Release the lock, nudge it, and fall
                    // back to a synchronous reservation as a rare safety net
                    // so liveness is preserved even if the refiller stalled.
                    drop(registry);
                    self.refill_notify.notify_one();
                    self.ensure_batch()?;
                    continue;
                }
            };
            catalog.put_surrogate(database_id, tenant_id, collection, pk_bytes, surrogate)?;
            // Emit a durable WAL bind before the lock releases. Order is
            // load-bearing: a crash between catalog write and bind append
            // is invisible (the catalog row is already on disk via redb's
            // own WAL); a crash before the catalog write leaves nothing
            // to recover; a crash between bind append and lock release is
            // recovered by replaying the bind into the catalog (idempotent
            // via the two-table overwrite).
            self.wal_appender.record_bind_to_wal(
                database_id,
                tenant_id,
                surrogate.as_u32(),
                collection,
                pk_bytes,
            )?;
            self.maybe_flush(&registry, catalog)?;
            return Ok(surrogate);
        }
    }

    /// Allocate a FRESH surrogate for a row with no content primary key — a
    /// collection whose primary key is the auto-generated `_rowid` (no
    /// `PRIMARY KEY` was declared), or a timeseries row.
    ///
    /// [`assign`](Self::assign) has a fast-path lookup. This does not. Every
    /// call allocates a new value, so N rows get N distinct surrogates.
    ///
    /// The surrogate self-binds under its identity, so a later keyed lookup
    /// resolves. The identity is the surrogate's decimal string, matching the
    /// `_rowid` value the Data Plane writes for an auto-`_rowid` row, so
    /// `WHERE _rowid = N` resolves back to it.
    ///
    /// Reuses `assign`'s bind/flush machinery, so the hwm advance persists
    /// and Raft-proposes identically. Returns the bound identity string,
    /// which the caller uses verbatim.
    pub fn assign_fresh(
        &self,
        database_id: DatabaseId,
        tenant_id: TenantId,
        collection: &str,
    ) -> crate::Result<(Surrogate, String)> {
        let catalog = self.credential_store.catalog();

        // Allocate + self-bind + maybe-flush under the registry write lock,
        // with the same empty-batch refill fallback as the `assign` slow path.
        loop {
            let registry = self.registry_write()?;
            let surrogate = match self.alloc_locked(&registry)? {
                Some(s) => {
                    self.nudge_refill_if_low(&registry);
                    s
                }
                None => {
                    drop(registry);
                    self.refill_notify.notify_one();
                    self.ensure_batch()?;
                    continue;
                }
            };
            let pk =
                crate::engine::document::store::RowIdentity::for_surrogate(surrogate).into_string();
            let pk_bytes = pk.as_bytes();
            catalog.put_surrogate(database_id, tenant_id, collection, pk_bytes, surrogate)?;
            self.wal_appender.record_bind_to_wal(
                database_id,
                tenant_id,
                surrogate.as_u32(),
                collection,
                pk_bytes,
            )?;
            self.maybe_flush(&registry, catalog)?;
            return Ok((surrogate, pk));
        }
    }

    /// Read-only lookup: return the surrogate previously bound to
    /// `(collection, pk_bytes)` without ever allocating or writing.
    /// Used by point-read/update/delete planning where a missing
    /// binding means the row does not exist (semantic no-op).
    ///
    /// Delegates straight to `catalog.get_surrogate_for_pk`: returns
    /// `Ok(None)` when no binding exists for the key, `Ok(Some(surrogate))`
    /// when one does, and `Err` on a catalog read failure. There is no
    /// catalog-less fallback here — every caller goes through the real
    /// catalog lookup, so a missing binding is always reported as `None`
    /// rather than synthesized as `Surrogate::ZERO`.
    pub fn lookup(
        &self,
        database_id: DatabaseId,
        tenant_id: TenantId,
        collection: &str,
        pk_bytes: &[u8],
    ) -> crate::Result<Option<Surrogate>> {
        let catalog = self.credential_store.catalog();
        catalog.get_surrogate_for_pk(database_id, tenant_id, collection, pk_bytes)
    }

    /// Bind `(collection, pk_bytes)` to a *carried* surrogate without ever
    /// allocating, resolving concurrent carried values **first-wins** and
    /// returning the *authoritative* surrogate the caller must use.
    ///
    /// Used on the Raft apply path: a coordinator assigned the surrogate at
    /// plan time, embedded it in the plan, and carried it on the wire; the
    /// owner installs that identity rather than drawing a fresh (divergent)
    /// one from its own allocator. Because two different non-owner
    /// coordinators can each assign a *different* surrogate (from disjoint
    /// HiLo batches) for the *same* key, the owner must resolve this
    /// deterministically: the FIRST binding wins and every later carried
    /// value is discarded. The returned `Surrogate` is the authoritative one
    /// (the already-bound value when one exists, else the carried value just
    /// persisted) and MUST be used as the storage key by the caller —
    /// otherwise the owner would create duplicate rows under different
    /// surrogates for the same key.
    ///
    /// - No catalog (in-memory test fixture): returns `Ok(surrogate)` — the
    ///   carried value is authoritative, nothing to persist (mirrors
    ///   `assign`'s catalog-less branch).
    /// - Binding already exists: returns `Ok(existing)` — first-wins, never
    ///   overwrites, discards the carried value even if it differs.
    /// - Otherwise: persist the binding + emit the durable WAL bind under the
    ///   registry write lock (same order as `assign`), `restore_hwm` so the
    ///   global watermark stays ahead of the carried value, and return the
    ///   now-bound `surrogate`.
    ///
    /// Replay/retry is idempotent: re-applying the same entry finds the
    /// existing binding in the pre-check and returns it without writing.
    ///
    /// Crucially this never touches `alloc_locked`/`maybe_flush`: the
    /// allocator counter must NOT advance on a bind — that would burn a
    /// surrogate and diverge from the coordinator.
    pub fn bind(
        &self,
        database_id: DatabaseId,
        tenant_id: TenantId,
        collection: &str,
        pk_bytes: &[u8],
        surrogate: Surrogate,
    ) -> crate::Result<Surrogate> {
        let catalog = self.credential_store.catalog();

        // First-wins pre-check under a read lock: if any binding is already
        // installed (replay, retry, or a competing coordinator's carried
        // value applied first) it is authoritative — return it, never
        // overwrite, discard the carried value even if it differs.
        if let Some(existing) =
            catalog.get_surrogate_for_pk(database_id, tenant_id, collection, pk_bytes)?
        {
            return Ok(existing);
        }

        // Hold the registry write lock across (re-check, persist binding,
        // WAL bind, hwm advance) so it is one critical section — same lock
        // discipline as `assign`, which also serializes on this write lock.
        // `restore_hwm` itself is atomic on the counter; we call it through
        // the held guard rather than re-locking (which would deadlock on
        // this std `RwLock`).
        let registry = self.registry_write()?;
        // Re-check under the lock (TOCTOU): a concurrent `assign`/`bind` on
        // this node could have written between the pre-check and the lock.
        // First-wins still applies — return the existing value.
        if let Some(existing) =
            catalog.get_surrogate_for_pk(database_id, tenant_id, collection, pk_bytes)?
        {
            return Ok(existing);
        }
        catalog.put_surrogate(database_id, tenant_id, collection, pk_bytes, surrogate)?;
        self.wal_appender.record_bind_to_wal(
            database_id,
            tenant_id,
            surrogate.as_u32(),
            collection,
            pk_bytes,
        )?;
        // Advance the local watermark past the carried value so a later
        // LOCAL `assign`/`assign_anonymous` on this node can never re-issue
        // it. Idempotent and monotonic — never lowers, never advances the
        // allocator's draw position (only the hwm floor).
        registry
            .restore_hwm(surrogate.as_u32())
            .map_err(|e| crate::Error::Internal {
                detail: format!("surrogate bind restore_hwm failed: {e}"),
            })?;
        Ok(surrogate)
    }

    /// Allocate a fresh surrogate for an entity that has no user-facing
    /// primary key (e.g. headless vector inserts). The surrogate is
    /// self-keyed in the catalog (`pk_bytes = surrogate.as_u32().to_be_bytes()`)
    /// so the binding round-trips homogeneously with named-PK rows: a
    /// later lookup via the self-bytes returns the same surrogate, and
    /// the reverse lookup returns the self-bytes back. Keeps the
    /// catalog single-shaped — no special-case "unbound" rows.
    pub fn assign_anonymous(
        &self,
        database_id: DatabaseId,
        tenant_id: TenantId,
        collection: &str,
    ) -> crate::Result<Surrogate> {
        let catalog = self.credential_store.catalog();

        loop {
            let registry = self.registry_write()?;
            let surrogate = match self.alloc_locked(&registry)? {
                Some(s) => {
                    self.nudge_refill_if_low(&registry);
                    s
                }
                None => {
                    drop(registry);
                    self.refill_notify.notify_one();
                    self.ensure_batch()?;
                    continue;
                }
            };
            let self_bytes = surrogate.as_u32().to_be_bytes();
            catalog.put_surrogate(database_id, tenant_id, collection, &self_bytes, surrogate)?;
            self.wal_appender.record_bind_to_wal(
                database_id,
                tenant_id,
                surrogate.as_u32(),
                collection,
                &self_bytes,
            )?;
            self.maybe_flush(&registry, catalog)?;
            return Ok(surrogate);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::super::types::SurrogateAssigner;
    use nodedb_types::{DatabaseId, Surrogate, TenantId};
    use std::sync::{Arc, RwLock};

    use crate::control::security::credential::CredentialStore;
    use crate::control::surrogate::registry::SurrogateRegistry;
    use crate::control::surrogate::wal_appender::{NoopWalAppender, SurrogateWalAppender};

    fn open_test() -> (tempfile::TempDir, Arc<SurrogateAssigner>) {
        let dir = tempfile::tempdir().unwrap();
        let credentials = Arc::new(CredentialStore::open(&dir.path().join("system.redb")).unwrap());
        let reg = Arc::new(RwLock::new(SurrogateRegistry::new()));
        let wal: Arc<dyn SurrogateWalAppender> = Arc::new(NoopWalAppender);
        let a = Arc::new(SurrogateAssigner::new(reg, credentials, wal));
        (dir, a)
    }

    const T0: TenantId = TenantId::new(0);

    #[test]
    fn assign_is_idempotent_for_same_pk() {
        let (_dir, a) = open_test();
        let s1 = a
            .assign(DatabaseId::DEFAULT, T0, "users", b"alice")
            .unwrap();
        let s2 = a
            .assign(DatabaseId::DEFAULT, T0, "users", b"alice")
            .unwrap();
        assert_eq!(s1, s2);
        assert_eq!(s1, Surrogate::new(1));
    }

    #[test]
    fn assign_distinct_tenants_do_not_collide() {
        let (_dir, a) = open_test();
        let t1 = TenantId::new(1);
        let t2 = TenantId::new(2);
        let s1 = a
            .assign(DatabaseId::DEFAULT, t1, "users", b"alice")
            .unwrap();
        let s2 = a
            .assign(DatabaseId::DEFAULT, t2, "users", b"alice")
            .unwrap();
        assert_ne!(s1, s2);
    }

    #[test]
    fn assign_distinct_pks_returns_distinct_surrogates() {
        let (_dir, a) = open_test();
        let s1 = a
            .assign(DatabaseId::DEFAULT, T0, "users", b"alice")
            .unwrap();
        let s2 = a.assign(DatabaseId::DEFAULT, T0, "users", b"bob").unwrap();
        assert_ne!(s1, s2);
    }

    #[test]
    fn assign_writes_reverse_binding() {
        let (_dir, a) = open_test();
        let s = a
            .assign(DatabaseId::DEFAULT, T0, "users", b"alice")
            .unwrap();
        let cat = a.credential_store.catalog();
        assert_eq!(
            cat.get_pk_for_surrogate(DatabaseId::DEFAULT, T0, "users", s)
                .unwrap(),
            Some(b"alice".to_vec())
        );
    }

    #[test]
    fn assign_persists_hwm_at_flush_threshold() {
        let (_dir, a) = open_test();
        // Allocate just up to and across the 1024 ops threshold.
        let n = crate::control::surrogate::registry::FLUSH_OPS_THRESHOLD as usize;
        for i in 0..n {
            let pk = format!("u{i}");
            let _ = a
                .assign(DatabaseId::DEFAULT, T0, "users", pk.as_bytes())
                .unwrap();
        }
        // Either threshold (1024 ops or 200 ms elapsed) may fire
        // first; assert only that the catalog persisted *some*
        // checkpoint inside the (0, n] band.
        let cat = a.credential_store.catalog();
        let persisted = cat.get_surrogate_hwm().unwrap();
        assert!(persisted > 0 && persisted <= n as u32);
    }
}
