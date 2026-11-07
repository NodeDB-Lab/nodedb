// SPDX-License-Identifier: BUSL-1.1

//! Fresh draws from this node's surrogate registry on
//! [`super::SurrogateAssigner`]: `mint_candidates`, `assign_fresh_many`,
//! `assign_anonymous_many` and their one-surrogate forms.
//!
//! A draw of N surrogates takes the registry write lock once and binds all N
//! in one catalog transaction, so it costs one durable sync.
//!
//! A cluster node draws from its reserved batch. When the batch is empty, a
//! draw commits what it drew, awaits the background refill loop's next batch,
//! and never blocks the runtime.

use nodedb_types::{CollectionKey, Surrogate, TenantId};

use super::types::SurrogateAssigner;

impl SurrogateAssigner {
    /// Draw surrogates until `out` holds `n` or this node's reserved batch
    /// runs empty. Returns `false` on an empty batch, after nudging the refill
    /// loop. The caller holds the registry write lock `registry` came from.
    fn try_draw_into<P>(
        &self,
        registry: &crate::control::surrogate::registry::SurrogateRegistry,
        n: usize,
        pk_of: &impl Fn(Surrogate) -> P,
        out: &mut Vec<(Surrogate, P)>,
    ) -> crate::Result<bool> {
        while out.len() < n {
            match self.alloc_locked(registry)? {
                Some(surrogate) => out.push((surrogate, pk_of(surrogate))),
                None => {
                    self.refill_notify.notify_one();
                    self.nudge_refill_if_low(registry);
                    return Ok(false);
                }
            }
        }
        self.nudge_refill_if_low(registry);
        Ok(true)
    }

    /// Draw `n` fresh surrogates and bind each under `pk_of(surrogate)` in
    /// `key`. Every pass binds its draws in one catalog transaction, with the
    /// due watermark row, before the registry lock releases.
    async fn draw_bound_many<P: AsRef<[u8]>>(
        &self,
        key: CollectionKey<'_>,
        tenant_id: TenantId,
        n: usize,
        pk_of: impl Fn(Surrogate) -> P,
    ) -> crate::Result<Vec<(Surrogate, P)>> {
        self.refuse_mirror_bind(key)?;
        let mut out = Vec::with_capacity(n);
        loop {
            let complete = {
                let registry = self.registry_write()?;
                let start = out.len();
                let complete = self.try_draw_into(&registry, n, &pk_of, &mut out)?;
                let fresh = &out[start..];
                if !fresh.is_empty() {
                    let rows: Vec<(&[u8], Surrogate)> = fresh
                        .iter()
                        .map(|(surrogate, pk)| (pk.as_ref(), *surrogate))
                        .collect();
                    let hwm = self.due_flush_hwm(&registry);
                    let bound = self.commit_bindings(key, tenant_id, &rows, hwm)?;
                    if hwm.is_some() {
                        self.finish_committed_flush(&registry)?;
                    }
                    // A fresh surrogate's own key names no other row. A
                    // different winner means the catalog binds a key past the
                    // allocator's watermark.
                    for (binding, &(_, surrogate)) in bound.iter().zip(&rows) {
                        if binding.surrogate != surrogate {
                            return Err(crate::Error::Internal {
                                detail: format!(
                                    "fresh surrogate {} in '{}' collides with key bound to {}",
                                    surrogate.as_u32(),
                                    key.name(),
                                    binding.surrogate.as_u32()
                                ),
                            });
                        }
                    }
                }
                complete
            };
            if complete {
                return Ok(out);
            }
            self.await_reserved_batch().await?;
        }
    }

    /// Draw `n` fresh surrogates from the registry and bind none of them. The
    /// caller binds them: the collection home proposes them in its Raft log
    /// (`surrogate_exchange::authority`).
    pub async fn mint_candidates(&self, n: usize) -> crate::Result<Vec<Surrogate>> {
        let mut out: Vec<(Surrogate, ())> = Vec::with_capacity(n);
        loop {
            let complete = {
                let registry = self.registry_write()?;
                let start = out.len();
                let complete = self.try_draw_into(&registry, n, &|_: Surrogate| (), &mut out)?;
                if out.len() > start {
                    self.maybe_flush(&registry, self.credential_store.catalog())?;
                }
                complete
            };
            if complete {
                return Ok(out.into_iter().map(|(surrogate, ())| surrogate).collect());
            }
            self.await_reserved_batch().await?;
        }
    }

    /// [`Self::mint_candidates`] for one surrogate.
    pub async fn mint_candidate(&self) -> crate::Result<Surrogate> {
        let mut drawn = self.mint_candidates(1).await?;
        drawn.pop().ok_or_else(|| crate::Error::Internal {
            detail: "surrogate mint returned no candidate".into(),
        })
    }

    /// Allocate `n` fresh surrogates for rows with no content primary key, in
    /// one catalog transaction per registry pass. Each binds under its
    /// identity string, as [`Self::assign_fresh`] does.
    pub async fn assign_fresh_many(
        &self,
        key: CollectionKey<'_>,
        tenant_id: TenantId,
        n: usize,
    ) -> crate::Result<Vec<(Surrogate, String)>> {
        self.draw_bound_many(key, tenant_id, n, |surrogate| {
            crate::engine::document::store::RowIdentity::for_surrogate(surrogate).into_string()
        })
        .await
    }

    /// Allocate `n` self-keyed surrogates, as [`Self::assign_anonymous`] does,
    /// in one catalog transaction per registry pass.
    pub async fn assign_anonymous_many(
        &self,
        key: CollectionKey<'_>,
        tenant_id: TenantId,
        n: usize,
    ) -> crate::Result<Vec<Surrogate>> {
        let drawn = self
            .draw_bound_many(key, tenant_id, n, |surrogate| {
                surrogate.as_u32().to_be_bytes()
            })
            .await?;
        Ok(drawn.into_iter().map(|(surrogate, _)| surrogate).collect())
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
    /// The draw, the bind and the flush check run under one registry write
    /// lock. Returns the bound identity string, which the caller uses
    /// verbatim.
    pub async fn assign_fresh(
        &self,
        key: CollectionKey<'_>,
        tenant_id: TenantId,
    ) -> crate::Result<(Surrogate, String)> {
        let mut drawn = self.assign_fresh_many(key, tenant_id, 1).await?;
        drawn.pop().ok_or_else(|| crate::Error::Internal {
            detail: format!("fresh surrogate draw in '{}' returned nothing", key.name()),
        })
    }

    /// Allocate a fresh surrogate for an entity that has no user-facing
    /// primary key (e.g. headless vector inserts). The surrogate is
    /// self-keyed in the catalog (`pk_bytes = surrogate.as_u32().to_be_bytes()`)
    /// so the binding round-trips homogeneously with named-PK rows: a
    /// later lookup via the self-bytes returns the same surrogate, and
    /// the reverse lookup returns the self-bytes back. Keeps the
    /// catalog single-shaped — no special-case "unbound" rows.
    pub async fn assign_anonymous(
        &self,
        key: CollectionKey<'_>,
        tenant_id: TenantId,
    ) -> crate::Result<Surrogate> {
        let mut drawn = self.assign_anonymous_many(key, tenant_id, 1).await?;
        drawn.pop().ok_or_else(|| crate::Error::Internal {
            detail: format!(
                "anonymous surrogate draw in '{}' returned nothing",
                key.name()
            ),
        })
    }
}

#[cfg(test)]
mod tests {
    use std::sync::{Arc, RwLock};

    use nodedb_types::{CollectionKey, DatabaseId, TenantId};

    use super::super::types::SurrogateAssigner;
    use crate::control::security::credential::CredentialStore;
    use crate::control::surrogate::registry::SurrogateRegistry;
    use crate::control::surrogate::wal_appender::{NoopWalAppender, SurrogateWalAppender};

    fn open_test() -> (tempfile::TempDir, SurrogateAssigner) {
        let dir = tempfile::tempdir().unwrap();
        let credentials = Arc::new(CredentialStore::open(&dir.path().join("system.redb")).unwrap());
        let registry = Arc::new(RwLock::new(SurrogateRegistry::new()));
        let wal: Arc<dyn SurrogateWalAppender> = Arc::new(NoopWalAppender);
        (dir, SurrogateAssigner::new(registry, credentials, wal))
    }

    const T0: TenantId = TenantId::new(0);

    #[tokio::test]
    async fn draws_bind_their_identity() {
        let (_dir, assigner) = open_test();
        let key = CollectionKey::from_bare(DatabaseId::DEFAULT, "rows");

        let (fresh, identity) = assigner.assign_fresh(key, T0).await.unwrap();
        assert_eq!(
            assigner.lookup_bound(key, T0, identity.as_bytes()).unwrap(),
            Some(fresh)
        );

        let anonymous = assigner.assign_anonymous(key, T0).await.unwrap();
        assert_ne!(anonymous, fresh);
        assert_eq!(
            assigner
                .lookup_bound(key, T0, &anonymous.as_u32().to_be_bytes())
                .unwrap(),
            Some(anonymous)
        );

        let candidate = assigner.mint_candidate().await.unwrap();
        assert_ne!(candidate, anonymous);
        let (next_fresh, _) = assigner.assign_fresh(key, T0).await.unwrap();
        assert_ne!(next_fresh, candidate);
    }

    /// An async draw on an empty cluster batch waits for the refill to install
    /// the next batch, then draws from it.
    #[tokio::test]
    async fn async_draw_awaits_the_installed_batch() {
        let dir = tempfile::tempdir().unwrap();
        let credentials = Arc::new(CredentialStore::open(&dir.path().join("system.redb")).unwrap());
        let registry = Arc::new(RwLock::new(SurrogateRegistry::from_persisted_cluster(0, 0)));
        let wal: Arc<dyn SurrogateWalAppender> = Arc::new(NoopWalAppender);
        let assigner = Arc::new(SurrogateAssigner::new(registry, credentials, wal));

        let drawer = Arc::clone(&assigner);
        let draw = tokio::spawn(async move {
            let key = CollectionKey::from_bare(DatabaseId::DEFAULT, "rows");
            drawer.assign_anonymous(key, T0).await
        });
        for _ in 0..8 {
            tokio::task::yield_now().await;
        }
        assert!(!draw.is_finished(), "no batch is installed yet");

        let _waiter = assigner.await_reservation_for_test(9);
        assigner.complete_reservation(9, 100, 116);
        let drawn = draw.await.unwrap().unwrap();
        assert!((100..116).contains(&drawn.as_u32()));
    }
}
