// SPDX-License-Identifier: BUSL-1.1

//! One merged pass over a collection's bound rows: each row's staged value,
//! or the TTL delta of a row with no staged value.
//!
//! The pass reads the identity bindings in key-byte order of the encoded
//! identity, so a resolve, a replay and every replica emit the rows of one
//! collection in that one order. It holds no row it has passed.

use std::borrow::Cow;

use nodedb_types::RowIdentity;

use super::staged::{CollKey, Staged, TxnOverlay};
use super::staged_sidecar::StagedTtl;

/// What one bound row of a collection holds.
pub enum StagedSlot<'a> {
    /// A staged value or tombstone under `surrogate`.
    Written {
        surrogate: u32,
        staged: Cow<'a, Staged>,
    },
    /// Only a TTL delta on a base row.
    TtlOnly(StagedTtl),
}

impl TxnOverlay {
    /// Every bound row of `coll_key` with what it holds. With `ttl_only`
    /// false, a row that holds only a TTL delta is left out.
    pub fn iter_slots_for_collection<'a>(
        &'a self,
        coll_key: &CollKey,
        ttl_only: bool,
    ) -> impl Iterator<Item = crate::Result<(RowIdentity, StagedSlot<'a>)>> + 'a {
        let poisoned = self.guard().err();
        let overlay = match poisoned {
            Some(_) => None,
            None => self.collections.get(coll_key),
        };
        let reader = self.space.reader();
        let entries = overlay.into_iter().flat_map(move |overlay| {
            overlay
                .doc_id_to_surrogate
                .iter()
                .filter_map(move |binding| {
                    let slot = |doc_id: RowIdentity,
                                surrogate: u32|
                     -> crate::Result<Option<(RowIdentity, StagedSlot<'a>)>> {
                        if let Some(staged) = overlay.by_surrogate.get(&surrogate, reader)? {
                            return Ok(Some((doc_id, StagedSlot::Written { surrogate, staged })));
                        }
                        if !ttl_only {
                            return Ok(None);
                        }
                        Ok(overlay
                            .ttl_by_surrogate
                            .get(&surrogate, reader)?
                            .map(|ttl| (doc_id, StagedSlot::TtlOnly(ttl.into_owned()))))
                    };
                    match binding
                        .and_then(|(doc_id, surrogate)| slot(doc_id, surrogate.into_owned()))
                    {
                        Ok(Some(entry)) => Some(Ok(entry)),
                        Ok(None) => None,
                        Err(e) => Some(Err(e)),
                    }
                })
        });
        poisoned.map(Err).into_iter().chain(entries)
    }
}
