// SPDX-License-Identifier: BUSL-1.1

//! The destination surrogate of each restored row that is keyed by its
//! backed-up surrogate: columnar keys, vector rows and sync-frame text.
//!
//! A row the backup carries a key for re-issues under the destination's
//! surrogate of that key, which the collection's home assigns. A backup can
//! hold a row with no carried key. A collection purge that runs between the
//! backup's data capture and its bind capture removes the row's bind, but
//! not the row. The row's backed-up surrogate can name another row in the
//! destination. Such a row takes a fresh surrogate there, under the identity
//! a keyless row carries: the surrogate's decimal text.

use nodedb_types::{CollectionKey, RowIdentity, Surrogate};

use crate::Error;
use crate::control::state::SharedState;
use crate::types::{TenantId, TraceId};

/// The key one restored row binds in the destination.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) enum RowKey {
    /// A key the backup carries: a bind, or one the row itself names.
    Carried(Vec<u8>),
    /// No carried key. The row takes a fresh surrogate.
    Keyless,
}

/// The destination surrogate and the bound key of each row of `keys`, in
/// `keys` order. `collection` names the collection in errors.
///
/// Carried keys resolve at the collection's home in one batch. Keyless rows
/// draw fresh surrogates that no row holds. Each unit's identities bind the
/// pair on every replica.
pub(super) async fn destination_keys(
    state: &SharedState,
    key: CollectionKey<'_>,
    tenant: TenantId,
    collection: &str,
    keys: &[RowKey],
) -> Result<Vec<(Surrogate, Vec<u8>)>, Error> {
    let carried: Vec<&[u8]> = keys
        .iter()
        .filter_map(|row| match row {
            RowKey::Carried(pk) => Some(pk.as_slice()),
            RowKey::Keyless => None,
        })
        .collect();
    let keyless = keys.len() - carried.len();
    let assigned = if carried.is_empty() {
        Vec::new()
    } else {
        crate::control::server::surrogate_exchange::assign_surrogates_routed(
            state,
            key,
            tenant,
            &carried,
            TraceId::ZERO,
        )
        .await?
    };
    let minted = if keyless == 0 {
        Vec::new()
    } else {
        state.surrogate_assigner.mint_candidates(keyless).await?
    };
    if assigned.len() != carried.len() || minted.len() != keyless {
        return Err(Error::Internal {
            detail: format!(
                "restore of '{collection}' bound {} of {} keyed and {} of {keyless} keyless rows",
                assigned.len(),
                carried.len(),
                minted.len(),
            ),
        });
    }
    Ok(pair_keys(keys, assigned, minted))
}

/// Pair each row of `keys` with its surrogate: the next of `assigned` for a
/// carried key, the next of `minted` for a keyless row. A keyless row binds
/// the decimal identity of its fresh surrogate.
fn pair_keys(
    keys: &[RowKey],
    assigned: Vec<Surrogate>,
    minted: Vec<Surrogate>,
) -> Vec<(Surrogate, Vec<u8>)> {
    let mut assigned = assigned.into_iter();
    let mut minted = minted.into_iter();
    keys.iter()
        .filter_map(|row| match row {
            RowKey::Carried(pk) => assigned.next().map(|surrogate| (surrogate, pk.clone())),
            RowKey::Keyless => minted.next().map(|surrogate| {
                let identity = RowIdentity::for_surrogate(surrogate);
                (surrogate, identity.as_str().as_bytes().to_vec())
            }),
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Carried keys take the assigned surrogates in order, and keyless rows
    /// the minted ones under their decimal identity.
    #[test]
    fn rows_pair_with_their_surrogates_in_order() {
        let keys = [
            RowKey::Carried(b"a".to_vec()),
            RowKey::Keyless,
            RowKey::Carried(b"b".to_vec()),
        ];
        let paired = pair_keys(
            &keys,
            vec![Surrogate::new(10), Surrogate::new(11)],
            vec![Surrogate::new(42)],
        );
        assert_eq!(
            paired,
            vec![
                (Surrogate::new(10), b"a".to_vec()),
                (Surrogate::new(42), b"42".to_vec()),
                (Surrogate::new(11), b"b".to_vec()),
            ]
        );
    }
}
