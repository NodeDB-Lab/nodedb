// SPDX-License-Identifier: BUSL-1.1

//! Protocol-neutral clone CoW write-path interception.
//!
//! Every dispatch entry point calls [`intercept_and_authorize`] before it can
//! obtain a [`CloneCheckedTask`] — the only capability the Data-Plane dispatch
//! boundary accepts. For a `Shadowed`/`Materializing` clone it applies
//! copy-up/tombstone so the source is never modified. `entry` routes by plan
//! shape; `document`/`kv`/`kv_insert` hold each engine's protocol. A write
//! shape none of them claims is refused outright rather than dispatched
//! unprotected — merging an un-suppressed source row into a target read is
//! only sound where copy-up/tombstone maintains that invariant.

mod copy_source;
mod document;
mod entry;
mod gate;
mod kv;
mod kv_insert;
mod probes;
mod util;

// `CloneCheckedTask`'s inner field is private to `gate`, so this module path
// being fully `pub` (matching `dispatch_authorized_to_data_plane` and friends,
// which integration tests call directly) does not weaken the guarantee: the
// type can still only be constructed by `intercept_and_authorize`.
pub(in crate::control::server) use entry::{
    CloneWriteOutcome, TxnCloneWriteOutcome, maybe_intercept_clone_write,
    maybe_intercept_clone_write_in_txn,
};
pub use gate::{
    ARRAY_DESCRIPTOR_VERSION, CloneCheckedOutcome, CloneCheckedTask, InterceptAndAuthorizeParams,
    array_descriptor, collections_write_lease, intercept_and_authorize,
    intercept_authorize_and_dispatch, write_lease,
};
