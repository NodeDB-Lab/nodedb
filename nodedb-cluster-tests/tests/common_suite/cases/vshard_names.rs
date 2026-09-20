// SPDX-License-Identifier: BUSL-1.1

//! Deterministic name picking for cross-vShard tests.
//!
//! vShards are per collection (`VShardId::from_collection_in_database`) and
//! per graph endpoint key (`VShardId::from_key`). Both are pure functions of
//! their input bytes, so a test can pick names that land on distinct vShards
//! without probing the cluster.

use nodedb::types::{DatabaseId, VShardId};

/// Upper bound on candidate names tried before giving up.
const MAX_TRIES: u32 = 512;

/// `(first, second)` collection names whose vShard ids differ. `first` is
/// used verbatim; `second` is `{second_prefix}_{i}` for the lowest `i` that
/// hashes away from `first`.
pub fn distinct_vshard_collections(first: &str, second_prefix: &str) -> (String, String) {
    let first_vshard = VShardId::from_collection_in_database(DatabaseId::DEFAULT, first);
    for i in 0..MAX_TRIES {
        let second = format!("{second_prefix}_{i}");
        if VShardId::from_collection_in_database(DatabaseId::DEFAULT, &second) != first_vshard {
            return (first.to_owned(), second);
        }
    }
    panic!(
        "no collection name under prefix {second_prefix} hashes away from {first} \
         in {MAX_TRIES} tries"
    );
}

/// A graph endpoint key `{prefix}_{i}` whose `from_key` vShard differs from
/// `collection`'s own vShard, so an implicit edge task homed on the key is
/// dispatched to a different vShard than the document write.
pub fn key_on_other_vshard(collection: &str, prefix: &str) -> String {
    let coll_vshard = VShardId::from_collection_in_database(DatabaseId::DEFAULT, collection);
    for i in 0..MAX_TRIES {
        let key = format!("{prefix}_{i}");
        if VShardId::from_key(key.as_bytes()) != coll_vshard {
            return key;
        }
    }
    panic!(
        "no key under prefix {prefix} hashes away from collection {collection} \
         in {MAX_TRIES} tries"
    );
}
