// SPDX-License-Identifier: BUSL-1.1

//! Filesystem naming for vector checkpoints: directories, generation
//! directories, and the per-index checkpoint key.
//!
//! The write path, the load path, and reclaim all build every path through
//! these helpers so the three can never drift. A path divergence between writer
//! and reader is silent, and its symptom is indistinguishable from data loss.

use nodedb_types::DatabaseId;

use crate::data::executor::checkpoint_key::{CheckpointEngine, CheckpointKey};
use crate::types::TenantId;

/// Filename of the manifest that names the live generation.
pub(crate) const VECTOR_CKPT_MANIFEST: &str = "MANIFEST";

/// Canonical path for a core's vector checkpoint directory.
///
/// The per-core subdir is required because `data_dir` is shared across all TPC
/// cores; without it a flat directory made every core load every collection's
/// index. It also means the loader needs no core-ownership filter — a core only
/// ever sees its own indexes.
pub(crate) fn vector_ckpt_dir(data_dir: &std::path::Path, core_id: usize) -> std::path::PathBuf {
    data_dir.join("vector-ckpt").join(format!("core-{core_id}"))
}

/// Directory holding one generation's per-index files.
pub(crate) fn vector_ckpt_gen_dir(
    ckpt_dir: &std::path::Path,
    generation: u64,
) -> std::path::PathBuf {
    ckpt_dir.join(format!("gen-{generation}"))
}

/// The checkpoint key for one index: `coll_key` is `{collection}` for the
/// default index and `{collection}:{field}` for a named-field one.
///
/// The single authority on the key, shared with reclaim so a DROP can never
/// miss a file the write path produced. The file name is a fixed-length hash
/// of this key, so no collection or field name length can overflow it.
pub(crate) fn vector_ckpt_key(db: u64, tid: u64, coll_key: &str) -> CheckpointKey {
    CheckpointKey::new(
        CheckpointEngine::Vector,
        db,
        tid,
        vec![coll_key.to_string()],
    )
}

/// The `(DatabaseId, TenantId, coll_key)` map key a stored vector checkpoint
/// key restores under, or `None` when the key is not a vector key.
pub(crate) fn vector_map_key(key: &CheckpointKey) -> Option<(DatabaseId, TenantId, String)> {
    match (key.engine, key.parts.as_slice()) {
        (CheckpointEngine::Vector, [coll_key]) => Some((
            DatabaseId::new(key.database_id),
            TenantId::new(key.tenant_id),
            coll_key.clone(),
        )),
        _ => None,
    }
}

/// Parse a `"{db}:{tid}:{coll_key}"` string (the in-memory `BuildComplete.key`
/// form, produced by `vector_build_key`) back into the
/// `(DatabaseId, TenantId, String)` tuple map key.
///
/// Returns `None` when the string is not in that format — i.e. it does not have
/// at least three `:`-separated components whose first two parse as `u64`
/// (db, tid). `coll_key` is the verbatim remainder and may itself contain `:`
/// (e.g. `collection:field`).
pub(super) fn parse_build_key(s: &str) -> Option<(DatabaseId, TenantId, String)> {
    let mut it = s.splitn(3, ':');
    let db_str = it.next()?;
    let tid_str = it.next()?;
    let coll_key = it.next()?;
    let db = db_str.parse::<u64>().ok()?;
    let tid = tid_str.parse::<u64>().ok()?;
    Some((
        DatabaseId::new(db),
        TenantId::new(tid),
        coll_key.to_string(),
    ))
}

#[cfg(test)]
mod tests {
    use super::*;

    /// `vector_ckpt_dir` must isolate cores sharing one `data_dir` — without the
    /// per-core subdir every core loads every other core's indexes.
    #[test]
    fn per_core_dirs_are_distinct() {
        let base = std::path::Path::new("/data");
        let d0 = vector_ckpt_dir(base, 0);
        let d1 = vector_ckpt_dir(base, 1);
        assert_ne!(d0, d1, "different cores must get different checkpoint dirs");
        assert!(d0.to_str().expect("utf8 path").contains("core-0"));
        assert!(d1.to_str().expect("utf8 path").contains("core-1"));
    }

    #[test]
    fn generation_dirs_are_distinct() {
        let base = std::path::Path::new("/data/vector-ckpt/core-0");
        assert_ne!(vector_ckpt_gen_dir(base, 0), vector_ckpt_gen_dir(base, 1));
    }

    #[test]
    fn key_roundtrips_through_map_key() {
        for coll_key in ["docs", "docs:emb", "2/docs", "a-tenant-b", "-key-"] {
            let key = vector_ckpt_key(3, 9, coll_key);
            let (db, tid, parsed) = vector_map_key(&key).expect("vector key");
            assert_eq!(db, DatabaseId::new(3));
            assert_eq!(tid, TenantId::new(9));
            assert_eq!(parsed, coll_key);
        }
    }

    /// A 255-byte collection with a 200-byte field still gets a short name.
    #[test]
    fn long_names_get_a_short_file_name() {
        let coll_key = format!("{}:{}", "c".repeat(255), "f".repeat(200));
        let name = vector_ckpt_key(0, 1, &coll_key).file_name();
        assert!(name.len() < 64, "{name}");
        assert!(nodedb_types::is_plain_path_component(&name));
    }

    #[test]
    fn non_vector_key_is_none() {
        let key = CheckpointKey::new(CheckpointEngine::Crdt, 0, 1, vec!["docs".into()]);
        assert!(vector_map_key(&key).is_none());
    }

    /// A named-field key keeps its `:` in the collection remainder — the parse
    /// splits only the first two components.
    #[test]
    fn field_qualified_key_keeps_its_remainder() {
        let (_, _, coll) = parse_build_key("0:1:docs:emb").expect("must parse");
        assert_eq!(coll, "docs:emb");
    }

    #[test]
    fn non_numeric_key_is_none() {
        assert!(parse_build_key("a:b:c").is_none());
        assert!(parse_build_key("0:1").is_none());
    }
}
