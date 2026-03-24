//! C FFI bindings for NodeDB-Lite.
//!
//! Exposes the `NodeDb` trait as C-callable functions for Swift (iOS)
//! and Kotlin/JNI (Android) interop.
//!
//! Memory model:
//! - `nodedb_open` creates a handle; `nodedb_close` frees it.
//! - String parameters (`*const c_char`) are borrowed — caller owns the memory.
//! - Returned strings/buffers are Rust-allocated — caller must free via `nodedb_free_*`.
//! - Error codes: 0 = success, -1 = null pointer, -2 = invalid UTF-8, -3 = operation failed.

pub mod jni_bridge;

use std::ffi::{CStr, CString};
use std::os::raw::c_char;
use std::sync::Arc;

use nodedb_client::NodeDb;
use nodedb_lite::{NodeDbLite, RedbStorage};

/// Error codes returned by FFI functions.
pub const NODEDB_OK: i32 = 0;
pub const NODEDB_ERR_NULL: i32 = -1;
pub const NODEDB_ERR_UTF8: i32 = -2;
pub const NODEDB_ERR_FAILED: i32 = -3;
pub const NODEDB_ERR_NOT_FOUND: i32 = -4;

/// Opaque handle to a NodeDB-Lite database.
///
/// Created by `nodedb_open`, freed by `nodedb_close`.
pub struct NodeDbHandle {
    db: Arc<NodeDbLite<RedbStorage>>,
    rt: tokio::runtime::Runtime,
}

/// Open or create a NodeDB-Lite database at the given path.
///
/// Returns an opaque handle on success, NULL on failure.
/// The caller must call `nodedb_close` to free the handle.
///
/// # Safety
/// `path` must be a valid null-terminated UTF-8 string.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn nodedb_open(path: *const c_char, peer_id: u64) -> *mut NodeDbHandle {
    let path = match ptr_to_str(path) {
        Some(s) => s,
        None => return std::ptr::null_mut(),
    };

    let rt = match tokio::runtime::Builder::new_multi_thread()
        .worker_threads(1)
        .enable_all()
        .build()
    {
        Ok(rt) => rt,
        Err(_) => return std::ptr::null_mut(),
    };

    let storage = if path == ":memory:" {
        match RedbStorage::open_in_memory() {
            Ok(s) => s,
            Err(_) => return std::ptr::null_mut(),
        }
    } else {
        match RedbStorage::open(path) {
            Ok(s) => s,
            Err(_) => return std::ptr::null_mut(),
        }
    };

    let db = match rt.block_on(NodeDbLite::open(storage, peer_id)) {
        Ok(db) => Arc::new(db),
        Err(_) => return std::ptr::null_mut(),
    };

    Box::into_raw(Box::new(NodeDbHandle { db, rt }))
}

/// Close a NodeDB-Lite database and free the handle.
///
/// # Safety
/// `handle` must be a valid pointer returned by `nodedb_open`, or NULL (no-op).
#[unsafe(no_mangle)]
pub unsafe extern "C" fn nodedb_close(handle: *mut NodeDbHandle) {
    if !handle.is_null() {
        drop(unsafe { Box::from_raw(handle) });
    }
}

/// Flush all in-memory state to disk.
///
/// # Safety
/// `handle` must be a valid pointer returned by `nodedb_open`.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn nodedb_flush(handle: *mut NodeDbHandle) -> i32 {
    let Some(h) = handle_ref(handle) else {
        return NODEDB_ERR_NULL;
    };
    match h.rt.block_on(h.db.flush()) {
        Ok(()) => NODEDB_OK,
        Err(_) => NODEDB_ERR_FAILED,
    }
}

// ─── Vector Operations ───────────────────────────────────────────────

/// Insert a vector into a collection.
///
/// # Safety
/// All pointer parameters must be valid. `embedding` must point to `dim` floats.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn nodedb_vector_insert(
    handle: *mut NodeDbHandle,
    collection: *const c_char,
    id: *const c_char,
    embedding: *const f32,
    dim: usize,
) -> i32 {
    let Some(h) = handle_ref(handle) else {
        return NODEDB_ERR_NULL;
    };
    let Some(collection) = ptr_to_str(collection) else {
        return NODEDB_ERR_UTF8;
    };
    let Some(id) = ptr_to_str(id) else {
        return NODEDB_ERR_UTF8;
    };
    if embedding.is_null() || dim == 0 {
        return NODEDB_ERR_NULL;
    }
    let emb = unsafe { std::slice::from_raw_parts(embedding, dim) };

    match h.rt.block_on(h.db.vector_insert(collection, id, emb, None)) {
        Ok(()) => NODEDB_OK,
        Err(_) => NODEDB_ERR_FAILED,
    }
}

/// Search for the k nearest vectors. Results are written as JSON to `out_json`.
///
/// # Safety
/// `query` must point to `dim` floats. `out_json` receives a malloc'd C string
/// that the caller must free via `nodedb_free_string`.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn nodedb_vector_search(
    handle: *mut NodeDbHandle,
    collection: *const c_char,
    query: *const f32,
    dim: usize,
    k: usize,
    out_json: *mut *mut c_char,
) -> i32 {
    let Some(h) = handle_ref(handle) else {
        return NODEDB_ERR_NULL;
    };
    let Some(collection) = ptr_to_str(collection) else {
        return NODEDB_ERR_UTF8;
    };
    if query.is_null() || dim == 0 || out_json.is_null() {
        return NODEDB_ERR_NULL;
    }
    let q = unsafe { std::slice::from_raw_parts(query, dim) };

    match h.rt.block_on(h.db.vector_search(collection, q, k, None)) {
        Ok(results) => {
            // Serialize results as JSON array.
            let json_items: Vec<serde_json::Value> = results
                .iter()
                .map(|r| {
                    serde_json::json!({
                        "id": r.id,
                        "distance": r.distance,
                    })
                })
                .collect();
            let json_str = serde_json::to_string(&json_items).unwrap_or_else(|_| "[]".into());
            match CString::new(json_str) {
                Ok(cs) => {
                    unsafe { *out_json = cs.into_raw() };
                    NODEDB_OK
                }
                Err(_) => NODEDB_ERR_FAILED,
            }
        }
        Err(_) => NODEDB_ERR_FAILED,
    }
}

/// Delete a vector by ID.
///
/// # Safety
/// All pointer parameters must be valid.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn nodedb_vector_delete(
    handle: *mut NodeDbHandle,
    collection: *const c_char,
    id: *const c_char,
) -> i32 {
    let Some(h) = handle_ref(handle) else {
        return NODEDB_ERR_NULL;
    };
    let Some(collection) = ptr_to_str(collection) else {
        return NODEDB_ERR_UTF8;
    };
    let Some(id) = ptr_to_str(id) else {
        return NODEDB_ERR_UTF8;
    };
    match h.rt.block_on(h.db.vector_delete(collection, id)) {
        Ok(()) => NODEDB_OK,
        Err(_) => NODEDB_ERR_FAILED,
    }
}

// ─── Graph Operations ────────────────────────────────────────────────

/// Insert a directed graph edge.
///
/// # Safety
/// All pointer parameters must be valid null-terminated UTF-8.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn nodedb_graph_insert_edge(
    handle: *mut NodeDbHandle,
    from: *const c_char,
    to: *const c_char,
    edge_type: *const c_char,
) -> i32 {
    let Some(h) = handle_ref(handle) else {
        return NODEDB_ERR_NULL;
    };
    let Some(from) = ptr_to_str(from) else {
        return NODEDB_ERR_UTF8;
    };
    let Some(to) = ptr_to_str(to) else {
        return NODEDB_ERR_UTF8;
    };
    let Some(edge_type) = ptr_to_str(edge_type) else {
        return NODEDB_ERR_UTF8;
    };

    let from_id = nodedb_types::id::NodeId::new(from);
    let to_id = nodedb_types::id::NodeId::new(to);

    match h
        .rt
        .block_on(h.db.graph_insert_edge(&from_id, &to_id, edge_type, None))
    {
        Ok(_) => NODEDB_OK,
        Err(_) => NODEDB_ERR_FAILED,
    }
}

/// Traverse the graph from a start node. Results written as JSON to `out_json`.
///
/// # Safety
/// `start` must be valid UTF-8. `out_json` receives a malloc'd C string.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn nodedb_graph_traverse(
    handle: *mut NodeDbHandle,
    start: *const c_char,
    depth: u8,
    out_json: *mut *mut c_char,
) -> i32 {
    let Some(h) = handle_ref(handle) else {
        return NODEDB_ERR_NULL;
    };
    let Some(start) = ptr_to_str(start) else {
        return NODEDB_ERR_UTF8;
    };
    if out_json.is_null() {
        return NODEDB_ERR_NULL;
    }

    let start_id = nodedb_types::id::NodeId::new(start);

    match h.rt.block_on(h.db.graph_traverse(&start_id, depth, None)) {
        Ok(subgraph) => {
            let json = serde_json::json!({
                "nodes": subgraph.nodes.iter().map(|n| serde_json::json!({
                    "id": n.id.as_str(),
                    "depth": n.depth,
                })).collect::<Vec<_>>(),
                "edges": subgraph.edges.iter().map(|e| serde_json::json!({
                    "from": e.from.as_str(),
                    "to": e.to.as_str(),
                    "label": e.label,
                })).collect::<Vec<_>>(),
            });
            let json_str = serde_json::to_string(&json).unwrap_or_else(|_| "{}".into());
            match CString::new(json_str) {
                Ok(cs) => {
                    unsafe { *out_json = cs.into_raw() };
                    NODEDB_OK
                }
                Err(_) => NODEDB_ERR_FAILED,
            }
        }
        Err(_) => NODEDB_ERR_FAILED,
    }
}

// ─── Document Operations ─────────────────────────────────────────────

/// Get a document by ID. Result written as JSON to `out_json`.
///
/// Returns `NODEDB_ERR_NOT_FOUND` if the document doesn't exist.
///
/// # Safety
/// All pointer parameters must be valid.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn nodedb_document_get(
    handle: *mut NodeDbHandle,
    collection: *const c_char,
    id: *const c_char,
    out_json: *mut *mut c_char,
) -> i32 {
    let Some(h) = handle_ref(handle) else {
        return NODEDB_ERR_NULL;
    };
    let Some(collection) = ptr_to_str(collection) else {
        return NODEDB_ERR_UTF8;
    };
    let Some(id) = ptr_to_str(id) else {
        return NODEDB_ERR_UTF8;
    };
    if out_json.is_null() {
        return NODEDB_ERR_NULL;
    }

    match h.rt.block_on(h.db.document_get(collection, id)) {
        Ok(Some(doc)) => {
            let json_str = serde_json::to_string(&doc).unwrap_or_else(|_| "{}".into());
            match CString::new(json_str) {
                Ok(cs) => {
                    unsafe { *out_json = cs.into_raw() };
                    NODEDB_OK
                }
                Err(_) => NODEDB_ERR_FAILED,
            }
        }
        Ok(None) => NODEDB_ERR_NOT_FOUND,
        Err(_) => NODEDB_ERR_FAILED,
    }
}

/// Put (insert or update) a document. Body is a JSON string.
///
/// If the JSON has no `"id"` field or it is empty, a UUIDv7 is auto-generated.
/// If `out_id` is non-NULL, the document ID (auto-generated or provided) is
/// written as a malloc'd C string that the caller must free via `nodedb_free_string`.
///
/// # Safety
/// All pointer parameters must be valid. `out_id` may be NULL.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn nodedb_document_put(
    handle: *mut NodeDbHandle,
    collection: *const c_char,
    json_body: *const c_char,
    out_id: *mut *mut c_char,
) -> i32 {
    let Some(h) = handle_ref(handle) else {
        return NODEDB_ERR_NULL;
    };
    let Some(collection) = ptr_to_str(collection) else {
        return NODEDB_ERR_UTF8;
    };
    let Some(json_str) = ptr_to_str(json_body) else {
        return NODEDB_ERR_UTF8;
    };

    let mut doc: nodedb_types::Document = match serde_json::from_str(json_str) {
        Ok(d) => d,
        Err(_) => return NODEDB_ERR_FAILED,
    };

    if doc.id.is_empty() {
        doc.id = nodedb_types::id_gen::uuid_v7();
    }

    // Write the document ID to out_id if requested.
    if !out_id.is_null()
        && let Ok(cs) = CString::new(doc.id.clone())
    {
        unsafe { *out_id = cs.into_raw() };
    }

    match h.rt.block_on(h.db.document_put(collection, doc)) {
        Ok(()) => NODEDB_OK,
        Err(_) => NODEDB_ERR_FAILED,
    }
}

/// Delete a document by ID.
///
/// # Safety
/// All pointer parameters must be valid.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn nodedb_document_delete(
    handle: *mut NodeDbHandle,
    collection: *const c_char,
    id: *const c_char,
) -> i32 {
    let Some(h) = handle_ref(handle) else {
        return NODEDB_ERR_NULL;
    };
    let Some(collection) = ptr_to_str(collection) else {
        return NODEDB_ERR_UTF8;
    };
    let Some(id) = ptr_to_str(id) else {
        return NODEDB_ERR_UTF8;
    };
    match h.rt.block_on(h.db.document_delete(collection, id)) {
        Ok(()) => NODEDB_OK,
        Err(_) => NODEDB_ERR_FAILED,
    }
}

// ─── ID Generation ──────────────────────────────────────────────────

/// Generate a UUIDv7 (time-sortable, recommended for primary keys).
///
/// Returns a malloc'd C string that the caller must free via `nodedb_free_string`.
///
/// # Safety
/// `out` must be a valid pointer to a `*mut c_char`.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn nodedb_generate_id(out: *mut *mut c_char) -> i32 {
    if out.is_null() {
        return NODEDB_ERR_NULL;
    }
    let id = nodedb_types::id_gen::uuid_v7();
    match CString::new(id) {
        Ok(cs) => {
            unsafe { *out = cs.into_raw() };
            NODEDB_OK
        }
        Err(_) => NODEDB_ERR_FAILED,
    }
}

/// Generate an ID of the specified type.
///
/// Supported types: "uuidv7", "uuidv4", "ulid", "cuid2", "nanoid".
/// Returns a malloc'd C string that the caller must free via `nodedb_free_string`.
///
/// # Safety
/// `id_type` must be a valid null-terminated UTF-8 string. `out` must be a valid pointer.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn nodedb_generate_id_typed(
    id_type: *const c_char,
    out: *mut *mut c_char,
) -> i32 {
    if out.is_null() {
        return NODEDB_ERR_NULL;
    }
    let Some(id_type_str) = ptr_to_str(id_type) else {
        return NODEDB_ERR_UTF8;
    };
    let id = match nodedb_types::id_gen::generate_by_type(id_type_str) {
        Some(id) => id,
        None => return NODEDB_ERR_FAILED,
    };
    match CString::new(id) {
        Ok(cs) => {
            unsafe { *out = cs.into_raw() };
            NODEDB_OK
        }
        Err(_) => NODEDB_ERR_FAILED,
    }
}

// ─── Memory Management ──────────────────────────────────────────────

/// Free a string returned by nodedb_* functions.
///
/// # Safety
/// `ptr` must be a string previously returned by a nodedb function, or NULL.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn nodedb_free_string(ptr: *mut c_char) {
    if !ptr.is_null() {
        drop(unsafe { CString::from_raw(ptr) });
    }
}

// ─── Internal Helpers ────────────────────────────────────────────────

/// # Safety
/// `ptr` must be a valid null-terminated C string, or null.
fn ptr_to_str<'a>(ptr: *const c_char) -> Option<&'a str> {
    if ptr.is_null() {
        return None;
    }
    // SAFETY: caller guarantees ptr is a valid null-terminated C string.
    unsafe { CStr::from_ptr(ptr) }.to_str().ok()
}

/// # Safety
/// `handle` must be a valid `NodeDbHandle` pointer, or null.
fn handle_ref<'a>(handle: *mut NodeDbHandle) -> Option<&'a NodeDbHandle> {
    if handle.is_null() {
        None
    } else {
        // SAFETY: caller guarantees handle is valid and not freed.
        Some(unsafe { &*handle })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::ffi::CString;

    #[test]
    fn open_close_in_memory() {
        let path = CString::new(":memory:").unwrap();
        unsafe {
            let handle = nodedb_open(path.as_ptr(), 1);
            assert!(!handle.is_null());
            nodedb_close(handle);
        }
    }

    #[test]
    fn null_handle_returns_error() {
        unsafe {
            assert_eq!(nodedb_flush(std::ptr::null_mut()), NODEDB_ERR_NULL);
        }
    }

    #[test]
    fn close_null_is_noop() {
        unsafe {
            nodedb_close(std::ptr::null_mut());
        }
    }

    #[test]
    fn vector_insert_and_search() {
        let path = CString::new(":memory:").unwrap();
        unsafe {
            let handle = nodedb_open(path.as_ptr(), 1);
            assert!(!handle.is_null());

            let coll = CString::new("vecs").unwrap();
            let id = CString::new("v1").unwrap();
            let emb = [1.0f32, 0.0, 0.0];

            let rc = nodedb_vector_insert(handle, coll.as_ptr(), id.as_ptr(), emb.as_ptr(), 3);
            assert_eq!(rc, NODEDB_OK);

            let query = [1.0f32, 0.0, 0.0];
            let mut out: *mut c_char = std::ptr::null_mut();
            let rc = nodedb_vector_search(handle, coll.as_ptr(), query.as_ptr(), 3, 5, &mut out);
            assert_eq!(rc, NODEDB_OK);
            assert!(!out.is_null());

            let json = CStr::from_ptr(out).to_str().unwrap();
            assert!(json.contains("v1"));
            nodedb_free_string(out);

            nodedb_close(handle);
        }
    }

    #[test]
    fn graph_insert_and_traverse() {
        let path = CString::new(":memory:").unwrap();
        unsafe {
            let handle = nodedb_open(path.as_ptr(), 1);

            let from = CString::new("alice").unwrap();
            let to = CString::new("bob").unwrap();
            let label = CString::new("KNOWS").unwrap();

            let rc = nodedb_graph_insert_edge(handle, from.as_ptr(), to.as_ptr(), label.as_ptr());
            assert_eq!(rc, NODEDB_OK);

            let mut out: *mut c_char = std::ptr::null_mut();
            let rc = nodedb_graph_traverse(handle, from.as_ptr(), 2, &mut out);
            assert_eq!(rc, NODEDB_OK);
            assert!(!out.is_null());

            let json = CStr::from_ptr(out).to_str().unwrap();
            assert!(json.contains("alice"));
            assert!(json.contains("bob"));
            nodedb_free_string(out);

            nodedb_close(handle);
        }
    }

    #[test]
    fn document_crud_via_ffi() {
        let path = CString::new(":memory:").unwrap();
        unsafe {
            let handle = nodedb_open(path.as_ptr(), 1);

            let coll = CString::new("notes").unwrap();
            let body =
                CString::new(r#"{"id":"n1","fields":{"title":{"String":"Hello"}}}"#).unwrap();

            let rc =
                nodedb_document_put(handle, coll.as_ptr(), body.as_ptr(), std::ptr::null_mut());
            assert_eq!(rc, NODEDB_OK);

            let id = CString::new("n1").unwrap();
            let mut out: *mut c_char = std::ptr::null_mut();
            let rc = nodedb_document_get(handle, coll.as_ptr(), id.as_ptr(), &mut out);
            assert_eq!(rc, NODEDB_OK);
            assert!(!out.is_null());

            let json = CStr::from_ptr(out).to_str().unwrap();
            assert!(json.contains("n1"));
            nodedb_free_string(out);

            let rc = nodedb_document_delete(handle, coll.as_ptr(), id.as_ptr());
            assert_eq!(rc, NODEDB_OK);

            let rc = nodedb_document_get(handle, coll.as_ptr(), id.as_ptr(), &mut out);
            assert_eq!(rc, NODEDB_ERR_NOT_FOUND);

            nodedb_close(handle);
        }
    }

    #[test]
    fn free_null_string_is_noop() {
        unsafe {
            nodedb_free_string(std::ptr::null_mut());
        }
    }
}
