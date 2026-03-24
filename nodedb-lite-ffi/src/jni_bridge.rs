//! JNI bridge — Kotlin/Android native method implementations.
//!
//! Uses jni 0.21 API (stable, widely used in Android Rust projects).

use jni::JNIEnv;
use jni::objects::JFloatArray;
use jni::objects::{JClass, JObject, JString};
use jni::sys::{jint, jlong, jstring};

use super::{NODEDB_ERR_FAILED, NODEDB_OK, NodeDbHandle};

fn get_handle(ptr: jlong) -> Option<&'static NodeDbHandle> {
    if ptr == 0 {
        return None;
    }
    Some(unsafe { &*(ptr as *const NodeDbHandle) })
}

#[unsafe(no_mangle)]
pub extern "system" fn Java_com_nodedb_lite_NodeDbLite_00024Companion_nativeOpen(
    mut env: JNIEnv,
    _class: JClass,
    path: JString,
    peer_id: jlong,
) -> jlong {
    let path: String = match env.get_string(&path) {
        Ok(s) => s.into(),
        Err(_) => return 0,
    };
    let path_c = match std::ffi::CString::new(path) {
        Ok(c) => c,
        Err(_) => return 0,
    };
    let handle = unsafe { super::nodedb_open(path_c.as_ptr(), peer_id as u64) };
    handle as jlong
}

#[unsafe(no_mangle)]
pub extern "system" fn Java_com_nodedb_lite_NodeDbLite_nativeClose(
    _env: JNIEnv,
    _obj: JObject,
    handle: jlong,
) {
    if handle != 0 {
        unsafe { super::nodedb_close(handle as *mut NodeDbHandle) };
    }
}

#[unsafe(no_mangle)]
pub extern "system" fn Java_com_nodedb_lite_NodeDbLite_nativeFlush(
    _env: JNIEnv,
    _obj: JObject,
    handle: jlong,
) -> jint {
    let Some(h) = get_handle(handle) else {
        return NODEDB_ERR_FAILED;
    };
    match h.rt.block_on(h.db.flush()) {
        Ok(()) => NODEDB_OK,
        Err(_) => NODEDB_ERR_FAILED,
    }
}

#[unsafe(no_mangle)]
pub extern "system" fn Java_com_nodedb_lite_NodeDbLite_nativeVectorInsert(
    mut env: JNIEnv,
    _obj: JObject,
    handle: jlong,
    collection: JString,
    id: JString,
    embedding: JFloatArray,
    _dim: jint,
) -> jint {
    let Some(h) = get_handle(handle) else {
        return NODEDB_ERR_FAILED;
    };
    let collection: String = match env.get_string(&collection) {
        Ok(s) => s.into(),
        Err(_) => return NODEDB_ERR_FAILED,
    };
    let id: String = match env.get_string(&id) {
        Ok(s) => s.into(),
        Err(_) => return NODEDB_ERR_FAILED,
    };

    let len = match env.get_array_length(&embedding) {
        Ok(l) => l as usize,
        Err(_) => return NODEDB_ERR_FAILED,
    };
    let mut buf = vec![0.0f32; len];
    if env.get_float_array_region(&embedding, 0, &mut buf).is_err() {
        return NODEDB_ERR_FAILED;
    }

    use nodedb_client::NodeDb;
    match h
        .rt
        .block_on(h.db.vector_insert(&collection, &id, &buf, None))
    {
        Ok(()) => NODEDB_OK,
        Err(_) => NODEDB_ERR_FAILED,
    }
}

#[unsafe(no_mangle)]
pub extern "system" fn Java_com_nodedb_lite_NodeDbLite_nativeVectorSearch(
    mut env: JNIEnv,
    _obj: JObject,
    handle: jlong,
    collection: JString,
    query: JFloatArray,
    _dim: jint,
    k: jint,
) -> jstring {
    let h = match get_handle(handle) {
        Some(h) => h,
        None => return std::ptr::null_mut(),
    };
    let collection: String = match env.get_string(&collection) {
        Ok(s) => s.into(),
        Err(_) => return std::ptr::null_mut(),
    };
    let len = match env.get_array_length(&query) {
        Ok(l) => l as usize,
        Err(_) => return std::ptr::null_mut(),
    };
    let mut buf = vec![0.0f32; len];
    if env.get_float_array_region(&query, 0, &mut buf).is_err() {
        return std::ptr::null_mut();
    }

    use nodedb_client::NodeDb;
    let results = match h
        .rt
        .block_on(h.db.vector_search(&collection, &buf, k as usize, None))
    {
        Ok(r) => r,
        Err(_) => return std::ptr::null_mut(),
    };

    let json: Vec<serde_json::Value> = results
        .iter()
        .map(|r| serde_json::json!({"id": r.id, "distance": r.distance}))
        .collect();
    let json_str = serde_json::to_string(&json).unwrap_or_else(|_| "[]".into());

    match env.new_string(&json_str) {
        Ok(s) => s.into_raw(),
        Err(_) => std::ptr::null_mut(),
    }
}

#[unsafe(no_mangle)]
pub extern "system" fn Java_com_nodedb_lite_NodeDbLite_nativeVectorDelete(
    mut env: JNIEnv,
    _obj: JObject,
    handle: jlong,
    collection: JString,
    id: JString,
) -> jint {
    let Some(h) = get_handle(handle) else {
        return NODEDB_ERR_FAILED;
    };
    let collection: String = match env.get_string(&collection) {
        Ok(s) => s.into(),
        Err(_) => return NODEDB_ERR_FAILED,
    };
    let id: String = match env.get_string(&id) {
        Ok(s) => s.into(),
        Err(_) => return NODEDB_ERR_FAILED,
    };
    use nodedb_client::NodeDb;
    match h.rt.block_on(h.db.vector_delete(&collection, &id)) {
        Ok(()) => NODEDB_OK,
        Err(_) => NODEDB_ERR_FAILED,
    }
}

#[unsafe(no_mangle)]
pub extern "system" fn Java_com_nodedb_lite_NodeDbLite_nativeGraphInsertEdge(
    mut env: JNIEnv,
    _obj: JObject,
    handle: jlong,
    from: JString,
    to: JString,
    edge_type: JString,
) -> jint {
    let Some(h) = get_handle(handle) else {
        return NODEDB_ERR_FAILED;
    };
    let from: String = match env.get_string(&from) {
        Ok(s) => s.into(),
        Err(_) => return NODEDB_ERR_FAILED,
    };
    let to: String = match env.get_string(&to) {
        Ok(s) => s.into(),
        Err(_) => return NODEDB_ERR_FAILED,
    };
    let edge_type: String = match env.get_string(&edge_type) {
        Ok(s) => s.into(),
        Err(_) => return NODEDB_ERR_FAILED,
    };

    use nodedb_client::NodeDb;
    let from_id = nodedb_types::id::NodeId::new(&from);
    let to_id = nodedb_types::id::NodeId::new(&to);
    match h
        .rt
        .block_on(h.db.graph_insert_edge(&from_id, &to_id, &edge_type, None))
    {
        Ok(_) => NODEDB_OK,
        Err(_) => NODEDB_ERR_FAILED,
    }
}

#[unsafe(no_mangle)]
pub extern "system" fn Java_com_nodedb_lite_NodeDbLite_nativeGraphTraverse(
    mut env: JNIEnv,
    _obj: JObject,
    handle: jlong,
    start: JString,
    depth: jint,
) -> jstring {
    let h = match get_handle(handle) {
        Some(h) => h,
        None => return std::ptr::null_mut(),
    };
    let start: String = match env.get_string(&start) {
        Ok(s) => s.into(),
        Err(_) => return std::ptr::null_mut(),
    };

    use nodedb_client::NodeDb;
    let start_id = nodedb_types::id::NodeId::new(&start);
    let subgraph = match h
        .rt
        .block_on(h.db.graph_traverse(&start_id, depth as u8, None))
    {
        Ok(sg) => sg,
        Err(_) => return std::ptr::null_mut(),
    };

    let json = serde_json::json!({
        "nodes": subgraph.nodes.iter().map(|n| serde_json::json!({"id": n.id.as_str(), "depth": n.depth})).collect::<Vec<_>>(),
        "edges": subgraph.edges.iter().map(|e| serde_json::json!({"from": e.from.as_str(), "to": e.to.as_str(), "label": e.label})).collect::<Vec<_>>(),
    });
    let json_str = serde_json::to_string(&json).unwrap_or_else(|_| "{}".into());
    match env.new_string(&json_str) {
        Ok(s) => s.into_raw(),
        Err(_) => std::ptr::null_mut(),
    }
}

#[unsafe(no_mangle)]
pub extern "system" fn Java_com_nodedb_lite_NodeDbLite_nativeDocumentGet(
    mut env: JNIEnv,
    _obj: JObject,
    handle: jlong,
    collection: JString,
    id: JString,
) -> jstring {
    let h = match get_handle(handle) {
        Some(h) => h,
        None => return std::ptr::null_mut(),
    };
    let collection: String = match env.get_string(&collection) {
        Ok(s) => s.into(),
        Err(_) => return std::ptr::null_mut(),
    };
    let id: String = match env.get_string(&id) {
        Ok(s) => s.into(),
        Err(_) => return std::ptr::null_mut(),
    };

    use nodedb_client::NodeDb;
    match h.rt.block_on(h.db.document_get(&collection, &id)) {
        Ok(Some(doc)) => {
            let json_str = serde_json::to_string(&doc).unwrap_or_else(|_| "{}".into());
            match env.new_string(&json_str) {
                Ok(s) => s.into_raw(),
                Err(_) => std::ptr::null_mut(),
            }
        }
        _ => std::ptr::null_mut(),
    }
}

#[unsafe(no_mangle)]
pub extern "system" fn Java_com_nodedb_lite_NodeDbLite_nativeDocumentPut(
    mut env: JNIEnv,
    _obj: JObject,
    handle: jlong,
    collection: JString,
    json_body: JString,
) -> jint {
    let Some(h) = get_handle(handle) else {
        return NODEDB_ERR_FAILED;
    };
    let collection: String = match env.get_string(&collection) {
        Ok(s) => s.into(),
        Err(_) => return NODEDB_ERR_FAILED,
    };
    let json_str: String = match env.get_string(&json_body) {
        Ok(s) => s.into(),
        Err(_) => return NODEDB_ERR_FAILED,
    };

    let mut doc: nodedb_types::Document = match serde_json::from_str(&json_str) {
        Ok(d) => d,
        Err(_) => return NODEDB_ERR_FAILED,
    };

    if doc.id.is_empty() {
        doc.id = nodedb_types::id_gen::uuid_v7();
    }

    use nodedb_client::NodeDb;
    match h.rt.block_on(h.db.document_put(&collection, doc)) {
        Ok(()) => NODEDB_OK,
        Err(_) => NODEDB_ERR_FAILED,
    }
}

#[unsafe(no_mangle)]
pub extern "system" fn Java_com_nodedb_lite_NodeDbLite_nativeDocumentDelete(
    mut env: JNIEnv,
    _obj: JObject,
    handle: jlong,
    collection: JString,
    id: JString,
) -> jint {
    let Some(h) = get_handle(handle) else {
        return NODEDB_ERR_FAILED;
    };
    let collection: String = match env.get_string(&collection) {
        Ok(s) => s.into(),
        Err(_) => return NODEDB_ERR_FAILED,
    };
    let id: String = match env.get_string(&id) {
        Ok(s) => s.into(),
        Err(_) => return NODEDB_ERR_FAILED,
    };
    use nodedb_client::NodeDb;
    match h.rt.block_on(h.db.document_delete(&collection, &id)) {
        Ok(()) => NODEDB_OK,
        Err(_) => NODEDB_ERR_FAILED,
    }
}

/// Generate a UUIDv7 (time-sortable, recommended for primary keys).
#[unsafe(no_mangle)]
pub extern "system" fn Java_com_nodedb_lite_NodeDbLite_00024Companion_nativeGenerateId(
    env: JNIEnv,
    _class: JClass,
) -> jstring {
    let id = nodedb_types::id_gen::uuid_v7();
    match env.new_string(&id) {
        Ok(s) => s.into_raw(),
        Err(_) => std::ptr::null_mut(),
    }
}

/// Generate an ID of the specified type.
///
/// Supported types: "uuidv7", "uuidv4", "ulid", "cuid2", "nanoid".
#[unsafe(no_mangle)]
pub extern "system" fn Java_com_nodedb_lite_NodeDbLite_00024Companion_nativeGenerateIdTyped(
    mut env: JNIEnv,
    _class: JClass,
    id_type: JString,
) -> jstring {
    let id_type_str: String = match env.get_string(&id_type) {
        Ok(s) => s.into(),
        Err(_) => return std::ptr::null_mut(),
    };
    let id = match nodedb_types::id_gen::generate_by_type(&id_type_str) {
        Some(id) => id,
        None => return std::ptr::null_mut(),
    };
    match env.new_string(&id) {
        Ok(s) => s.into_raw(),
        Err(_) => std::ptr::null_mut(),
    }
}
