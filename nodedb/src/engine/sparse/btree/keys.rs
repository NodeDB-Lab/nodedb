// SPDX-License-Identifier: BUSL-1.1

//! Key encoding for the sparse engine's non-versioned tables.
//!
//! Owns the `"{database_id}:{tenant_id}:{collection}:…"` layout shared by the
//! document and secondary-index tables: the prefix builders used for range
//! scans and renames, and the thread-local composite-key builders used on the
//! per-row hot path.

/// The `"{database_id}:{tenant_id}:{collection}:"` prefix shared by every
/// document / secondary-index key in the current (non-versioned) tables.
/// Centralised so encode, prefix-scan, range-bound, and rename sites can't
/// drift apart. The trailing `:` makes it a clean lower bound for a
/// prefix scan; the matching upper bound appends `\u{ffff}`.
pub(crate) fn coll_prefix(database_id: u64, tenant_id: u64, collection: &str) -> String {
    format!("{database_id}:{tenant_id}:{collection}:")
}

/// Lower bound for a `(database_id, tenant_id)` whole-tenant prefix scan.
pub(in crate::engine::sparse) fn tenant_prefix(database_id: u64, tenant_id: u64) -> String {
    format!("{database_id}:{tenant_id}:")
}

std::thread_local! {
    static KEY_BUF: std::cell::RefCell<String> = std::cell::RefCell::new(String::with_capacity(256));
}

/// Build a database/tenant-scoped composite key `"{db}:{tenant}:{a}:{b}"`
/// in the thread-local buffer. `b` is written via `Display`, so a
/// [`nodedb_types::StorageKey`] lands as its 8 hex characters with no
/// intermediate `String`.
pub(super) fn with_tenant_key<R>(
    database_id: u64,
    tenant_id: u64,
    a: &str,
    b: impl std::fmt::Display,
    f: impl FnOnce(&str) -> R,
) -> R {
    KEY_BUF.with(|buf| {
        let mut buf = buf.borrow_mut();
        buf.clear();
        use std::fmt::Write;
        let _ = write!(buf, "{database_id}");
        buf.push(':');
        let _ = write!(buf, "{tenant_id}");
        buf.push(':');
        buf.push_str(a);
        buf.push(':');
        let _ = write!(buf, "{b}");
        f(&buf)
    })
}

/// Build a database/tenant-scoped index key `"{db}:{tenant}:{a}:{b}:{c}:{d}"`.
pub(in crate::engine::sparse) fn with_tenant_key4<R>(
    database_id: u64,
    tenant_id: u64,
    a: &str,
    b: &str,
    c: &str,
    d: &str,
    f: impl FnOnce(&str) -> R,
) -> R {
    KEY_BUF.with(|buf| {
        let mut buf = buf.borrow_mut();
        buf.clear();
        use std::fmt::Write;
        let _ = write!(buf, "{database_id}");
        buf.push(':');
        let _ = write!(buf, "{tenant_id}");
        buf.push(':');
        buf.push_str(a);
        buf.push(':');
        buf.push_str(b);
        buf.push(':');
        buf.push_str(c);
        buf.push(':');
        buf.push_str(d);
        f(&buf)
    })
}
