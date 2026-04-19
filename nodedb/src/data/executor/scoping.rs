//! Lexical tenant scoping for persistent storage keys.

/// Construct a tenant-scoped collection key: `"{tid}:{collection}"`.
#[inline]
pub fn scoped_collection(tid: u32, collection: &str) -> String {
    format!("{tid}:{collection}")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn scoped_collection_format() {
        assert_eq!(scoped_collection(42, "orders"), "42:orders");
    }
}
