//! Zero-copy-capable array backing for CSR dense arrays.
//!
//! `DenseArray<T>` holds either an owned `Vec<T>` (from compaction or
//! mutation) or a zero-copy reference into a shared rkyv buffer (from
//! checkpoint load). This eliminates deserialization for CSR's largest
//! data structures (offset/target/label arrays — millions of elements)
//! on cold start.
//!
//! The shared buffer is kept alive via `Arc<rkyv::util::AlignedVec>`.
//! When compaction produces new arrays, they replace the zero-copy
//! references with owned Vecs — no copy-on-write complexity.

use std::sync::Arc;

/// A read-only array backed by either owned memory or a zero-copy
/// reference into a shared rkyv-archived buffer.
///
/// Implements `Deref<Target = [T]>` so it can be used anywhere `&[T]` is expected.
/// Clone always produces an owned copy (deep clone of data).
pub(crate) enum DenseArray<T: Copy> {
    /// Owned heap allocation (from compaction or deserialization).
    Owned(Vec<T>),
    /// Zero-copy slice into an rkyv-archived buffer.
    /// The `Arc` keeps the backing buffer alive for the lifetime of this reference.
    ZeroCopy {
        /// Shared ownership of the backing buffer.
        _backing: Arc<rkyv::util::AlignedVec>,
        /// Pointer to the first element within the backing buffer.
        ptr: *const T,
        /// Number of elements.
        len: usize,
    },
}

// SAFETY: The backing buffer is heap-allocated and immutable once archived.
// The Arc ensures the buffer outlives all DenseArray references.
// T: Copy means no Drop or interior mutability concerns.
unsafe impl<T: Copy + Send> Send for DenseArray<T> {}
unsafe impl<T: Copy + Sync> Sync for DenseArray<T> {}

impl<T: Copy> DenseArray<T> {
    /// Create a zero-copy reference into an rkyv buffer.
    ///
    /// # Safety
    ///
    /// `ptr` must point to `len` contiguous `T` values within the
    /// `backing` buffer. The caller must ensure proper alignment.
    pub unsafe fn zero_copy(
        backing: Arc<rkyv::util::AlignedVec>,
        ptr: *const T,
        len: usize,
    ) -> Self {
        Self::ZeroCopy {
            _backing: backing,
            ptr,
            len,
        }
    }

    /// Number of elements.
    pub fn len(&self) -> usize {
        match self {
            Self::Owned(v) => v.len(),
            Self::ZeroCopy { len, .. } => *len,
        }
    }

    /// Whether the array is empty.
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Convert to an owned Vec, copying data if zero-copy.
    pub fn to_vec(&self) -> Vec<T> {
        match self {
            Self::Owned(v) => v.clone(),
            Self::ZeroCopy { ptr, len, .. } => {
                let slice = unsafe { std::slice::from_raw_parts(*ptr, *len) };
                slice.to_vec()
            }
        }
    }
}

impl<T: Copy> std::ops::Deref for DenseArray<T> {
    type Target = [T];

    fn deref(&self) -> &[T] {
        match self {
            Self::Owned(v) => v,
            Self::ZeroCopy { ptr, len, .. } => unsafe { std::slice::from_raw_parts(*ptr, *len) },
        }
    }
}

impl<T: Copy> Clone for DenseArray<T> {
    /// Clone always produces an owned copy (materializes zero-copy data).
    fn clone(&self) -> Self {
        Self::Owned(self.to_vec())
    }
}

impl<T: Copy + std::fmt::Debug> std::fmt::Debug for DenseArray<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Owned(v) => write!(f, "DenseArray::Owned({} elements)", v.len()),
            Self::ZeroCopy { len, .. } => write!(f, "DenseArray::ZeroCopy({len} elements)"),
        }
    }
}

impl<T: Copy> Default for DenseArray<T> {
    fn default() -> Self {
        Self::Owned(Vec::new())
    }
}

impl<T: Copy> From<Vec<T>> for DenseArray<T> {
    fn from(v: Vec<T>) -> Self {
        Self::Owned(v)
    }
}
