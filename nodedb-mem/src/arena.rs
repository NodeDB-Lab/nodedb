// SPDX-License-Identifier: Apache-2.0

//! Per-thread jemalloc arena pinning for Thread-per-Core isolation.
//!
//! In a TPC architecture, multiple cores sharing jemalloc's default arenas
//! creates cross-core contention on arena locks — defeating the purpose of
//! shared-nothing isolation.
//!
//! This module pins each Data Plane core to a dedicated jemalloc arena at
//! startup, ensuring all allocations on that core are served from thread-local
//! memory with zero lock contention.
//!
//! The Control Plane (Tokio) uses jemalloc's default arena assignment, which
//! suits its work-stealing thread pool model.
//!
//! Arena pinning needs the `jemalloc` feature on a non-wasm32 target. Without
//! it, pinning returns the requested index unchanged and the current arena
//! reads as zero. NUMA binding is Linux-only and a no-op elsewhere.

#[cfg(all(
    not(target_arch = "wasm32"),
    any(target_os = "linux", feature = "jemalloc")
))]
use crate::error::MemError;
use crate::error::Result;

/// Bind the calling thread's memory allocation policy to its local NUMA node.
///
/// Uses `set_mempolicy(MPOL_BIND)` so all subsequent allocations on this
/// thread are served from local NUMA memory. On a single-node host there is
/// only node 0 — `MPOL_BIND` to node 0 is a no-op from the kernel's
/// perspective and returns `Ok(())` normally.
///
/// Non-Linux and wasm32 targets always return `Ok(())`.
pub fn bind_thread_to_local_numa() -> Result<()> {
    #[cfg(all(target_os = "linux", not(target_arch = "wasm32")))]
    {
        // Determine which NUMA node the calling CPU belongs to.
        let numa_node = local_numa_node()?;
        tracing::debug!(numa_node, "binding Data Plane thread to NUMA node");

        // Build a nodemask with bit `numa_node` set. The nodemask is an array
        // of `unsigned long`; one word covers nodes 0-63 which is sufficient
        // for all practical hardware.
        let mut nodemask: libc::c_ulong =
            1 << (numa_node as usize % (8 * std::mem::size_of::<libc::c_ulong>()));
        let maxnode: libc::c_ulong = numa_node as libc::c_ulong + 2;

        // SAFETY: set_mempolicy is a pure kernel policy syscall — it does not
        // dereference `nodemask` past `maxnode` bits and has no UB on any valid
        // (node < 63) input. MPOL_BIND = 2.
        let ret = unsafe {
            libc::syscall(
                libc::SYS_set_mempolicy,
                2i64, // MPOL_BIND
                &mut nodemask as *mut libc::c_ulong,
                maxnode,
            )
        };

        if ret != 0 {
            let errno = unsafe { *libc::__errno_location() };
            // ENOSYS: kernel was built without NUMA support. Treat as single-node.
            // EPERM:  insufficient privilege. Degrade gracefully.
            if errno == libc::ENOSYS || errno == libc::EPERM {
                tracing::debug!(errno, "set_mempolicy not available; using default policy");
                return Ok(());
            }
            return Err(MemError::Jemalloc(format!(
                "set_mempolicy(MPOL_BIND, node={numa_node}) failed: errno={errno}"
            )));
        }
    }
    Ok(())
}

/// Return the NUMA node index for the CPU the calling thread is currently
/// running on. Falls back to node 0 when the kernel interface is unavailable.
#[cfg(all(target_os = "linux", not(target_arch = "wasm32")))]
fn local_numa_node() -> Result<u32> {
    let mut cpu: u32 = 0;
    let mut node: u32 = 0;

    // SAFETY: getcpu writes into the two u32 outputs; the third argument
    // (tcache pointer) is unused when null.
    let ret = unsafe {
        libc::syscall(
            libc::SYS_getcpu,
            &mut cpu as *mut u32,
            &mut node as *mut u32,
            std::ptr::null_mut::<libc::c_void>(),
        )
    };

    if ret != 0 {
        // getcpu is a vDSO call and should never fail; treat failure as
        // single-node (node 0).
        return Ok(0);
    }
    Ok(node)
}

/// Pin the calling thread to a dedicated jemalloc arena.
///
/// Call this once at Data Plane core startup, before any allocations.
/// Each core should get a unique `arena_index` (typically core ID).
///
/// Returns the arena index that was assigned. Without the `jemalloc` feature,
/// or on wasm32, the supplied index is returned unchanged as a no-op.
pub fn pin_thread_arena(arena_index: u32) -> Result<u32> {
    #[cfg(all(feature = "jemalloc", not(target_arch = "wasm32")))]
    {
        let narenas = read_narenas()?;

        let target_arena = if (arena_index as usize) < narenas {
            arena_index
        } else {
            create_arena()?
        };

        set_thread_arena(target_arena)?;
        Ok(target_arena)
    }
    #[cfg(any(not(feature = "jemalloc"), target_arch = "wasm32"))]
    {
        Ok(arena_index)
    }
}

/// Query the current number of jemalloc arenas.
#[cfg(all(feature = "jemalloc", not(target_arch = "wasm32")))]
fn read_narenas() -> Result<usize> {
    tikv_jemalloc_ctl::arenas::narenas::read()
        .map(|v| v as usize)
        .map_err(|e| MemError::Jemalloc(format!("failed to read narenas: {e:?}")))
}

/// Create a new jemalloc arena. Returns the new arena's index.
#[cfg(all(feature = "jemalloc", not(target_arch = "wasm32")))]
fn create_arena() -> Result<u32> {
    // SAFETY: `arenas.create` is a standard jemalloc mallctl that creates a new
    // arena and returns its unsigned index. No pointers are involved.
    let arena_idx: u32 = unsafe { tikv_jemalloc_ctl::raw::read(b"arenas.create\0") }
        .map_err(|e| MemError::Jemalloc(format!("failed to create arena: {e:?}")))?;
    Ok(arena_idx)
}

/// Pin the calling thread to a specific arena.
#[cfg(all(feature = "jemalloc", not(target_arch = "wasm32")))]
fn set_thread_arena(arena: u32) -> Result<()> {
    // SAFETY: `thread.arena` is a standard jemalloc mallctl that pins the
    // calling thread to the specified arena index.
    unsafe { tikv_jemalloc_ctl::raw::write(b"thread.arena\0", arena) }
        .map_err(|e| MemError::Jemalloc(format!("failed to set thread arena: {e:?}")))?;
    Ok(())
}

/// Read the arena index the calling thread is currently pinned to.
///
/// Without the `jemalloc` feature, or on wasm32, returns 0 as a neutral value.
pub fn current_thread_arena() -> Result<u32> {
    #[cfg(all(feature = "jemalloc", not(target_arch = "wasm32")))]
    {
        // SAFETY: `thread.arena` read returns the current arena index for this thread.
        let arena: u32 = unsafe { tikv_jemalloc_ctl::raw::read(b"thread.arena\0") }
            .map_err(|e| MemError::Jemalloc(format!("failed to read thread arena: {e:?}")))?;
        Ok(arena)
    }
    #[cfg(any(not(feature = "jemalloc"), target_arch = "wasm32"))]
    {
        Ok(0)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn pin_and_read_arena() {
        let assigned = pin_thread_arena(0).unwrap();
        assert_eq!(assigned, 0);

        let current = current_thread_arena().unwrap();
        assert_eq!(current, 0);
    }

    #[cfg(all(feature = "jemalloc", not(target_arch = "wasm32")))]
    #[test]
    fn create_and_pin_new_arena() {
        let narenas = read_narenas().unwrap();
        let assigned = pin_thread_arena(narenas as u32 + 100).unwrap();

        let current = current_thread_arena().unwrap();
        assert_eq!(current, assigned);
    }
}
