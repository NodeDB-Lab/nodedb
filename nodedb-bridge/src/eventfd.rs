//! Cross-runtime wake signaling via Linux eventfd (or pipe on other platforms).
//!
//! eventfd is the only safe primitive for waking across Tokio and Glommio/monoio:
//!
//! - Both runtimes can poll a file descriptor.
//! - No `Send` requirement on the waker itself — just read/write an fd.
//! - Coalescing: multiple writes produce a single readable event (Linux only;
//!   the pipe fallback coalesces up to the read buffer size).
//!
//! ## Usage
//!
//! Two EventFd instances per bridge channel:
//!
//! - `producer_wake`: Consumer writes → Producer reads (queue was full, now has space)
//! - `consumer_wake`: Producer writes → Consumer reads (queue was empty, now has data)
//!
//! The runtime-specific integration (registering the fd with epoll/io_uring) is
//! done by the caller. This module only provides raw fd-based signaling.

use std::io;
use std::os::unix::io::{AsRawFd, FromRawFd, OwnedFd, RawFd};

// ── Linux implementation (eventfd) ──────────────────────────────────────────

/// A cross-runtime wake signal backed by a Linux eventfd.
///
/// Write to signal, read to consume. Multiple signals coalesce into one.
/// The fd can be registered with any event loop (epoll, io_uring, kqueue fallback).
///
/// On non-Linux platforms this is backed by a non-blocking pipe instead.
pub struct EventFd {
    #[cfg(target_os = "linux")]
    fd: OwnedFd,

    /// Pipe read end (non-Linux only).
    #[cfg(not(target_os = "linux"))]
    read_fd: OwnedFd,
    /// Pipe write end (non-Linux only).
    #[cfg(not(target_os = "linux"))]
    write_fd: OwnedFd,
}

#[cfg(target_os = "linux")]
impl EventFd {
    /// Create a new eventfd in semaphore mode.
    ///
    /// `EFD_NONBLOCK` ensures reads/writes never block the calling thread.
    /// `EFD_CLOEXEC` prevents fd leaks across fork/exec.
    pub fn new() -> io::Result<Self> {
        // SAFETY: eventfd2 is a standard Linux syscall. Flags are valid.
        let fd = unsafe { libc::eventfd(0, libc::EFD_NONBLOCK | libc::EFD_CLOEXEC) };
        if fd < 0 {
            return Err(io::Error::last_os_error());
        }
        // SAFETY: fd is a valid file descriptor returned by eventfd().
        let fd = unsafe { OwnedFd::from_raw_fd(fd) };
        Ok(Self { fd })
    }

    /// Signal the other side to wake up.
    ///
    /// Writes 1 to the eventfd counter. Multiple writes accumulate but
    /// a single read clears all pending signals.
    pub fn notify(&self) -> io::Result<()> {
        let val: u64 = 1;
        // SAFETY: writing 8 bytes to a valid eventfd.
        let ret = unsafe {
            libc::write(
                self.fd.as_raw_fd(),
                &val as *const u64 as *const libc::c_void,
                8,
            )
        };
        if ret < 0 {
            Err(io::Error::last_os_error())
        } else {
            Ok(())
        }
    }

    /// Consume the pending signal count, returning the accumulated value.
    ///
    /// Returns `Ok(0)` if no signal was pending (EAGAIN on non-blocking fd).
    /// Returns `Ok(n)` where n is the accumulated signal count.
    pub fn try_read(&self) -> io::Result<u64> {
        let mut val: u64 = 0;
        // SAFETY: reading 8 bytes from a valid eventfd.
        let ret = unsafe {
            libc::read(
                self.fd.as_raw_fd(),
                &mut val as *mut u64 as *mut libc::c_void,
                8,
            )
        };
        if ret < 0 {
            let err = io::Error::last_os_error();
            if err.kind() == io::ErrorKind::WouldBlock {
                Ok(0)
            } else {
                Err(err)
            }
        } else {
            Ok(val)
        }
    }

    /// Get the raw file descriptor for registration with an event loop.
    pub fn as_fd(&self) -> RawFd {
        self.fd.as_raw_fd()
    }
}

// ── non-Linux implementation (pipe fallback) ─────────────────────────────────

#[cfg(not(target_os = "linux"))]
impl EventFd {
    /// Create a new wake fd backed by a non-blocking pipe.
    pub fn new() -> io::Result<Self> {
        let mut fds = [0i32; 2];
        // SAFETY: pipe2 / pipe + fcntl are standard POSIX calls.
        unsafe {
            if libc::pipe(fds.as_mut_ptr()) != 0 {
                return Err(io::Error::last_os_error());
            }
            for &fd in &fds {
                libc::fcntl(fd, libc::F_SETFL, libc::O_NONBLOCK);
                libc::fcntl(fd, libc::F_SETFD, libc::FD_CLOEXEC);
            }
        }
        Ok(Self {
            read_fd: unsafe { OwnedFd::from_raw_fd(fds[0]) },
            write_fd: unsafe { OwnedFd::from_raw_fd(fds[1]) },
        })
    }

    /// Signal the other side.
    ///
    /// Writes one byte to the pipe. If the pipe buffer is full (unlikely —
    /// 64 KiB default on macOS) a signal is already pending; the write is
    /// silently skipped.
    pub fn notify(&self) -> io::Result<()> {
        let byte: u8 = 1;
        let ret = unsafe {
            libc::write(
                self.write_fd.as_raw_fd(),
                &byte as *const u8 as *const libc::c_void,
                1,
            )
        };
        if ret < 0 {
            let err = io::Error::last_os_error();
            // Pipe full → a signal is already pending; treat as success.
            if err.kind() == io::ErrorKind::WouldBlock {
                return Ok(());
            }
            return Err(err);
        }
        Ok(())
    }

    /// Drain pending signals from the pipe, returning how many were pending.
    ///
    /// Returns `Ok(0)` if no signal was pending.
    pub fn try_read(&self) -> io::Result<u64> {
        // Read up to 64 bytes at once to coalesce rapid fire signals.
        let mut buf = [0u8; 64];
        let ret = unsafe {
            libc::read(
                self.read_fd.as_raw_fd(),
                buf.as_mut_ptr() as *mut libc::c_void,
                buf.len(),
            )
        };
        if ret < 0 {
            let err = io::Error::last_os_error();
            if err.kind() == io::ErrorKind::WouldBlock {
                return Ok(0);
            }
            return Err(err);
        }
        Ok(ret as u64)
    }

    /// Get the raw file descriptor (read end) for registration with an event loop.
    pub fn as_fd(&self) -> RawFd {
        self.read_fd.as_raw_fd()
    }
}

// ── shared impls ─────────────────────────────────────────────────────────────

// SAFETY: The underlying fd (or pipe) is a kernel object accessible from any thread.
unsafe impl Send for EventFd {}
unsafe impl Sync for EventFd {}

/// A pair of eventfds for bidirectional wake signaling across the bridge.
///
/// ```text
/// Producer (Tokio)          Consumer (TPC)
///    │                          │
///    │── notify(consumer_wake) ──→│  "queue has data"
///    │                          │
///    │←── notify(producer_wake) ──│  "queue has space"
/// ```
pub struct WakePair {
    /// Producer reads this to know the consumer freed space.
    pub producer_wake: EventFd,
    /// Consumer reads this to know the producer enqueued data.
    pub consumer_wake: EventFd,
}

impl WakePair {
    /// Create a new wake pair.
    pub fn new() -> io::Result<Self> {
        Ok(Self {
            producer_wake: EventFd::new()?,
            consumer_wake: EventFd::new()?,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn notify_and_read() {
        let efd = EventFd::new().unwrap();

        // No pending signal.
        assert_eq!(efd.try_read().unwrap(), 0);

        // Signal once.
        efd.notify().unwrap();
        assert!(efd.try_read().unwrap() >= 1);

        // Consumed — nothing pending.
        assert_eq!(efd.try_read().unwrap(), 0);
    }

    #[test]
    fn multiple_notifies_accumulate() {
        let efd = EventFd::new().unwrap();

        efd.notify().unwrap();
        efd.notify().unwrap();
        efd.notify().unwrap();

        // At least one signal consumed in a single read.
        assert!(efd.try_read().unwrap() >= 1);
        assert_eq!(efd.try_read().unwrap(), 0);
    }

    #[test]
    fn wake_pair_bidirectional() {
        let pair = WakePair::new().unwrap();

        // Producer signals consumer.
        pair.consumer_wake.notify().unwrap();
        assert!(pair.consumer_wake.try_read().unwrap() >= 1);

        // Consumer signals producer.
        pair.producer_wake.notify().unwrap();
        assert!(pair.producer_wake.try_read().unwrap() >= 1);
    }

    #[test]
    fn fd_is_valid() {
        let efd = EventFd::new().unwrap();
        assert!(efd.as_fd() >= 0);
    }
}
