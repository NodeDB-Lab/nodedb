//! Thin wrapper around Linux `eventfd` (or a pipe on other platforms) for
//! TPC core wake signaling.
//!
//! When the Control Plane pushes a request into the SPSC ring buffer, it
//! writes to the fd to wake the Data Plane core from `libc::poll`.
//! This replaces the 50µs busy-poll sleep with an interrupt-driven wake.

use std::os::unix::io::RawFd;

// ── Linux: eventfd ───────────────────────────────────────────────────────────

/// An eventfd / pipe file descriptor for cross-thread wake signaling.
///
/// `!Send` and `!Sync` — each core owns its own EventFd on the Data Plane side.
/// The Control Plane holds a cloneable `EventFdNotifier` (which is `Send + Sync`).
pub struct EventFd {
    /// Read fd (and write fd for Linux eventfd, where they share one fd).
    fd: RawFd,
    /// Write fd (pipe only; -1 for Linux where the single fd is both R/W).
    #[cfg(not(target_os = "linux"))]
    write_fd: RawFd,
}

#[cfg(target_os = "linux")]
impl EventFd {
    /// Create a new eventfd in semaphore mode (EFD_SEMAPHORE).
    pub fn new() -> crate::Result<Self> {
        // SAFETY: eventfd2 is a standard Linux syscall. EFD_SEMAPHORE makes
        // each read decrement by 1, EFD_NONBLOCK prevents blocking reads.
        let fd = unsafe { libc::eventfd(0, libc::EFD_NONBLOCK | libc::EFD_SEMAPHORE) };
        if fd < 0 {
            return Err(crate::Error::Io(std::io::Error::last_os_error()));
        }
        Ok(Self { fd })
    }

    /// Get the raw fd for use with `libc::poll`.
    pub fn as_raw_fd(&self) -> RawFd {
        self.fd
    }

    /// Drain one pending notification (semaphore mode: returns 1 or 0).
    pub fn drain(&self) -> u64 {
        let mut buf = 0u64;
        // SAFETY: reading 8 bytes from an eventfd is the documented API.
        let ret = unsafe {
            libc::read(
                self.fd,
                &mut buf as *mut u64 as *mut libc::c_void,
                std::mem::size_of::<u64>(),
            )
        };
        if ret == 8 { buf } else { 0 }
    }

    /// Block until a signal arrives, with a timeout.
    ///
    /// Returns `true` if a signal was received, `false` on timeout.
    pub fn poll_wait(&self, timeout_ms: i32) -> bool {
        let mut pfd = libc::pollfd {
            fd: self.fd,
            events: libc::POLLIN,
            revents: 0,
        };
        // SAFETY: standard poll syscall on a valid fd.
        let ret = unsafe { libc::poll(&mut pfd, 1, timeout_ms) };
        ret > 0 && (pfd.revents & libc::POLLIN) != 0
    }

    /// Create a `Send + Sync` notifier handle for the Control Plane.
    pub fn notifier(&self) -> EventFdNotifier {
        EventFdNotifier { fd: self.fd }
    }
}

#[cfg(target_os = "linux")]
impl Drop for EventFd {
    fn drop(&mut self) {
        unsafe { libc::close(self.fd); }
    }
}

// ── non-Linux: pipe fallback ─────────────────────────────────────────────────

#[cfg(not(target_os = "linux"))]
impl EventFd {
    /// Create a new wake fd backed by a non-blocking pipe.
    ///
    /// Each `notify()` writes one byte; each `drain()` reads one byte,
    /// matching Linux's EFD_SEMAPHORE behaviour (each read returns 1).
    pub fn new() -> crate::Result<Self> {
        let mut fds = [0i32; 2];
        // SAFETY: pipe + fcntl are standard POSIX calls.
        let rc = unsafe { libc::pipe(fds.as_mut_ptr()) };
        if rc != 0 {
            return Err(crate::Error::Io(std::io::Error::last_os_error()));
        }
        unsafe {
            for &fd in &fds {
                libc::fcntl(fd, libc::F_SETFL, libc::O_NONBLOCK);
                libc::fcntl(fd, libc::F_SETFD, libc::FD_CLOEXEC);
            }
        }
        Ok(Self { fd: fds[0], write_fd: fds[1] })
    }

    /// Get the raw read fd for use with `libc::poll`.
    pub fn as_raw_fd(&self) -> RawFd {
        self.fd
    }

    /// Drain one pending notification (reads one byte → returns 1, or 0 if none).
    pub fn drain(&self) -> u64 {
        let mut byte: u8 = 0;
        let ret = unsafe {
            libc::read(
                self.fd,
                &mut byte as *mut u8 as *mut libc::c_void,
                1,
            )
        };
        if ret == 1 { 1 } else { 0 }
    }

    /// Block until a signal arrives, with a timeout.
    ///
    /// Returns `true` if a signal was received, `false` on timeout.
    pub fn poll_wait(&self, timeout_ms: i32) -> bool {
        let mut pfd = libc::pollfd {
            fd: self.fd,
            events: libc::POLLIN,
            revents: 0,
        };
        let ret = unsafe { libc::poll(&mut pfd, 1, timeout_ms) };
        ret > 0 && (pfd.revents & libc::POLLIN) != 0
    }

    /// Create a `Send + Sync` notifier handle for the Control Plane.
    pub fn notifier(&self) -> EventFdNotifier {
        EventFdNotifier { fd: self.write_fd }
    }
}

#[cfg(not(target_os = "linux"))]
impl Drop for EventFd {
    fn drop(&mut self) {
        unsafe {
            libc::close(self.fd);
            libc::close(self.write_fd);
        }
    }
}

// ── shared ───────────────────────────────────────────────────────────────────

/// A `Send + Sync` handle for signaling an EventFd from the Control Plane.
///
/// The underlying fd is owned by the `EventFd` on the Data Plane side.
/// The notifier only writes to it — it does not close the fd on drop.
#[derive(Clone, Copy)]
pub struct EventFdNotifier {
    fd: RawFd,
}

// SAFETY: writes to eventfd/pipe are thread-safe and atomic for small sizes.
unsafe impl Send for EventFdNotifier {}
unsafe impl Sync for EventFdNotifier {}

impl EventFdNotifier {
    /// Signal the Data Plane core to wake up.
    pub fn notify(&self) {
        #[cfg(target_os = "linux")]
        {
            let val: u64 = 1;
            // SAFETY: writing 8 bytes to an eventfd is the documented API.
            unsafe {
                libc::write(
                    self.fd,
                    &val as *const u64 as *const libc::c_void,
                    std::mem::size_of::<u64>(),
                );
            }
        }
        #[cfg(not(target_os = "linux"))]
        {
            let byte: u8 = 1;
            // SAFETY: writing one byte to the pipe write end. EAGAIN means
            // the pipe is full (extremely unlikely) — signal already pending.
            unsafe {
                libc::write(self.fd, &byte as *const u8 as *const libc::c_void, 1);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn create_and_signal() {
        let efd = EventFd::new().unwrap();
        let notifier = efd.notifier();

        // No signals yet.
        assert_eq!(efd.drain(), 0);

        // Signal and drain.
        notifier.notify();
        assert_eq!(efd.drain(), 1);

        // Drained — should be 0 again.
        assert_eq!(efd.drain(), 0);
    }

    #[test]
    fn multiple_signals_accumulate() {
        let efd = EventFd::new().unwrap();
        let notifier = efd.notifier();

        notifier.notify();
        notifier.notify();
        notifier.notify();

        // Semaphore mode: each drain returns 1, decrements by 1.
        assert_eq!(efd.drain(), 1);
        assert_eq!(efd.drain(), 1);
        assert_eq!(efd.drain(), 1);
        assert_eq!(efd.drain(), 0);
    }

    #[test]
    fn poll_wait_timeout() {
        let efd = EventFd::new().unwrap();
        // No signal — should timeout quickly.
        assert!(!efd.poll_wait(1));
    }

    #[test]
    fn poll_wait_signaled() {
        let efd = EventFd::new().unwrap();
        let notifier = efd.notifier();

        notifier.notify();
        assert!(efd.poll_wait(100));
    }

    #[test]
    fn notifier_is_send_sync() {
        fn assert_send_sync<T: Send + Sync>() {}
        assert_send_sync::<EventFdNotifier>();
    }
}
