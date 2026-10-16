// SPDX-License-Identifier: BUSL-1.1

//! Cross-thread core wake signaling for the Data Plane.
//!
//! When the Control Plane pushes a request into the SPSC ring buffer, it
//! signals the Data Plane core to wake from `libc::poll`. This replaces the
//! 50µs busy-poll sleep with an interrupt-driven wake.
//!
//! ## Platform backends
//! - **Linux**: `eventfd` in `EFD_SEMAPHORE` mode — the production path.
//! - **Other Unix (macOS, BSD)**: a self-pipe — slower per-wake than eventfd
//!   but functionally identical, so the server boots and serves on developer
//!   machines. This is a dev/correctness path, not a performance path.
//! - **Non-Unix (wasm, Windows)**: unsupported; `EventFd::new()` errors.

#[cfg(unix)]
use std::os::fd::{AsRawFd as _, FromRawFd as _, OwnedFd};
#[cfg(unix)]
use std::os::unix::io::RawFd;
#[cfg(unix)]
use std::sync::Arc;

#[cfg(not(unix))]
type RawFd = i32;

/// A file descriptor for cross-thread wake signaling.
///
/// `!Send` and `!Sync` — each core owns its own EventFd on the Data Plane side.
/// The Control Plane holds a cloneable `EventFdNotifier` (which is `Send + Sync`).
pub struct EventFd {
    // Shared with every `EventFdNotifier`: the descriptor closes when the core
    // and the last notifier are gone, never while a notifier can still write.
    #[cfg(target_os = "linux")]
    fd: Arc<OwnedFd>,
    // Self-pipe: read end lives with the core, the notifier writes to `write_fd`.
    #[cfg(all(unix, not(target_os = "linux")))]
    read_fd: OwnedFd,
    #[cfg(all(unix, not(target_os = "linux")))]
    write_fd: Arc<OwnedFd>,
}

impl EventFd {
    /// Create a new core-wake handle.
    pub fn new() -> crate::Result<Self> {
        #[cfg(target_os = "linux")]
        {
            // SAFETY: eventfd2 is a standard Linux syscall. EFD_SEMAPHORE makes
            // each read decrement by 1, EFD_NONBLOCK prevents blocking reads.
            let fd = unsafe { libc::eventfd(0, libc::EFD_NONBLOCK | libc::EFD_SEMAPHORE) };
            if fd < 0 {
                return Err(crate::Error::Io(std::io::Error::last_os_error()));
            }
            // SAFETY: `fd` was just returned by eventfd2 and nothing else owns it.
            Ok(Self {
                fd: Arc::new(unsafe { OwnedFd::from_raw_fd(fd) }),
            })
        }
        #[cfg(all(unix, not(target_os = "linux")))]
        {
            let mut fds = [0 as libc::c_int; 2];
            // SAFETY: `fds` points to two valid c_int slots for pipe() to fill.
            if unsafe { libc::pipe(fds.as_mut_ptr()) } < 0 {
                return Err(crate::Error::Io(std::io::Error::last_os_error()));
            }
            // SAFETY: both fds were just returned by pipe() and nothing else owns
            // them. The owners close both if the flag setup below fails.
            let (read_fd, write_fd) =
                unsafe { (OwnedFd::from_raw_fd(fds[0]), OwnedFd::from_raw_fd(fds[1])) };
            // Both ends non-blocking + close-on-exec: drain() must not block on
            // an empty pipe, and notify() must not block on a full one.
            set_pipe_flags(read_fd.as_raw_fd())
                .and_then(|()| set_pipe_flags(write_fd.as_raw_fd()))
                .map_err(crate::Error::Io)?;
            Ok(Self {
                read_fd,
                write_fd: Arc::new(write_fd),
            })
        }
        #[cfg(not(unix))]
        {
            Err(crate::Error::Io(std::io::Error::new(
                std::io::ErrorKind::Unsupported,
                "data-plane core wake is only available on Unix platforms",
            )))
        }
    }

    /// Get the raw fd for use with `libc::poll`.
    pub fn as_raw_fd(&self) -> RawFd {
        #[cfg(target_os = "linux")]
        {
            self.fd.as_raw_fd()
        }
        #[cfg(all(unix, not(target_os = "linux")))]
        {
            self.read_fd.as_raw_fd()
        }
        #[cfg(not(unix))]
        {
            -1
        }
    }

    /// Drain all pending notifications (non-blocking).
    ///
    /// Returns the number of signals accumulated since the last drain.
    /// Returns 0 if no signals are pending.
    pub fn drain(&self) -> u64 {
        #[cfg(target_os = "linux")]
        {
            let mut buf = 0u64;
            // SAFETY: reading 8 bytes from an eventfd is the documented API.
            let ret = unsafe {
                libc::read(
                    self.fd.as_raw_fd(),
                    &mut buf as *mut u64 as *mut libc::c_void,
                    std::mem::size_of::<u64>(),
                )
            };
            if ret == 8 { buf } else { 0 }
        }
        #[cfg(all(unix, not(target_os = "linux")))]
        {
            // Read every queued wake byte until the pipe drains (EAGAIN). The
            // caller's `while drain() > 0 {}` loop relies on a 0 once empty.
            let mut total: u64 = 0;
            let mut buf = [0u8; 256];
            loop {
                // SAFETY: reading into a valid local buffer from a non-blocking fd.
                let ret = unsafe {
                    libc::read(
                        self.read_fd.as_raw_fd(),
                        buf.as_mut_ptr() as *mut libc::c_void,
                        buf.len(),
                    )
                };
                if ret > 0 {
                    total = total.saturating_add(ret as u64);
                    // A short read means the pipe is now empty.
                    if (ret as usize) < buf.len() {
                        return total;
                    }
                    continue;
                }
                // ret == 0 (EOF) or ret < 0 (EAGAIN / error): nothing more to read.
                return total;
            }
        }
        #[cfg(not(unix))]
        {
            0
        }
    }

    /// Block until a signal arrives, with a timeout.
    ///
    /// Returns `true` if a signal was received, `false` on timeout.
    pub fn poll_wait(&self, timeout_ms: i32) -> bool {
        #[cfg(unix)]
        {
            let mut pfd = libc::pollfd {
                fd: self.as_raw_fd(),
                events: libc::POLLIN,
                revents: 0,
            };
            // SAFETY: standard poll syscall on a valid fd.
            let ret = unsafe { libc::poll(&mut pfd, 1, timeout_ms) };
            ret > 0 && (pfd.revents & libc::POLLIN) != 0
        }
        #[cfg(not(unix))]
        {
            let _ = timeout_ms;
            false
        }
    }

    /// Create a `Send + Sync` notifier handle for the Control Plane.
    pub fn notifier(&self) -> EventFdNotifier {
        #[cfg(target_os = "linux")]
        {
            EventFdNotifier {
                fd: Arc::clone(&self.fd),
            }
        }
        #[cfg(all(unix, not(target_os = "linux")))]
        {
            EventFdNotifier {
                fd: Arc::clone(&self.write_fd),
            }
        }
        #[cfg(not(unix))]
        {
            EventFdNotifier {}
        }
    }
}

/// Set `O_NONBLOCK` and `FD_CLOEXEC` on a freshly-created pipe fd.
#[cfg(all(unix, not(target_os = "linux")))]
fn set_pipe_flags(fd: RawFd) -> std::io::Result<()> {
    // SAFETY: fcntl is called with a valid fd returned by pipe().
    let status_flags = unsafe { libc::fcntl(fd, libc::F_GETFL) };
    if status_flags < 0 {
        return Err(std::io::Error::last_os_error());
    }
    // SAFETY: F_SETFL updates the fd's status flags.
    if unsafe { libc::fcntl(fd, libc::F_SETFL, status_flags | libc::O_NONBLOCK) } < 0 {
        return Err(std::io::Error::last_os_error());
    }
    // SAFETY: fcntl is called with a valid fd returned by pipe().
    let fd_flags = unsafe { libc::fcntl(fd, libc::F_GETFD) };
    if fd_flags < 0 {
        return Err(std::io::Error::last_os_error());
    }
    // SAFETY: F_SETFD updates the fd's close-on-exec flag.
    if unsafe { libc::fcntl(fd, libc::F_SETFD, fd_flags | libc::FD_CLOEXEC) } < 0 {
        return Err(std::io::Error::last_os_error());
    }
    Ok(())
}

/// A `Send + Sync` handle for signaling an `EventFd` from the Control Plane.
///
/// The notifier shares ownership of the descriptor with the `EventFd`. A core
/// that exits, or panics while it opens its engines, drops its `EventFd`, and
/// the descriptor stays open for as long as any notifier is alive. A notifier
/// that held only the descriptor number would write into whatever file the
/// kernel next handed that number to, such as a database file opened after the
/// core died. The write (eventfd 8-byte / pipe 1-byte) is atomic and
/// thread-safe, so a notifier can be cloned across threads.
#[derive(Clone)]
pub struct EventFdNotifier {
    #[cfg(unix)]
    fd: Arc<OwnedFd>,
}

impl EventFdNotifier {
    /// Signal the Data Plane core to wake up.
    pub fn notify(&self) {
        #[cfg(target_os = "linux")]
        {
            let val: u64 = 1;
            // SAFETY: writing 8 bytes to an eventfd is the documented API.
            // This is atomic and thread-safe per the Linux man page.
            unsafe {
                libc::write(
                    self.fd.as_raw_fd(),
                    &val as *const u64 as *const libc::c_void,
                    std::mem::size_of::<u64>(),
                );
            }
        }
        #[cfg(all(unix, not(target_os = "linux")))]
        {
            let val: u8 = 1;
            // SAFETY: writing 1 byte to the self-pipe write end. A full pipe
            // (EAGAIN) is intentionally ignored — it already means a wake is
            // pending, so the level-triggered poll will still fire.
            unsafe {
                libc::write(
                    self.fd.as_raw_fd(),
                    &val as *const u8 as *const libc::c_void,
                    1,
                );
            }
        }
    }
}

#[cfg(all(test, unix))]
mod tests {
    use super::*;

    #[test]
    fn create_and_signal() {
        let efd = EventFd::new().unwrap();
        let notifier = efd.notifier();

        // No signals yet.
        assert_eq!(efd.drain(), 0);

        // Signal and drain — one notify yields one pending signal on both
        // backends (eventfd semaphore read == 1, self-pipe byte == 1).
        notifier.notify();
        assert_eq!(efd.drain(), 1);

        // Drained — should be 0 again.
        assert_eq!(efd.drain(), 0);
    }

    #[test]
    #[cfg(target_os = "linux")]
    fn multiple_signals_accumulate() {
        let efd = EventFd::new().unwrap();
        let notifier = efd.notifier();

        notifier.notify();
        notifier.notify();
        notifier.notify();

        // EFD_SEMAPHORE mode: each read returns 1, decrements by 1.
        assert_eq!(efd.drain(), 1);
        assert_eq!(efd.drain(), 1);
        assert_eq!(efd.drain(), 1);
        assert_eq!(efd.drain(), 0);
    }

    #[test]
    #[cfg(all(unix, not(target_os = "linux")))]
    fn multiple_signals_coalesce() {
        let efd = EventFd::new().unwrap();
        let notifier = efd.notifier();

        notifier.notify();
        notifier.notify();
        notifier.notify();

        // Self-pipe: a single drain consumes all queued wake bytes, then 0.
        assert_eq!(efd.drain(), 3);
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

    /// A core that exits drops its `EventFd`. The Control Plane keeps notifying
    /// it until it learns the core is gone, so the descriptor must outlive the
    /// core. A closed descriptor number goes to the next `open()`, and the
    /// notifier's 8-byte write then lands in that file.
    #[test]
    #[cfg(target_os = "linux")]
    fn notifier_outlives_the_core_without_reaching_a_reused_descriptor() {
        use std::io::Read as _;
        use std::os::fd::AsRawFd as _;

        let efd = EventFd::new().unwrap();
        let notifier = efd.notifier();
        let notifier_fd = notifier.fd.as_raw_fd();
        drop(efd);

        // The descriptor is still open: the kernel cannot hand its number out.
        // SAFETY: F_GETFD only reads the descriptor's flags.
        assert_ne!(
            unsafe { libc::fcntl(notifier_fd, libc::F_GETFD) },
            -1,
            "the notifier's descriptor was closed with the core"
        );

        let dir = tempfile::tempdir().unwrap();
        let mut files = Vec::new();
        for i in 0..8 {
            let file = std::fs::File::create(dir.path().join(format!("db-{i}.redb"))).unwrap();
            assert_ne!(
                file.as_raw_fd(),
                notifier_fd,
                "a file opened after the core died reused the notifier's descriptor"
            );
            files.push(file);
        }

        // Signals after the core died go to the eventfd, not to any file.
        for _ in 0..4 {
            notifier.notify();
        }
        for i in 0..8 {
            let mut contents = Vec::new();
            std::fs::File::open(dir.path().join(format!("db-{i}.redb")))
                .unwrap()
                .read_to_end(&mut contents)
                .unwrap();
            assert!(contents.is_empty(), "db-{i}.redb received notifier bytes");
        }
    }

    #[test]
    fn notifier_clones_keep_the_descriptor_until_the_last_one_drops() {
        let efd = EventFd::new().unwrap();
        let first = efd.notifier();
        let second = first.clone();
        drop(efd);
        drop(first);
        // `second` still signals without error and the descriptor is valid.
        second.notify();
        #[cfg(target_os = "linux")]
        {
            use std::os::fd::AsRawFd as _;
            // SAFETY: F_GETFD only reads the descriptor's flags.
            assert_ne!(
                unsafe { libc::fcntl(second.fd.as_raw_fd(), libc::F_GETFD) },
                -1
            );
        }
    }
}
