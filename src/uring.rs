use crate::common::*;
use io_uring::{opcode, types, IoUring, Probe};
use libc::{O_DIRECT, O_WRONLY};
use std::ffi::CString;
use std::os::unix::ffi::OsStrExt;
use std::os::unix::io::RawFd;
use std::path::Path;

struct CompletionData {
    wal_position: WalPosition,
    slice: AlignedSlice,
    notify: bool,
}

/// LinuxUring uses io_uring to write to the underlying device.
pub struct LinuxUring {
    fd: RawFd,
    uring: IoUring,
}

impl LinuxUring {
    pub fn new(path: &Path) -> std::io::Result<Self> {
        let path = CString::new(path.as_os_str().as_bytes())?;
        let fd = unsafe { libc::open(path.as_ptr(), O_WRONLY | O_DIRECT, 0o644) };
        if fd < 0 {
            return Err(std::io::Error::last_os_error());
        }

        let uring = IoUring::builder()
            .setup_sqpoll(100)
            .build(1024)
            .map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e))?;

        let mut probe = Probe::new();
        uring.submitter().register_probe(&mut probe)?;
        if !probe.is_supported(opcode::Write::CODE) {
            return Err(std::io::Error::new(
                std::io::ErrorKind::Unsupported,
                "io_uring write not supported",
            ));
        }

        Ok(LinuxUring { fd, uring })
    }
}

impl Drop for LinuxUring {
    fn drop(&mut self) {
        unsafe { libc::close(self.fd) };
    }
}

impl PersistentDevice for LinuxUring {
    fn write(&mut self, pos: WalPosition, data: AlignedSlice, notify: bool) -> std::io::Result<()> {
        let entry = opcode::Write::new(types::Fd(self.fd), data.buffer_ptr, data.size())
            .offset(pos.byte_offset())
            .build();

        let data_box = Box::new(CompletionData {
            slice: data,
            wal_position: pos,
            notify,
        });

        let entry = entry.user_data(Box::into_raw(data_box) as _);

        unsafe {
            let res = self.uring.submission().push(&entry);
            if res.is_err() {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::WouldBlock,
                    "submission queue full",
                ));
            }
        }

        self.uring.submitter().submit().map(|_| ())
    }

    // User needs to call this periodically from a thread to complete writes. One option would be
    // to call this before every append. However that isn't ideal if there is a long time between
    // appends as the data will be left around until the next append is called, and the user won't
    // be notified the data has been synced.
    fn process_completions(&mut self) -> std::vec::IntoIter<WalPosition> {
        type Iter = std::vec::IntoIter<WalPosition>;
        let mut v: Vec<WalPosition> = Vec::new();

        // TODO: Return the iterator live as we go rather than collecting first.
        for cqe in self.uring.completion() {
            let data = cqe.user_data();
            let data = unsafe { Box::from_raw(data as *mut CompletionData) };
            drop(data.slice);

            if cqe.result() >= 0 && data.notify {
                v.push(data.wal_position);
            }
            // TODO: How should an error result be handled, especially once this is converted to an
            // iterator. If we get an error here, its not clear if the underlying device is still
            // valid.
            //
            // The initial write buffer can now be dropped as the data is written to disk.
        }
        v.into_iter()
    }
}
