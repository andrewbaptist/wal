use crate::AlignedSlice;
use crate::WalPosition;
use log::debug;

use crate::PersistentDevice;

use libc::{self, c_void, F_NOCACHE, O_WRONLY};
use std::ffi::c_int;
use std::ffi::CString;
use std::os::unix::ffi::OsStrExt;
use std::os::unix::io::RawFd;
use std::path::Path;

// TODO: Figure out the right import for this
pub const SIGEV_KEVENT: c_int = 3;

// Same as Linux implementation.
struct CompletionData {
    wal_position: WalPosition,
    slice: AlignedSlice,
    notify: bool,
}

/// KQueue uses kqueue and aio to write to the underlying device.
pub struct KQueue {
    fd: RawFd,
    kq: RawFd,
}

// Struct to hold completion data along with the AIO control block.
struct AioRequest {
    aio: libc::aiocb,
    completion_data: CompletionData,
}

impl KQueue {
    pub fn new(path: &Path) -> std::io::Result<Self> {
        let path = CString::new(path.as_os_str().as_bytes())?;
        let fd = unsafe { libc::open(path.as_ptr(), O_WRONLY | F_NOCACHE, 0o644) };
        if fd < 0 {
            return Err(std::io::Error::last_os_error());
        }

        let kq = unsafe { libc::kqueue() };
        if kq == -1 {
            let err = std::io::Error::last_os_error();
            unsafe { libc::close(fd) };
            return Err(err);
        }

        Ok(KQueue { fd, kq })
    }
}

impl Drop for KQueue {
    fn drop(&mut self) {
        unsafe {
            libc::close(self.fd);
            libc::close(self.kq);
        }
    }
}

impl PersistentDevice for KQueue {
    fn write(&mut self, pos: WalPosition, data: AlignedSlice, notify: bool) -> std::io::Result<()> {
        let completion_data = CompletionData {
            wal_position: pos,
            slice: data,
            notify,
        };

        let mut aio: libc::aiocb;
        unsafe {
            aio = libc::aiocb {
                aio_fildes: self.fd,
                aio_offset: pos.byte_offset() as i64,
                aio_buf: completion_data.slice.buffer_ptr as *mut c_void,
                aio_nbytes: completion_data.slice.size() as usize,
                ..std::mem::zeroed()
            };
        }

        // Allocate AioRequest on the heap.
        let aio_request = Box::new(AioRequest {
            aio: aio,
            completion_data,
        });

        // Convert to raw pointer to manage ownership.
        let aio_request_ptr = Box::into_raw(aio_request);

        aio.aio_sigevent.sigev_notify = SIGEV_KEVENT;
        aio.aio_sigevent.sigev_signo = self.kq;
        aio.aio_sigevent.sigev_value = libc::sigval {
            sival_ptr: aio_request_ptr as *mut c_void,
        };

        // Submit the aio_write.
        let result = unsafe { libc::aio_write(&mut (*aio_request_ptr).aio) };
        if result != 0 {
            // Reclaim the Box on failure.
            let _ = unsafe { Box::from_raw(aio_request_ptr) };
            return Err(std::io::Error::last_os_error());
        }

        Ok(())
    }

    fn process_completions(&mut self) -> Box<dyn Iterator<Item = WalPosition>> {
        let mut completed_positions = Vec::new();
        let mut events = vec![unsafe { std::mem::zeroed::<libc::kevent>() }; 1024];

        let nev = unsafe {
            libc::kevent(
                self.kq,
                std::ptr::null(),
                0,
                events.as_mut_ptr(),
                events.len() as _,
                std::ptr::null(),
            )
        };

        if nev == -1 {
            return Box::new(completed_positions.into_iter());
        }

        for i in 0..nev as usize {
            let event = &events[i];
            if event.filter == libc::EVFILT_AIO {
                let aio_request_ptr = event.udata as *mut AioRequest;
                let mut aio_request = unsafe { Box::from_raw(aio_request_ptr) };

                let result = unsafe { libc::aio_error(&aio_request.aio) };
                if result == 0 {
                    let bytes_written = unsafe { libc::aio_return(&mut aio_request.aio) };
                    if bytes_written >= 0 && aio_request.completion_data.notify {
                        debug!(
                            "Completed write at {:?}",
                            aio_request.completion_data.wal_position
                        );
                        completed_positions.push(aio_request.completion_data.wal_position);
                    }
                }

                // The AlignedSlice is dropped when `aio_request` is dropped.
            }
        }

        Box::new(completed_positions.into_iter())
    }
}
