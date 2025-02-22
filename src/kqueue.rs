use crate::common::AlignedSlice;
use crate::common::WalPosition;
use log::debug;
use log::warn;

use crate::common::PersistentDevice;

use libc::{self, c_void, F_NOCACHE, O_NONBLOCK, O_WRONLY};
use std::ffi::c_int;
use std::ffi::CString;
use std::fs::File;
use std::fs::OpenOptions;
use std::io::Read;
use std::io::Seek;
use std::os::unix::ffi::OsStrExt;
use std::os::unix::io::RawFd;
use std::path::Path;

// TODO: Figure out the right import for this
pub const SIGEV_KEVENT: c_int = 3;
pub const SIGIO: c_int = 23;

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
    file: File,
}

// Struct to hold completion data along with the AIO control block.
struct AioRequest {
    aio: libc::aiocb,
    completion_data: CompletionData,
}

impl KQueue {
    pub fn new(path: &Path) -> std::io::Result<Self> {
        let file = OpenOptions::new().read(true).open(path)?;
        let path = CString::new(path.as_os_str().as_bytes())?;
        let fd = unsafe { libc::open(path.as_ptr(), O_NONBLOCK | O_WRONLY | F_NOCACHE, 0o644) };
        if fd < 0 {
            return Err(std::io::Error::last_os_error());
        }

        let kq = unsafe { libc::kqueue() };
        if kq == -1 {
            let err = std::io::Error::last_os_error();
            unsafe { libc::close(fd) };
            return Err(err);
        }

        Ok(KQueue { fd, kq, file })
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

const MAX_COMPLETIONS: usize = 1024;

impl PersistentDevice for KQueue {
    fn write(&mut self, pos: WalPosition, data: AlignedSlice, notify: bool) -> std::io::Result<()> {
        let completion_data = CompletionData {
            wal_position: pos,
            slice: data,
            notify,
        };

        // Allocate AioRequest on the heap.
        let aio_request = Box::new(AioRequest {
            aio: unsafe { std::mem::zeroed() },
            completion_data,
        });

        // Convert to raw pointer to manage ownership.
        let aio_request_ptr = Box::into_raw(aio_request);

        // Initialize the AIO control block. The aio struct is self-referencial which requires unsafe rust to accomplish.
        unsafe {
            (*aio_request_ptr).aio.aio_fildes = self.fd;
            (*aio_request_ptr).aio.aio_offset = pos.byte_offset() as i64;
            let mut event: libc::sigevent = std::mem::zeroed();
            event.sigev_notify = SIGEV_KEVENT;
            event.sigev_signo = SIGIO;
            event.sigev_value = libc::sigval {
                sival_ptr: aio_request_ptr as *mut c_void,
            };

            (*aio_request_ptr).aio.aio_sigevent = event;
            (*aio_request_ptr).aio.aio_buf =
                (*aio_request_ptr).completion_data.slice.buffer_ptr as *mut c_void;
            (*aio_request_ptr).aio.aio_nbytes =
                (*aio_request_ptr).completion_data.slice.size() as usize;
            println!("{:#?}", (*aio_request_ptr).aio);
        }

        // Submit the aio_write.
        let result = unsafe { libc::aio_write(&mut (*aio_request_ptr).aio) };
        if result != 0 {
            // Reclaim the Box on failure.
            let _ = unsafe { Box::from_raw(aio_request_ptr) };
            let err = Err(std::io::Error::last_os_error());
            warn!("kevent error: {:?}", err);
            return err;
        }

        // Register the AIO event with kqueue
        let mut kev = libc::kevent {
            ident: self.fd as usize,
            filter: libc::EVFILT_AIO,
            flags: libc::EV_ADD | libc::EV_ENABLE,
            fflags: 0,
            data: 0,
            udata: aio_request_ptr as *mut c_void,
        };

        let result = unsafe {
            libc::kevent(
                self.kq,
                &mut kev as *mut libc::kevent,
                1,
                std::ptr::null_mut(),
                0,
                std::ptr::null(),
            )
        };

        if result == -1 {
            // Cancel the AIO request and clean up
            unsafe { libc::aio_cancel(self.fd, &mut (*aio_request_ptr).aio) };
            let _ = unsafe { Box::from_raw(aio_request_ptr) };
            return Err(std::io::Error::last_os_error());
        }

        Ok(())
    }

    fn process_completions(&mut self) -> Box<dyn Iterator<Item = WalPosition>> {
        let mut completed_positions = Vec::new();
        let mut events = vec![unsafe { std::mem::zeroed::<libc::kevent>() }; MAX_COMPLETIONS];

        loop {
            let nev = unsafe {
                libc::kevent(
                    self.kq,
                    std::ptr::null(),
                    0,
                    events.as_mut_ptr(),
                    events.len() as _,
                    // Use non-blocking mode with zero timeout
                    &libc::timespec {
                        tv_sec: 0,
                        tv_nsec: 0,
                    },
                )
            };

            if nev == -1 {
                let err = std::io::Error::last_os_error();
                if err.kind() == std::io::ErrorKind::WouldBlock {
                    // No more events available
                    break;
                }
                warn!("kevent error: {}", err);
                break;
            }

            if nev == 0 {
                // No events available
                break;
            }

            debug!("Found {nev} events");

            for event in events.iter().take(nev as usize) {
                if event.filter == libc::EVFILT_AIO {
                    let aio_request_ptr = event.udata as *mut AioRequest;
                    let mut aio_request = unsafe { Box::from_raw(aio_request_ptr) };

                    let result = unsafe { libc::aio_error(&aio_request.aio) };
                    if result == 0 {
                        // Success case
                        let bytes_written = unsafe { libc::aio_return(&mut aio_request.aio) };
                        if bytes_written >= 0 && aio_request.completion_data.notify {
                            debug!(
                                "Completed write at {:?} ({} bytes)",
                                aio_request.completion_data.wal_position, bytes_written
                            );
                            completed_positions.push(aio_request.completion_data.wal_position);
                        }
                    } else if result == libc::EINPROGRESS {
                        // Still in progress, put it back
                        let _ = Box::into_raw(aio_request);
                        continue;
                    } else {
                        // Error case
                        warn!(
                            "AIO error for position {:?}: {}",
                            aio_request.completion_data.wal_position,
                            std::io::Error::from_raw_os_error(result)
                        );
                    }

                    // AlignedSlice will be dropped when aio_request goes out of scope
                }
            }
        }

        Box::new(completed_positions.into_iter())
    }

    fn read(&mut self, pos: u64, len: usize) -> std::io::Result<Vec<u8>> {
        let mut buffer = vec![0; len];
        self.file.seek(std::io::SeekFrom::Start(pos))?;
        self.file.read_exact(&mut buffer)?;
        Ok(buffer)
    }
}
