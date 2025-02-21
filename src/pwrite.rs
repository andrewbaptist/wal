use crate::common::*;
use log::debug;
//use crossbeam::channel::{self, TrySendError};
use libc::{self, F_NOCACHE, O_WRONLY};
use std::ffi::CString;
use std::os::unix::ffi::OsStrExt;
use std::path::Path;
use std::sync::mpsc;
use std::sync::mpsc::TrySendError;

struct CompletionData {
    wal_position: WalPosition,
    slice: AlignedSlice,
    notify: bool,
}

/// MacOsAsyncIO uses a background thread with pwrite and F_NOCACHE for direct I/O
pub struct MacOsAsyncIO {
    task_sender: mpsc::SyncSender<CompletionData>,
    completion_receiver: mpsc::Receiver<WalPosition>,
}

impl MacOsAsyncIO {
    pub fn new(path: &Path) -> std::io::Result<Self> {
        let path = CString::new(path.as_os_str().as_bytes())?;

        // Open file with write-only mode and NOCACHE,
        let fd = unsafe { libc::open(path.as_ptr(), O_WRONLY | F_NOCACHE, 0o644) };
        if fd < 0 {
            return Err(std::io::Error::last_os_error());
        }

        // Create communication channels
        let (task_sender, task_receiver) = mpsc::sync_channel::<CompletionData>(1000);
        let (completion_sender, completion_receiver) = mpsc::channel::<WalPosition>();

        // Spawn worker thread that owns the file descriptor
        std::thread::spawn(move || {
            // Main worker loop
            while let Ok(data) = task_receiver.recv() {
                let res = unsafe {
                    libc::pwrite(
                        fd,
                        data.slice.buffer_ptr as *const libc::c_void,
                        data.slice.size() as usize,
                        data.wal_position.byte_offset() as i64,
                    )
                };

                // Handle completion notification
                if res >= 0 && data.notify {
                    debug!("pwrite completed at {:?}", data.wal_position);
                    let _ = completion_sender.send(data.wal_position);
                }

                // Explicitly drop the AlignedSlice to release resources
                drop(data.slice);
            }

            // Cleanup: close file descriptor when done
            unsafe { libc::close(fd) };
        });

        Ok(Self {
            task_sender,
            completion_receiver,
        })
    }
}

impl PersistentDevice for MacOsAsyncIO {
    fn write(&mut self, pos: WalPosition, data: AlignedSlice, notify: bool) -> std::io::Result<()> {
        let data = CompletionData {
            wal_position: pos,
            slice: data,
            notify,
        };

        self.task_sender.try_send(data).map_err(|e| match e {
            TrySendError::Full(_) => {
                std::io::Error::new(std::io::ErrorKind::WouldBlock, "task queue full")
            }
            TrySendError::Disconnected(_) => {
                std::io::Error::new(std::io::ErrorKind::BrokenPipe, "worker thread disconnected")
            }
        })
    }

    fn process_completions(&mut self) -> Box<dyn Iterator<Item = WalPosition>> {
        let mut completions = Vec::new();

        // Drain all available completion notifications
        while let Ok(pos) = self.completion_receiver.try_recv() {
            completions.push(pos);
        }

        Box::new(completions.into_iter())
    }
}
