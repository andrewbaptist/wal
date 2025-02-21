use crate::common::*;
use log::warn;
use std::collections::VecDeque;
use std::fs::OpenOptions;
use std::io::{Seek, Write};
use std::path::Path;

/// SyncDevice uses standard synchronous file operations with deferred fsync
pub struct SyncDevice {
    file: std::fs::File,
    pending_syncs: VecDeque<WalPosition>,
}

impl SyncDevice {
    pub fn new(path: &Path) -> std::io::Result<Self> {
        let file = OpenOptions::new().write(true).create(true).open(path)?;

        Ok(Self {
            file,
            pending_syncs: VecDeque::new(),
        })
    }
}

impl PersistentDevice for SyncDevice {
    fn write(&mut self, pos: WalPosition, data: AlignedSlice, notify: bool) -> std::io::Result<()> {
        // Convert AlignedSlice to a regular slice
        let buffer = unsafe { std::slice::from_raw_parts(data.buffer_ptr, data.size() as usize) };

        // Perform the write using standard file operations
        self.file
            .seek(std::io::SeekFrom::Start(pos.byte_offset()))?;
        self.file.write_all(buffer)?;

        // Queue position for sync if requested
        if notify {
            self.pending_syncs.push_back(pos);
        }

        Ok(())
    }

    fn process_completions(&mut self) -> Box<dyn Iterator<Item = WalPosition>> {
        // Sync all pending writes
        if let Err(e) = self.file.sync_data() {
            warn!("Failed to sync data: {}", e);
            return Box::new(std::iter::empty());
        }

        // Return an iterator over the completed positions
        let completed = self.pending_syncs.drain(..).collect::<Vec<_>>();
        Box::new(completed.into_iter())
    }
}
