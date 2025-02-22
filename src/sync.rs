use crate::common::*;
use log::warn;
use std::collections::VecDeque;
use std::fs::OpenOptions;
use std::io::{Read, Seek, Write};
use std::path::Path;

/// SyncDevice uses standard synchronous file operations with deferred fsync
pub struct SyncDevice {
    file: std::fs::File,
    pending_syncs: VecDeque<WalPosition>,
}

impl SyncDevice {
    // The user must create the file before calling new.
    pub fn new(path: &Path) -> std::io::Result<Self> {
        let file = OpenOptions::new()
            .write(true)
            .read(true)
            .truncate(false)
            .create(false)
            .open(path)?;

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
        let file_len = self.file.metadata()?.len();
        let write_end = pos.byte_offset() + buffer.len() as u64;
        if write_end > file_len {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "Write position exceeds file length",
            ));
        }
        self.file
            .seek(std::io::SeekFrom::Start(pos.byte_offset()))?;
        self.file.write_all(buffer)?;

        // Queue position for sync if requested
        if notify {
            self.pending_syncs.push_back(pos);
        }

        Ok(())
    }

    fn process_completions(&mut self) -> std::vec::IntoIter<WalPosition> {
        // Sync all pending writes
        if let Err(e) = self.file.sync_data() {
            warn!("Failed to sync data: {}", e);
            return Vec::new().into_iter();
        }

        // Return an iterator over the completed positions
        let completed = self.pending_syncs.drain(..).collect::<Vec<_>>();
        completed.into_iter()
    }

    fn read(&mut self, pos: u64, len: usize) -> std::io::Result<Vec<u8>> {
        let mut buffer = vec![0; len];
        self.file.seek(std::io::SeekFrom::Start(pos))?;
        self.file.read_exact(&mut buffer)?;
        Ok(buffer)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Read;
    use tempfile::NamedTempFile;

    #[test]
    fn test_sync_device_basic_operations() -> std::io::Result<()> {
        let temp_file = NamedTempFile::new()?;
        temp_file.as_file().set_len(16 * 1024)?;
        let path = temp_file.path();

        // Create new SyncDevice
        let mut device = SyncDevice::new(path)?;

        // Create test data
        let test_data = b"Hello, world!";
        let mut aligned = AlignedSlice::new(test_data.len());
        aligned.as_slice()[..test_data.len()].copy_from_slice(test_data);

        // Test write without notification
        let pos = WalPosition {
            offset: 0,
            rollover: 0,
        };
        device.write(pos, aligned, false)?;

        // Verify no completions
        let completions: Vec<_> = device.process_completions().collect();
        assert!(completions.is_empty());

        // Test write with notification
        let mut aligned = AlignedSlice::new(test_data.len());
        aligned.as_slice()[..test_data.len()].copy_from_slice(test_data);
        device.write(pos, aligned, true)?;

        // Verify completion
        let completions: Vec<_> = device.process_completions().collect();
        assert_eq!(completions, vec![pos]);

        // Verify data was written correctly
        let mut file = std::fs::File::open(path)?;
        let mut buffer = vec![0; test_data.len()];
        file.seek(std::io::SeekFrom::Start(pos.byte_offset()))?;
        file.read_exact(&mut buffer)?;
        assert_eq!(buffer, test_data);

        Ok(())
    }

    #[test]
    fn test_sync_device_multiple_writes() -> std::io::Result<()> {
        let temp_file = NamedTempFile::new()?;
        temp_file.as_file().set_len(16 * 1024)?;
        let path = temp_file.path();

        let mut device = SyncDevice::new(path)?;

        // Write multiple positions
        let pos1 = WalPosition {
            offset: 0,
            rollover: 0,
        };
        let pos2 = WalPosition {
            offset: 1,
            rollover: 0,
        };

        let aligned = AlignedSlice::new(10);
        device.write(pos1, aligned, true)?;

        let aligned = AlignedSlice::new(10);
        device.write(pos2, aligned, true)?;

        // Add write that shouldn't trigger completion
        let pos3 = WalPosition {
            offset: 2,
            rollover: 0,
        };
        let aligned = AlignedSlice::new(10);
        device.write(pos3, aligned, false)?;

        // Verify both completions
        let completions: Vec<_> = device.process_completions().collect();
        assert_eq!(completions.len(), 2);
        assert!(completions.contains(&pos1));
        assert!(completions.contains(&pos2));

        Ok(())
    }

    #[test]
    fn test_sync_device_error_handling() -> std::io::Result<()> {
        let temp_file = NamedTempFile::new()?;
        temp_file.as_file().set_len(16 * 1024)?;
        let path = temp_file.path();

        let mut device = SyncDevice::new(path)?;

        // Test invalid position
        let invalid_pos = WalPosition {
            offset: 100000,
            rollover: 0,
        };
        let aligned = AlignedSlice::new(10);
        let result = device.write(invalid_pos, aligned, true);
        assert!(result.is_err());

        Ok(())
    }
}
