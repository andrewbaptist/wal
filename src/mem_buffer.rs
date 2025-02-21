use crate::common::*;
use std::collections::HashMap;

/// MemBufferDevice is an in-memory implementation of PersistentDevice that
/// actually stores the written data for testing purposes.
pub struct MemBufferDevice {
    buffer: HashMap<WalPosition, Vec<u8>>,
    completions: Vec<WalPosition>,
}

impl MemBufferDevice {
    pub fn new() -> Self {
        Self {
            buffer: HashMap::new(),
            completions: Vec::new(),
        }
    }
}

impl PersistentDevice for MemBufferDevice {
    fn write(&mut self, pos: WalPosition, data: AlignedSlice, notify: bool) -> std::io::Result<()> {
        // Store the data in memory
        let slice = unsafe { std::slice::from_raw_parts(data.buffer_ptr, data.size() as usize) };
        self.buffer.insert(pos, slice.to_vec());

        // Track completion if requested
        if notify {
            self.completions.push(pos);
        }

        Ok(())
    }

    fn process_completions(&mut self) -> Box<dyn Iterator<Item = WalPosition>> {
        // Return the completions and clear the list
        let completions = std::mem::take(&mut self.completions);
        Box::new(completions.into_iter())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_mem_buffer_device() -> std::io::Result<()> {
        let mut device = MemBufferDevice::new();

        // Test write with notification
        let pos1 = WalPosition {
            offset: 0,
            rollover: 0,
        };
        let mut aligned1 = AlignedSlice::new(10);
        aligned1.as_slice()[..5].copy_from_slice(b"hello");
        device.write(pos1, aligned1, true)?;

        // Test write without notification
        let pos2 = WalPosition {
            offset: 1,
            rollover: 0,
        };
        let mut aligned2 = AlignedSlice::new(10);
        aligned2.as_slice()[..5].copy_from_slice(b"world");
        device.write(pos2, aligned2, false)?;

        // Verify completions
        let completions: Vec<_> = device.process_completions().collect();
        assert_eq!(completions, vec![pos1]);

        // Verify data was stored
        assert_eq!(device.buffer.get(&pos1).unwrap(), b"hello\0\0\0\0\0");
        assert_eq!(device.buffer.get(&pos2).unwrap(), b"world\0\0\0\0\0");

        Ok(())
    }
}
