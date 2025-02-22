use log::info;

use crate::common::*;
use std::collections::HashMap;

/// MemDevice is an in-memory implementation of PersistentDevice that
/// holds the buffer in memory.
pub struct MemDevice {
    buffer: HashMap<u32, Vec<u8>>,
    completions: Vec<WalPosition>,
    capacity_blocks: u32,
}

impl MemDevice {
    pub fn new(capacity_blocks: u32) -> Self {
        info!("Initalizing mem device with capacity {}", capacity_blocks);
        Self {
            buffer: HashMap::new(),
            completions: Vec::new(),
            capacity_blocks,
        }
    }
}

impl PersistentDevice for MemDevice {
    fn write(&mut self, pos: WalPosition, data: AlignedSlice, notify: bool) -> std::io::Result<()> {
        // Check if write would exceed capacity
        let write_end = pos.offset + data.blocks;
        if write_end > self.capacity_blocks {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "Write would exceed device capacity",
            ));
        }

        // Store the data in memory
        let slice = unsafe { std::slice::from_raw_parts(data.buffer_ptr, data.size() as usize) };
        self.buffer.insert(pos.offset, slice.to_vec());

        // Track completion if requested
        if notify {
            self.completions.push(pos);
        }

        Ok(())
    }

    fn process_completions(&mut self) -> std::vec::IntoIter<WalPosition> {
        // Return the completions and clear the list
        let completions = std::mem::take(&mut self.completions);
        completions.into_iter()
    }

    fn read(&mut self, pos: u64, len: usize) -> std::io::Result<Vec<u8>> {
        self.buffer
            .get(&((pos / BLOCK_SIZE as u64) as u32))
            .map(|data| {
                if len > data.len() {
                    Err(std::io::Error::new(
                        std::io::ErrorKind::InvalidInput,
                        "Read length exceeds available data",
                    ))
                } else {
                    Ok(data[..len].to_vec())
                }
            })
            .unwrap_or_else(|| Ok(vec![0; len]))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_mem_buffer_device() -> std::io::Result<()> {
        let mut device = MemDevice::new(1024); // 1024 blocks capacity

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

        // Verify data was stored (only check first 16 bytes)
        assert_eq!(
            &device.buffer.get(&pos1.offset).unwrap()[..16],
            b"hello\0\0\0\0\0\0\0\0\0\0\0"
        );
        assert_eq!(
            &device.buffer.get(&pos2.offset).unwrap()[..16],
            b"world\0\0\0\0\0\0\0\0\0\0\0"
        );

        Ok(())
    }
}
