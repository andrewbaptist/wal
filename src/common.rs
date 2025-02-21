use std::alloc::{alloc_zeroed, dealloc, Layout};
use std::cmp::Ordering::{Equal, Greater, Less};

/// Use a 4K block size to align to the underlying hardware requirements.
pub const BLOCK_SIZE: u32 = 4096;

/// A PersistentDevice allows writing aligned slices to it and should return immediately.
pub trait PersistentDevice: Send {
    /// Write this aligned slice at the given position and return immediately. An error is returned
    /// if it is unable to initate the write.
    fn write(&mut self, pos: WalPosition, data: AlignedSlice, notify: bool) -> std::io::Result<()>;

    // TODO: This shouldn't require a Box, but I'm not sure how to do this correctly.
    /// process_completions must be called periodically by the client to determine which previously
    /// written data has been synced to disk. It will write any completed data to the given channel.
    fn process_completions(&mut self) -> Box<dyn Iterator<Item = WalPosition>>;

    // AI! Add a method read() which pakes a WalPostion and a length and returns the data in it.
}

#[derive(Debug, Copy, Clone, PartialEq, Eq, Hash)]
pub struct WalPosition {
    // block offset into the file
    pub offset: u32,
    pub rollover: u32,
}

impl WalPosition {
    pub fn byte_offset(&self) -> u64 {
        self.offset as u64 * BLOCK_SIZE as u64
    }
}

impl PartialOrd for WalPosition {
    fn partial_cmp(&self, other: &WalPosition) -> Option<std::cmp::Ordering> {
        if self.rollover > other.rollover {
            Some(Greater)
        } else if self.rollover < other.rollover {
            Some(Less)
        } else if self.offset > other.offset {
            Some(Greater)
        } else if self.offset < other.offset {
            Some(Less)
        } else {
            Some(Equal)
        }
    }
}

/// AlignedSlice takes an unaligned size and creates an underlying buffer that is aligned to the
/// BLOCK_SIZE of the underlying device. It will free the memory when the AlignedSlice is dropped.
/// Alignment of the slice means that we always write at block boundaries.
pub struct AlignedSlice {
    pub buffer_ptr: *mut u8,
    pub blocks: u32,
}

unsafe impl Send for AlignedSlice {}

impl AlignedSlice {
    pub fn new(raw_size: usize) -> Self {
        let blocks = (raw_size).div_ceil(BLOCK_SIZE as usize) as u32;
        let layout = AlignedSlice::get_layout(blocks);
        let buffer_ptr = unsafe { alloc_zeroed(layout) };
        AlignedSlice { buffer_ptr, blocks }
    }

    pub fn as_slice(&mut self) -> &mut [u8] {
        unsafe {
            std::slice::from_raw_parts_mut(self.buffer_ptr, (self.blocks * BLOCK_SIZE) as usize)
        }
    }

    fn get_layout(blocks: u32) -> Layout {
        Layout::from_size_align((blocks * BLOCK_SIZE) as usize, BLOCK_SIZE as usize)
            .expect("invalid layout")
    }

    pub fn size(&self) -> u32 {
        self.blocks * BLOCK_SIZE
    }
}

impl Drop for AlignedSlice {
    fn drop(&mut self) {
        let layout = AlignedSlice::get_layout(self.blocks);
        unsafe {
            dealloc(self.buffer_ptr, layout);
        }
    }
}
