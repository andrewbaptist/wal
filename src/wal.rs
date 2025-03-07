use crate::common::*;
use log::{debug, info, warn};

#[cfg(target_os = "linux")]
use crate::uring::LinuxUring;

#[cfg(target_os = "macos")]
use crate::pwrite::MacOsAsyncIO;

use crate::sync::SyncDevice;

use crc32fast::Hasher;
use std::io::Error;
use std::path::Path;
use zerocopy::{FromBytes, IntoBytes};
use zerocopy_derive::{FromBytes, Immutable, IntoBytes, KnownLayout};

static HEADER_SIZE: usize = std::mem::size_of::<EntryHeader>();

#[repr(C, packed)]
#[derive(Copy, Clone, Debug, KnownLayout, Immutable, FromBytes, IntoBytes)]
struct EntryHeader {
    crc: u32,
    rollover: u32,
    // The length of the data.
    len: u32,
}

impl EntryHeader {
    // computes the crc skipping the first 4 bytes (which is where the CRC goes).
    fn compute_crc(&self, buffer: &[u8]) -> u32 {
        let mut hasher = Hasher::new();
        hasher.update(&buffer[4..HEADER_SIZE + self.len as usize]);
        hasher.finalize()
    }

    // This returns how many blocks are required to store the full entry.
    fn num_blocks(&self) -> u32 {
        (HEADER_SIZE + self.len as usize).div_ceil(BLOCK_SIZE as usize) as u32
    }
}

pub struct WalIterator<'a> {
    dev: &'a mut Box<dyn PersistentDevice>,
    current: WalPosition,
    end: WalPosition,
    // number of blocks in the file.
    capacity: u32,
}

impl<'a> WalIterator<'a> {
    pub fn new(
        dev: &'a mut Box<dyn PersistentDevice>,
        start: WalPosition,
        end: WalPosition,
        capacity: u32,
    ) -> Self {
        WalIterator {
            dev,
            current: start,
            end,
            capacity,
        }
    }
}

impl Iterator for WalIterator<'_> {
    type Item = std::io::Result<(WalPosition, Vec<u8>)>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.current >= self.end {
            return None;
        }

        // Read header
        let buffer = self
            .dev
            .read(self.current.byte_offset(), HEADER_SIZE)
            .ok()?;
        let header = match EntryHeader::read_from_bytes(&buffer) {
            Ok(h) => h,
            Err(_) => {
                return Some(Err(std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    "Invalid header",
                )))
            }
        };
        debug!("Found header {:?}", header);
        // Now we need to create a big enough buffer to hold the entire content if its bigger than
        // one block. We could use an aligned slice, but its not strictly necessary.
        let buffer = self
            .dev
            .read(
                self.current.byte_offset(),
                HEADER_SIZE + header.len as usize,
            )
            .ok()?;

        // Verify CRC - somewhat redundant, but done anyways.
        let crc = header.compute_crc(&buffer);
        if header.len != 0 && header.crc != 0 && crc != header.crc {
            return Some(Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                format!(
                    "iterator CRC mismatch {crc} != {:?} at {:?} with bytes {:?}",
                    header, self.current, &buffer
                ),
            )));
        }

        // Calculate next position
        let next_offset = self.current.offset + header.num_blocks();
        let current_pos = WalPosition {
            offset: self.current.offset,
            rollover: header.rollover,
        };

        if next_offset >= self.capacity {
            self.current = WalPosition {
                offset: 0,
                rollover: header.rollover + 1,
            };
        } else {
            self.current = WalPosition {
                offset: next_offset,
                rollover: header.rollover,
            };
        }

        Some(Ok((
            current_pos,
            buffer[HEADER_SIZE..][..header.len as usize].to_vec(),
        )))
    }
}

pub struct Wal {
    dev: Box<dyn PersistentDevice>,

    // capacity in blocks
    capacity: u32,
    // offset into the file.
    head: WalPosition,
    // offset into the file.
    tail: WalPosition,
}

pub type WalResult = Result<WalPosition, Error>;

impl Wal {
    // appends an entry to this WAL. The data is copied. The data is not guaranteed to be persisted
    // to disk when this returns. To get the completion, listen on the receiver channel.
    pub fn append(&mut self, data: &[u8]) -> std::io::Result<WalPosition> {
        let mut aligned = AlignedSlice::new(data.len() + HEADER_SIZE);
        let write_size = aligned.blocks;

        // Move the head for the next write and clear out all the existing data between the
        // head and that position.
        if self.head.offset + write_size > self.capacity {
            // TODO: This is going to confuse the caller since this will get returned from the call
            // to process_completions. We should figure out a way to exclude this write. as the
            // user never asked for it.
            let aligned =
                AlignedSlice::new(((self.capacity - self.head.offset) * BLOCK_SIZE) as usize);
            self.dev
                .write(self.head, aligned, false)
                .map(|_| self.head)?;

            self.head = WalPosition {
                offset: 0,
                rollover: self.head.rollover + 1,
            }
        }

        // Create an aligned buffer that outlives this function. It is destroyed when completion
        // happens.
        let buffer = aligned.as_slice();

        let mut header = EntryHeader {
            crc: 0,
            rollover: self.head.rollover,
            len: data.len() as u32,
        };
        debug!("Writing header {:?}", header);

        buffer[..HEADER_SIZE].copy_from_slice(header.as_bytes());
        buffer[HEADER_SIZE..HEADER_SIZE + data.len()].copy_from_slice(data);

        header.crc = header.compute_crc(buffer);
        // Re-copy the header with the CRC filled.
        buffer[..HEADER_SIZE].copy_from_slice(header.as_bytes());

        let res = self.dev.write(self.head, aligned, true).map(|_| self.head);

        // move the head to the next position for the next write. Note that this might be the end
        // of the file, but that is OK as it will be fixed by the subsequent write.
        self.head.offset += write_size;
        res
    }

    // truncate will move the tail forward to this position. If the position is behind the current
    // tail, then truncate is a no-op.
    pub fn truncate(&mut self, position: WalPosition) {
        let cur_rollover = self.head.rollover;
        if position.rollover < cur_rollover {
            // nothing to do, we are already past this position.
        }

        if position > self.tail {
            self.tail = position
        }
    }

    pub fn iterate(&mut self) -> WalIterator {
        let iterator = WalIterator::new(&mut self.dev, self.tail, self.head, self.capacity);
        info!("Recovering from {:?} to {:?}", self.tail, self.head);
        iterator
    }

    // Note that truncated entries can be revived during a recover as truncation is not persistent.
    // The caller needs to handle this and should call truncate after processing all the entries.
    /// Open the given URI and begin recovery. The WalIterator is returned.
    /// Supported URIs:
    ///   - mem:// - Use an in-memory device
    ///   - file:///path/to/file - Use a file-based device
    ///   - /path/to/file - Use a file-based device (backwards compatibility)
    pub fn open(url: url::Url) -> std::io::Result<Self> {
        info!("Starting recovery from {}", url);

        let (dev, capacity) = Self::create_device(url)?;

        let init_position = WalPosition {
            offset: 0,
            rollover: 0,
        };
        let mut wal = Wal {
            dev,
            capacity,
            head: init_position,
            tail: init_position,
        };

        recover(&mut wal)?;

        Ok(wal)
    }

    pub fn process_completions(&mut self) -> std::vec::IntoIter<WalPosition> {
        self.dev.process_completions()
    }

    fn create_device(url: url::Url) -> std::io::Result<(Box<dyn PersistentDevice>, u32)> {
        if url.scheme() == "mem" {
            // Parse size from path (e.g. mem://64 means 64 blocks)
            let blocks = url.path().parse::<u32>().unwrap();
            let dev: Box<dyn PersistentDevice> = Box::new(crate::mem::MemDevice::new(blocks));
            Ok((dev, blocks))
        } else if url.scheme() == "file" {
            let dev: Box<dyn PersistentDevice>;
            // Handle file paths
            let path = Path::new(url.path());
            println!("{:?}", path);

            // Check if we should force using specific devices
            // TODO: This would be better as a different url scheme.
            let use_sync = std::env::var("WAL_SYNC_DEVICE").is_ok();

            if use_sync {
                dev = Box::new(SyncDevice::new(path)?);
            } else {
                // Use platform-specific device implementations
                #[cfg(target_os = "linux")]
                {
                    dev = Box::new(LinuxUring::new(path)?);
                }
                #[cfg(target_os = "macos")]
                {
                    dev = Box::new(MacOsAsyncIO::new(path)?);
                }
                #[cfg(not(any(target_os = "linux", target_os = "macos")))]
                {
                    dev = Box::new(SyncDevice::new(path)?);
                }
            }

            let path = Path::new(url.path());
            let capacity_bytes = path.metadata()?.len();
            if capacity_bytes % BLOCK_SIZE as u64 != 0 {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    format!(
                        "size {} is not a multiple of BLOCK_SIZE {}",
                        capacity_bytes, BLOCK_SIZE
                    ),
                ));
            }
            let capacity = (capacity_bytes / BLOCK_SIZE as u64) as u32;
            Ok((dev, capacity))
        } else {
            Err(Error::new(
                std::io::ErrorKind::Unsupported,
                "Unknown URI scheme",
            ))
        }
    }
}

// Reads from the device to initialize the wal head and tail.
fn recover(wal: &mut Wal) -> Result<(), Error> {
    loop {
        let buffer = wal.dev.read(wal.head.byte_offset(), BLOCK_SIZE as usize)?;

        // Read the header including the CRC.
        let header = match EntryHeader::read_from_bytes(&buffer[..HEADER_SIZE]) {
            Ok(h) => h,
            Err(_) => break,
        };

        // We don't support writing 0 length entries. If we find a zero it means the data
        // wasn't initialized.
        // TODO: Enforce not allowing 0 length writes.
        if header.len == 0 {
            debug!("Found empty entry");
            break;
        }

        // Back up and read the entire data in one buffer.
        let buffer = wal
            .dev
            .read(wal.head.byte_offset(), HEADER_SIZE + header.len as usize)?;

        // Verify CRC
        let crc = header.compute_crc(&buffer);
        if crc != header.crc {
            warn!("open CRC mismatch {crc}, {:?}", header);
            break;
        }

        debug!("Head {:?}, found {:?}", wal.head, header);
        // Stop once we find an entry that goes backwards.
        if header.rollover < wal.head.rollover {
            debug!("Found older entry");
            break;
        }

        // Otherwise find the next place to try and read from (TODO: Handle the overflow case).
        let next_offset = wal.head.offset + header.num_blocks();
        if next_offset >= wal.capacity {
            debug!("Found end of file");
            break;
        }
        wal.head.offset = next_offset;
        wal.head.rollover = header.rollover;
        debug!("Moving head to {:?}", wal.head);
    }
    Ok(if wal.head.rollover > 0 {
        wal.tail = WalPosition {
            offset: wal.head.offset,
            rollover: wal.head.rollover - 1,
        };

        debug!("Finding tail starting from {:?}", wal.tail);

        // We need to find the old tail based on where the head ended. Scan forward from where the
        // head currently is until we find a valid entry that is one rollover behind us.
        for offset in (wal.tail.offset..wal.capacity).step_by(BLOCK_SIZE as usize) {
            debug!("Checking offset {}", offset);

            let buffer = wal.dev.read(wal.head.byte_offset(), BLOCK_SIZE as usize)?;
            // Read the header including the CRC.
            let header = EntryHeader::read_from_bytes(&buffer[..HEADER_SIZE]);

            // This can happen because there was garbage before our first entry, keep scanning
            // forwards until we find something useful.
            // TODO: This might not ever happen
            if header.is_err() {
                debug!("Found undecodable header, skipping");
                continue;
            }

            let header = header.unwrap();

            if header.rollover != wal.tail.rollover {
                debug!(
                    "Found a header with the wrong rollover, skipping {:?}",
                    header
                );
                continue;
            }

            // TODO: Add a security mechanism against someone writing a bad block that looks like a
            // header and checks out from a CRC perspective.
            //
            // Make sure the data really is valid by checking the CRC.
            let buffer = wal
                .dev
                .read(wal.head.byte_offset(), HEADER_SIZE + header.len as usize)?;

            // Verify CRC
            let crc = header.compute_crc(&buffer);
            if crc != header.crc {
                warn!("Tail CRC mismatch {crc}, {:?}", header);
                continue;
            }

            // At this point we found a valid old entry. Set this as our tail and we are done.
            wal.tail.offset = offset;
            break;
        }
    })
}

impl Drop for Wal {
    fn drop(&mut self) {
        // Discard all the data that is completed when the wal is being dropped.
        for _ in self.dev.process_completions() {}
    }
}
