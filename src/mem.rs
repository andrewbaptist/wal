use crate::common::*;
use std::mem;

pub struct MemDevice {
    v: Vec<WalPosition>,
}

impl MemDevice {
    pub fn new() -> std::io::Result<Self> {
        Ok(MemDevice { v: Vec::new() })
    }
}

impl PersistentDevice for MemDevice {
    fn write(&mut self, pos: WalPosition, _: AlignedSlice, notify: bool) -> std::io::Result<()> {
        if notify {
            self.v.push(pos);
        }
        Ok(())
    }

    fn process_completions(&mut self) -> Box<dyn Iterator<Item = WalPosition>> {
        let iter = mem::take(&mut self.v).into_iter();
        self.v = Vec::new();
        Box::new(iter)
    }
}
