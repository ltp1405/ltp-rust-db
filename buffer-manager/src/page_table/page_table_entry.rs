use std::{mem::size_of, ops::Deref};

/// This table map the disk block (in the form of page) to the frame in physical memory
/// Each entry represent a map from page ---> frame
#[derive(Clone)]
pub struct PageTableEntry {
    /// | timestamp: f64 | frame number: u32 | pin: u8 | dirty: u8 |
    entry: [u8; 8 + 4 + 1 + 1],
}

impl Deref for PageTableEntry {
    type Target = [u8];
    fn deref(&self) -> &Self::Target {
        &self.entry
    }
}

impl PageTableEntry {
    pub fn set_timestamp(&mut self, timestamp: f64) {
        self.entry[..8].copy_from_slice(&timestamp.to_be_bytes());
    }
    pub fn timestamp(&self) -> f64 {
        f64::from_be_bytes(self.entry[..8].try_into().unwrap())
    }

    pub fn size() -> usize {
        size_of::<PageTableEntry>()
    }

    pub fn zero() -> Self {
        PageTableEntry { entry: [0; 14] }
    }

    pub fn get_pin(&self) -> u8 {
        self.entry[12]
    }

    pub fn is_empty(&self) -> bool {
        self.entry == [0; 14]
    }

    pub fn from_bytes(bytes: &[u8]) -> Self {
        let mut entry = [0; 14];
        entry.copy_from_slice(bytes);
        PageTableEntry { entry }
    }

    pub fn pin(&mut self) {
        self.entry[12] += 1;
    }

    pub fn unpin(&mut self) {
        if self.entry[12] == 0 {
            panic!("Page is not pinned");
        }
        self.entry[12] -= 1;
    }

    pub fn get_frame_number(&self) -> u32 {
        u32::from_be_bytes(self.entry[8..12].try_into().unwrap())
    }

    pub fn set_frame_number(&mut self, frame_number: u32) {
        self.entry[8..12].copy_from_slice(&frame_number.to_be_bytes());
    }

    pub fn is_dirty(&self) -> bool {
        self.entry[13] == 1
    }

    pub fn set_dirty(&mut self) {
        self.entry[13] = 1;
    }
}
