/// This table map the disk block (in the form of page) to the frame in physical memory
/// Each entry represent a map from page ---> frame
#[derive(Clone, Copy)]
pub struct PageTableEntry {
    /// | timestamp: f32 | frame number: u32 | pin: u8 | dirty: u8 |
    pub(super) entry: [u8; 10],
}

impl PageTableEntry {
    pub(super) fn zero() -> Self {
        PageTableEntry { entry: [0; 10] }
    }

    pub fn get_pin(&self) -> u8 {
        self.entry[8]
    }

    pub(super) fn pin(&mut self) {
        self.entry[8] += 1;
    }

    pub(super) fn set_timestamp(&mut self, timestamp: f32) {
        self.entry[0..4].copy_from_slice(&timestamp.to_be_bytes());
    }

    pub fn timestamp(&self) -> f32 {
        f32::from_be_bytes(self.entry[0..4].try_into().unwrap())
    }

    pub(super) fn unpin(&mut self) {
        if self.entry[8] == 0 {
            panic!("Page is not pinned");
        }
        self.entry[8] -= 1;
    }

    pub fn get_frame_number(&self) -> u32 {
        u32::from_be_bytes(self.entry[4..8].try_into().unwrap())
    }

    pub(super) fn set_frame_number(&mut self, frame_number: u32) {
        self.entry[4..8].copy_from_slice(&frame_number.to_be_bytes());
    }
}
