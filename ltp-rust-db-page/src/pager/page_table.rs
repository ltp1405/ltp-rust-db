use super::TABLE_MAX_PAGES;

// NOTE: This is just a temporary solutions, this should be encoded in u64
#[derive(Clone, Copy)]
pub struct PageTableEntry {
    pub dirty: bool,
    pub frame_number: u32,
}

impl PageTableEntry {
    fn new() -> Self {
        PageTableEntry {
            dirty: false,
            frame_number: 0,
        }
    }

    fn is_unused(&self) -> bool {
        self.frame_number == 0
    }

    fn set_unused(&mut self) {
        self.frame_number = 0;
    }

    fn frame(&mut self, frame_number: u32) {
        self.frame_number = frame_number;
    }

    fn is_dirty(&self) -> bool {
        self.dirty
    }
}

pub struct PageTable {
    entries: [PageTableEntry; TABLE_MAX_PAGES],
}

impl PageTable {
    pub fn init() -> PageTable {
        PageTable {
            entries: [PageTableEntry::new(); TABLE_MAX_PAGES],
        }
    }
}
