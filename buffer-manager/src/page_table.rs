use memory::{PhysicalMemory, MemoryError};

struct PageTableEntry {
    dirty: bool,
    pin_count: u8,
    block_number: u32,
}

impl PageTableEntry {
    fn new() -> Self {
        PageTableEntry {
            dirty: false,
            pin_count: 0,
            block_number: 0,
        }
    }
}

struct PageTable<const PAGESIZE: usize, const CAPACITY: usize> {
    memory: PhysicalMemory<CAPACITY>,
}

impl<const PAGESIZE: usize, const CAPACITY: usize> PageTable<PAGESIZE, CAPACITY> {
    fn new(memory: &PhysicalMemory<CAPACITY>) -> Self {
        PageTable {
            memory: memory.clone(),
        }
    }
}
