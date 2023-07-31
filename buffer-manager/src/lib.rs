mod page_table;

use disk::Disk;
use memory::{MemoryError, PhysicalMemory};

/// This struct is responsible for managing the memory and disk.
/// It is responsible for loading and unloading pages from the disk to the memory.
/// It is also responsible for writing pages from the memory to the disk.
/// It is also responsible for keeping track of the pages in memory and the pages in disk.
/// [ ] Read page from disk to memory
/// [ ] Write page from memory to disk
struct BufferManager<
    const BLOCK_SIZE: usize,
    const DISK_CAPACITY: usize,
    const MEMORY_CAPACITY: usize,
> {
    memory: PhysicalMemory<MEMORY_CAPACITY>,
    disk: Disk<BLOCK_SIZE, DISK_CAPACITY>,
}

impl<const BLOCK_SIZE: usize, const DISK_CAPACITY: usize, const MEMORY_CAPACITY: usize>
    BufferManager<BLOCK_SIZE, DISK_CAPACITY, MEMORY_CAPACITY>
{
    pub fn new(
        disk: &Disk<BLOCK_SIZE, DISK_CAPACITY>,
        memory: &PhysicalMemory<MEMORY_CAPACITY>,
    ) -> Self {
        BufferManager {
            memory: memory.clone(),
            disk: disk.clone(),
        }
    }

    pub fn get_page(&mut self, page_number: usize) -> Result<&mut [u8], MemoryError> {
        todo!()
    }
}