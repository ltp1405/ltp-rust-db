use disk::Disk;
use memory::{MemoryError, PhysicalMemory};

// Algorithm for writing page from memory to disk:
// - Check if it is dirty
// - If it is dirty, write it to disk
// - If it is not dirty, do nothing

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
