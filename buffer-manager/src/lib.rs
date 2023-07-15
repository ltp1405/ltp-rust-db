use disk::Disk;
use memory::{MemoryError, PhysicalMemory};

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
    pub fn get_page(&mut self, page_id: usize) -> Result<&mut [u8], MemoryError> {
        todo!()
    }
}
