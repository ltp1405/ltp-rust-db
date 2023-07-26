pub mod page;

use disk::Disk;
use memory::{MemoryError, PhysicalMemory};

enum MemoryType {
    Memory { page_number: u32 },
    Disk { block_address: u32 },
}

impl MemoryType {
    pub fn from_bytes(bytes: &[u8]) -> Self {
        let mem_type = if bytes[0] == 0x1 {
            MemoryType::Memory {
                page_number: u32::from_be_bytes(bytes[1..5].try_into().unwrap()),
            }
        } else if bytes[0] == 0x2 {
            MemoryType::Disk {
                block_address: u32::from_be_bytes(bytes[1..5].try_into().unwrap()),
            }
        } else {
            panic!("Invalid memory type")
        };
        mem_type
    }

    pub fn to_bytes(&self) -> [u8; 5] {
        let mut bytes = [0; 5];
        match self {
            MemoryType::Memory { page_number } => {
                bytes[0] = 0x1;
                bytes[1..5].copy_from_slice(&page_number.to_be_bytes());
            }
            MemoryType::Disk {
                block_address: page_number,
            } => {
                bytes[0] = 0x2;
                bytes[1..5].copy_from_slice(&page_number.to_be_bytes());
            }
        }
        bytes
    }
}

struct BufferManager<
    'a,
    const BLOCK_SIZE: usize,
    const DISK_CAPACITY: usize,
    const MEMORY_CAPACITY: usize,
> {
    memory: [u8; MEMORY_CAPACITY],
    disk: Disk<BLOCK_SIZE, DISK_CAPACITY>,
}

impl<'a, const BLOCK_SIZE: usize, const DISK_CAPACITY: usize, const MEMORY_CAPACITY: usize>
    BufferManager<'a, BLOCK_SIZE, DISK_CAPACITY, MEMORY_CAPACITY>
{
    pub fn new(disk: &Disk<BLOCK_SIZE, DISK_CAPACITY>) -> Self {
        BufferManager {
            memory: [0; MEMORY_CAPACITY],
            disk: disk.clone(),
        }
    }

    pub fn get_page(&mut self, page_number: usize) -> page::Page<'a, BLOCK_SIZE> {
        todo!()
    }
}
