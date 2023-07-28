mod page;

use std::sync::{Arc, Mutex};

use disk::Disk;

use self::page::FrameAllocator;

pub use page::Page;
pub use page::PageTable;


pub struct BufferManager<
    'a,
    const BLOCK_SIZE: usize,
    const DISK_CAPACITY: usize,
    const MEMORY_CAPACITY: usize,
> {
    frame_allocator: Arc<Mutex<FrameAllocator<'a, BLOCK_SIZE, MEMORY_CAPACITY>>>,
    memory: &'a [u8],
    disk: Disk<BLOCK_SIZE, DISK_CAPACITY>,
}

impl<'a, const BLOCK_SIZE: usize, const DISK_CAPACITY: usize, const MEMORY_CAPACITY: usize>
    BufferManager<'a, BLOCK_SIZE, DISK_CAPACITY, MEMORY_CAPACITY>
{
    pub fn init(memory: &'a [u8], disk: &Disk<BLOCK_SIZE, DISK_CAPACITY>) -> Self {
        let frame_allocator = Arc::new(Mutex::new(FrameAllocator::init(memory)));
        BufferManager {
            frame_allocator,
            memory: &memory,
            disk: disk.clone(),
        }
    }

    pub fn get_page(&'a self, page_number: u32) -> page::Page<'a, BLOCK_SIZE> {
        match PageTable::read(&self.memory).get_page(page_number) {
            Some(page) => page,
            None => {
                let frame = unsafe {
                    self.frame_allocator
                        .lock()
                        .unwrap()
                        .allocate_frame()
                        .expect("Implement page replacement")
                };
                println!("Allocated frame: {}", frame);
                PageTable::read(&self.memory).map_to_frame(page_number, frame);
                let page = PageTable::read(&self.memory).get_page(page_number).unwrap();
                page
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::BufferManager;
    const BLOCK_SIZE: usize = 4096;
    const DISK_CAPACITY: usize = 4096 * 32;
    const MEMORY_CAPACITY: usize = 4096 * 16;

    #[test]
    fn simple_get_page() {
        let memory = [0u8; 4096 * 16];
        let disk = disk::Disk::<BLOCK_SIZE, DISK_CAPACITY>::create("simple_get_page").unwrap();
        let buffer_manager: BufferManager<'_, 4096, DISK_CAPACITY, MEMORY_CAPACITY> =
            BufferManager::init(&memory, &disk);
        let _page1 = buffer_manager.get_page(5);
        let _page2 = buffer_manager.get_page(14);
    }

    #[test]
    fn get_lots_of_pages() {
        let memory = [0u8; 4096 * 16];
        let disk = disk::Disk::<BLOCK_SIZE, DISK_CAPACITY>::create("simple_get_page").unwrap();
        let buffer_manager: BufferManager<'_, 4096, DISK_CAPACITY, MEMORY_CAPACITY> =
            BufferManager::init(&memory, &disk);
        let _page1 = buffer_manager.get_page(5);
        let _page2 = buffer_manager.get_page(14);
    }
}
