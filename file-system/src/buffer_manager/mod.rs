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
    pub fn page_table_pages_required() -> usize {
        10 * (MEMORY_CAPACITY / BLOCK_SIZE) / BLOCK_SIZE
    }

    pub fn frame_bitmap_pages_required() -> usize {
        FrameAllocator::<MEMORY_CAPACITY, BLOCK_SIZE>::size()
    }

    pub fn init(memory: &'a [u8], disk: &Disk<BLOCK_SIZE, DISK_CAPACITY>) -> Self {
        let page_table_size = Self::page_table_pages_required();
        let frame_allocator = Arc::new(Mutex::new(FrameAllocator::init(memory)));
        unsafe {
            for _ in 0..page_table_size / BLOCK_SIZE {
                frame_allocator.lock().unwrap().allocate_frame();
            }
        }
        BufferManager {
            frame_allocator,
            memory: &memory,
            disk: disk.clone(),
        }
    }

    pub fn save_page(&'a self, page_number: u32) -> Result<(), &'static str> {
        if PageTable::read(&self.memory)
            .is_pinned(page_number)
            .unwrap()
        {
            return Err("Page is pinned");
        }
        let page: Page<BLOCK_SIZE> = PageTable::read(&self.memory).get_page(page_number).unwrap();
        if PageTable::read(&self.memory).is_dirty(page_number).unwrap() {
            self.disk.write_block(page_number as usize, &page).unwrap();
        }
        Ok(())
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
                unsafe {
                    let mut memory_ptr = self.memory.as_ptr() as *mut u8;
                    memory_ptr = memory_ptr.add(frame as usize * BLOCK_SIZE);
                    let memory_slice = std::slice::from_raw_parts_mut(memory_ptr, BLOCK_SIZE);
                    let data = self.disk.read_block(page_number as usize).unwrap();
                    memory_slice.copy_from_slice(data.as_slice());
                }
                PageTable::read(&self.memory).map_to_frame(page_number, frame);
                let page = PageTable::read(&self.memory).get_page(page_number).unwrap();
                page
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::buffer_manager::PageTable;

    use super::BufferManager;
    const BLOCK_SIZE: usize = 4096;
    const DISK_CAPACITY: usize = 4096 * 32;
    const MEMORY_CAPACITY: usize = 4096 * 16;

    #[test]
    fn write_reload() {
        let disk = disk::Disk::<BLOCK_SIZE, DISK_CAPACITY>::create("write_reload").unwrap();
        {
            let memory = [0u8; 4096 * 16];
            let buffer_manager: BufferManager<'_, 4096, DISK_CAPACITY, MEMORY_CAPACITY> =
                BufferManager::init(&memory, &disk);
            let mut page1 = buffer_manager.get_page(5);
            page1.copy_from_slice(&[1u8; 4096]);
            let mut page2 = buffer_manager.get_page(14);
            page2.copy_from_slice(&[2u8; 4096]);
            drop(page1);
            drop(page2);
            buffer_manager.save_page(5).unwrap();
            buffer_manager.save_page(14).unwrap();
        }
        {
            let memory = [0u8; 4096 * 16];
            let buffer_manager: BufferManager<'_, 4096, DISK_CAPACITY, MEMORY_CAPACITY> =
                BufferManager::init(&memory, &disk);
            let page1 = buffer_manager.get_page(5);
            println!(
                "{:?}",
                PageTable::read(&buffer_manager.memory).get_frame_number(5)
            );
            assert_eq!(page1[0], 1u8);
            let page2 = buffer_manager.get_page(14);
            assert_eq!(page2[0], 2u8);
        }
    }

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
