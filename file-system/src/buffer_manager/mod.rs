mod page_table;
mod page;

use std::sync::{Arc, Mutex};

use disk::Disk;

use crate::frame_allocator::FrameAllocator;
pub use page_table::PageTable;
pub use page::Page;

pub struct BufferManager<
    'a,
    const BLOCK_SIZE: usize,
    const DISK_CAPACITY: usize,
    const MEMORY_CAPACITY: usize,
> {
    page_table: PageTable<BLOCK_SIZE, DISK_CAPACITY>,
    frame_allocator: Arc<Mutex<FrameAllocator<BLOCK_SIZE, MEMORY_CAPACITY>>>,
    memory: &'a [u8],
    disk: Disk<BLOCK_SIZE, DISK_CAPACITY>,
}

impl<'a, const BLOCK_SIZE: usize, const DISK_CAPACITY: usize, const MEMORY_CAPACITY: usize>
    BufferManager<'a, BLOCK_SIZE, DISK_CAPACITY, MEMORY_CAPACITY>
{
    pub fn init(memory: &'a [u8], disk: &Disk<BLOCK_SIZE, DISK_CAPACITY>) -> Self {
        let frame_allocator = Arc::new(Mutex::new(FrameAllocator::init()));
        let page_table = PageTable::init();
        BufferManager {
            page_table,
            frame_allocator,
            memory: &memory,
            disk: disk.clone(),
        }
    }

    pub fn save_page(&'a self, page_number: u32) -> Result<(), String> {
        if self.page_table.is_pinned(page_number).unwrap() {
            return Err(format!("Page {} is pinned", page_number).to_string());
        }
        if self.page_table.is_dirty(page_number).unwrap() {
            let frame_number = self.page_table.get_frame(page_number).unwrap();
            let page = Page::init(page_number, frame_number, self);
            self.disk.write_block(page_number as usize, &page).unwrap();
        }
        Ok(())
    }

    pub fn get_page(
        &'a self,
        page_number: u32,
    ) -> Page<'a, BLOCK_SIZE, DISK_CAPACITY, MEMORY_CAPACITY> {
        match self.page_table.get_frame(page_number) {
            Some(frame) => {
                let page = Page::init(page_number, frame, self);
                page
            }
            None => {
                let new_frame = unsafe { self.frame_allocator.lock().unwrap().allocate_frame() };
                let frame = match new_frame {
                    Some(frame) => {
                        assert!((frame as usize) < MEMORY_CAPACITY / BLOCK_SIZE);
                        log::info!("New frame allocated: {}", frame);
                        frame
                    }
                    None => {
                        let page_to_evict = self.page_table.get_oldest_page().unwrap();
                        log::info!("Evicting page {}", page_to_evict);
                        let frame_to_evict = self.page_table.get_frame(page_to_evict).unwrap();
                        self.page_table.unmap_page(page_to_evict);
                        log::info!("Page {} unmapped", page_to_evict);
                        frame_to_evict
                    }
                };
                unsafe {
                    let mut memory_ptr = self.memory.as_ptr() as *mut u8;
                    memory_ptr = memory_ptr.add(frame as usize * BLOCK_SIZE);
                    let memory_slice = std::slice::from_raw_parts_mut(memory_ptr, BLOCK_SIZE);
                    let data = self.disk.read_block(page_number as usize).unwrap();
                    memory_slice.copy_from_slice(data.as_slice());
                }
                self.page_table.map_to_frame(page_number, frame);
                log::info!("Page {} mapped to frame {}", page_number, frame);
                let frame = self.page_table.get_frame(page_number).unwrap();
                let page = Page::init(page_number, frame, self);
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
        // let _page2 = buffer_manager.get_page(14);
    }

    #[test]
    fn get_lots_of_pages() {
        let memory = [0u8; 4096 * 16];
        let disk = disk::Disk::<BLOCK_SIZE, DISK_CAPACITY>::create("get_lots_of_pages").unwrap();
        let buffer_manager: BufferManager<'_, 4096, DISK_CAPACITY, MEMORY_CAPACITY> =
            BufferManager::init(&memory, &disk);
        let _page1 = buffer_manager.get_page(5);
        let _page2 = buffer_manager.get_page(14);
    }
}
