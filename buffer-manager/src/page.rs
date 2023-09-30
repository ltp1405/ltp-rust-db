use std::{ptr::slice_from_raw_parts_mut, ops::{Deref, DerefMut}};

use super::BufferManager;

pub struct Page<
    'a,
    const PAGE_SIZE: usize,
    const DISK_CAPACITY: usize,
    const MEMORY_CAPACITY: usize,
> {
    page_number: u32,
    frame_number: u32,
    buffer_manager: &'a BufferManager<PAGE_SIZE, DISK_CAPACITY, MEMORY_CAPACITY>,
}

impl<'a, const PAGE_SIZE: usize, const DISK_CAPACITY: usize, const MEMORY_CAPACITY: usize>
    Page<'a, PAGE_SIZE, DISK_CAPACITY, MEMORY_CAPACITY>
{
    pub(super) fn init(
        page_number: u32,
        frame_number: u32,
        buffer_manager: &'a BufferManager<PAGE_SIZE, DISK_CAPACITY, MEMORY_CAPACITY>,
    ) -> Self {
        buffer_manager.page_table.pin_page(page_number);
        Page {
            page_number,
            frame_number,
            buffer_manager,
        }
    }

    fn buffer(&self) -> &[u8] {
        let frame_number = self.frame_number as usize;
        &self.buffer_manager.memory[frame_number * PAGE_SIZE..(frame_number + 1) * PAGE_SIZE]
    }

    fn buffer_mut(&self) -> &mut [u8] {
        let frame_number = self.frame_number as usize;
        let buffer_ptr = unsafe {
            self.buffer_manager
                .memory
                .as_ptr()
                .add(frame_number * PAGE_SIZE) as *mut u8
        };
        let s = slice_from_raw_parts_mut(buffer_ptr, PAGE_SIZE);
        unsafe { s.as_mut().unwrap() }
    }
}

impl<'a, const PAGE_SIZE: usize, const DISK_CAPACITY: usize, const MEMORY_CAPACITY: usize> Deref
    for Page<'a, PAGE_SIZE, DISK_CAPACITY, MEMORY_CAPACITY>
{
    type Target = [u8];

    fn deref(&self) -> &Self::Target {
        self.buffer()
    }
}

impl<'a, const PAGE_SIZE: usize, const DISK_CAPACITY: usize, const MEMORY_CAPACITY: usize> DerefMut
    for Page<'a, PAGE_SIZE, DISK_CAPACITY, MEMORY_CAPACITY>
{
    fn deref_mut(&mut self) -> &mut Self::Target {
        self.buffer_manager.page_table.set_dirty(self.page_number);
        self.buffer_mut()
    }
}

impl<'a, const PAGE_SIZE: usize, const DISK_CAPACITY: usize, const MEMORY_CAPACITY: usize> Drop
    for Page<'a, PAGE_SIZE, DISK_CAPACITY, MEMORY_CAPACITY>
{
    fn drop(&mut self) {
        self.buffer_manager.page_table.drop_page(self.page_number);
    }
}
