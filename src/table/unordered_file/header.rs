use std::mem::size_of;

use ltp_rust_db_page::page::Page;

pub struct FileHeader {
    pub cell_count: u64,
}

impl FileHeader {
    pub const fn size() -> usize {
        size_of::<u64>()
    }

    pub fn read_from(page: Page) -> Self {
        let offset = 0;
        unsafe {
            let cell_count = page.read_val_at::<u64>(offset);
            Self { cell_count }
        }
    }

    pub fn write_to(&self, page: Page) {
        let offset = 0;
        unsafe {
            page.write_val_at::<u64>(offset, self.cell_count);
        }
    }
}

pub struct FilePageHeader {
    pub free_space_start: u32,
    pub next: u32,
}

impl FilePageHeader {
    pub fn new(next: u32, free_space_start: u32) -> Self {
        Self {
            free_space_start,
            next,
        }
    }

    pub const fn size() -> usize {
        size_of::<u32>() * 2
    }

    pub fn read_from(is_head: bool, page: Page) -> Self {
        let offset = if is_head { FileHeader::size() } else { 0 };
        unsafe {
            let free_space_start = page.read_val_at(offset);
            let next = page.read_val_at(offset + size_of::<u32>());
            Self {
                free_space_start,
                next,
            }
        }
    }

    pub fn write_to(&self, is_head: bool, page: Page) {
        let offset = if is_head { FileHeader::size() } else { 0 };
        unsafe {
            page.write_val_at(offset, self.free_space_start);
            page.write_val_at(offset + size_of::<u32>(), self.next);
        }
    }
}
