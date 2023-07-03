use std::mem::size_of;

use ltp_rust_db_page::page::Page;

pub struct FileHeader {
    pub cell_count: u64,
    pub head_page_num: u32,
    pub tail_page_num: u32,
}

impl FileHeader {
    pub const fn size() -> usize {
        size_of::<u64>() + size_of::<u32>() + size_of::<u64>()
    }

    pub fn read_from(page: Page) -> Self {
        let offset = 0;
        unsafe {
            let cell_count = page.read_val_at::<u64>(offset);
            let head_page_num = page.read_val_at::<u32>(offset + size_of::<u64>());
            let tail_page_num = page.read_val_at::<u32>(offset + size_of::<u64>() + size_of::<u32>());
            Self {
                cell_count,
                head_page_num,
                tail_page_num,
            }
        }
    }

    pub fn write_to(&self, page: Page) {
        let offset = 0;
        unsafe {
            page.write_val_at::<u64>(offset, self.cell_count);
            page.write_val_at::<u32>(offset + size_of::<u64>(), self.head_page_num);
            page.write_val_at::<u32>(
                offset + size_of::<u64>() + size_of::<u32>(),
                self.tail_page_num,
            );
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
