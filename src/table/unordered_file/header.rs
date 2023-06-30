use std::mem::size_of;

use ltp_rust_db_page::page::Page;

pub struct FileHeader {}

impl FileHeader {
    pub const fn size() -> usize {
        0
    }

    pub fn new() -> Self {
        todo!()
    }

    pub fn read_from(page: Page) -> Self {
        todo!()
    }

    pub fn write_to(&self, page: Page) {
        todo!()
    }
}

pub struct FilePageHeader {
    pub free_space_start: u32,
    pub next: u32,
}

impl FilePageHeader {
    pub fn new() -> Self {
        todo!()
    }

    pub const fn size() -> usize {
        size_of::<u32>() * 2
    }

    pub fn read_from(first_page: bool, page: Page) -> Self {
        let offset = if first_page { FileHeader::size() } else { 0 };
        unsafe {
            let free_space_start = page.read_val_at(offset);
            let next = page.read_val_at(offset + size_of::<u32>());
            Self {
                free_space_start,
                next,
            }
        }
    }

    pub fn write_to(&self, first_page: bool, disk: Page) {
        let offset = if first_page { FileHeader::size() } else { 0 };
        unsafe {
            disk.write_val_at(offset, self.free_space_start);
            disk.write_val_at(offset + size_of::<u32>(), self.next);
        }
    }
}

