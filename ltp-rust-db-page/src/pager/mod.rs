mod page_table;

#[cfg(test)]
mod tests;

const TABLE_MAX_PAGES: usize = 1000;
use std::{
    fs::File,
    io::{Read, Seek, Write},
};

use crate::page::{Page, PAGE_SIZE};

use self::page_table::PageTable;

pub struct Pager {
    file: File,

    frames_num: usize,
    // Pages in memory
    pages: Vec<Option<Page>>,
    page_table: PageTable,
}

impl Pager {
    pub fn init(filename: &str) -> Self {
        let mut file = File::options()
            .write(true)
            .create(true)
            .read(true)
            .open(filename)
            .unwrap();
        file.set_len(TABLE_MAX_PAGES as u64 * PAGE_SIZE as u64)
            .unwrap();
        let mut pages = Vec::new();
        file.seek(std::io::SeekFrom::Start(0)).unwrap();
        let page_table = PageTable::init();
        pages.resize_with(TABLE_MAX_PAGES, || None);
        Self { file, frames_num:  pages: (), page_table: () }
    }

    pub fn get_free_page(&mut self) -> Option<usize> {
        let rs = Some(self.frames_num);
        self.frames_num += 1;
        rs
    }

    pub fn get_page(&mut self, page_num: usize) -> Option<Page> {
        // There are 4 cases that should be handled:
        // - Requested page not allowed (pass limit)
        // - Cache miss (the page needed is not in memory)
        // - The page in request is not initialized yet (in both memory and file)
        // - Page already in cache

        if page_num > TABLE_MAX_PAGES {
            return None;
        }

        let page = self.pages[page_num].get_or_insert_with(|| {
            self.frames_num += 1;
            Page::init()
        });
        let file_length = self.file.seek(std::io::SeekFrom::End(0)).unwrap();
        // Page not initialized yet
        let page = if (page_num + 1) * PAGE_SIZE > file_length as usize {
            page
        } else {
            self.file
                .seek(std::io::SeekFrom::Start((PAGE_SIZE * page_num) as u64))
                .unwrap();
            let mut buf = vec![];
            self.file.read_exact(&mut buf).unwrap();
            page.write_buf_at(0, &buf);
            page
        };

        Some(page.clone())
    }

    pub fn get_page_mut(&mut self, page_num: usize) -> Option<Page> {
        if page_num > TABLE_MAX_PAGES {
            return None;
        }
        let page = self.pages[page_num].get_or_insert_with(|| {
            self.frames_num += 1;
            Page::init()
        });
        let file_length = self.file.seek(std::io::SeekFrom::End(0)).unwrap();
        // Page not initialized yet
        let page = if (page_num + 1) * PAGE_SIZE > file_length as usize {
            page
        } else {
            self.file
                .seek(std::io::SeekFrom::Start((PAGE_SIZE * page_num) as u64))
                .unwrap();
            let mut buf = vec![];
            self.file.read_exact(&mut buf).unwrap();
            page.write_buf_at(0, &buf);
            page
        };

        Some(page.clone())
    }

    pub fn flush(&mut self) {
        for (page_num, page) in self.pages.iter().filter(|e| e.is_some()).enumerate() {
            self.file
                .seek(std::io::SeekFrom::Start((page_num * PAGE_SIZE) as u64))
                .unwrap();
            self.file
                .write(page.as_ref().unwrap().read_buf_at(0, PAGE_SIZE))
                .unwrap();
        }
    }
}

impl Drop for Pager {
    fn drop(&mut self) {
        self.flush();
    }
}
