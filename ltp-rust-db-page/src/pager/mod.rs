#[cfg(test)]
mod tests;

const TABLE_MAX_PAGES: usize = 1000;
use std::{
    fs::File,
    io::{Read, Seek, Write},
};

use crate::page::{Page, PAGE_SIZE};

#[derive(Debug)]
pub struct Pager {
    file: File,

    pages_num: usize,
    // Pages in memory
    pages: Vec<Option<Page>>,
}

impl Pager {
    pub fn init(filename: &str) -> Self {
        let mut file = File::options()
            .write(true)
            .create(true)
            .read(true)
            .open(filename)
            .unwrap();
        let mut pages = Vec::new();
        let file_length = file.seek(std::io::SeekFrom::End(0)).unwrap();
        file.seek(std::io::SeekFrom::Start(0)).unwrap();
        let pages_in_file = file_length / PAGE_SIZE as u64;

        for _ in 0..pages_in_file {
            let mut new_page = Page::init();
            let mut buf = vec![];
            file.read_exact(&mut buf).unwrap();
            new_page.write_buf_at(0, &buf);
            pages.push(Some(new_page));
        }

        for _ in pages_in_file..TABLE_MAX_PAGES as u64 {
            pages.push(None)
        }

        Self {
            file,
            pages,
            pages_num: pages_in_file as usize,
        }
    }

    pub fn get_free_page(&mut self) -> Option<usize> {
        let rs = Some(self.pages_num);
        self.pages_num += 1;
        rs
    }

    pub fn get_free_page_mut(&mut self) -> Option<(usize, &mut Page)> {
        todo!()
    }

    pub fn get_page(&mut self, page_num: usize) -> Option<&Page> {
        // There are 4 cases that should be handled:
        // - Requested page not allowed (pass limit)
        // - Cache miss (the page needed is not in memory)
        // - The page in request is not initialized yet (in both memory and file)
        // - Page already in cache

        if page_num > TABLE_MAX_PAGES {
            return None;
        }

        let page = self.pages[page_num].get_or_insert_with(|| {
            self.pages_num += 1;
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

        Some(page)
    }

    pub fn get_page_mut(&mut self, page_num: usize) -> Option<&mut Page> {
        if page_num > TABLE_MAX_PAGES {
            return None;
        }
        let page = self.pages[page_num].get_or_insert_with(|| {
            self.pages_num += 1;
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

        Some(page)
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
