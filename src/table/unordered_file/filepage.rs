use std::{
    mem::size_of,
    sync::{Arc, Mutex},
};

use ltp_rust_db_page::{page::PAGE_SIZE, pager::Pager};

use crate::table::unordered_file::header::FilePageHeader;

use super::record::Record;

pub enum InsertResult {
    Normal,
    Spill(usize),
    OutOfSpace,
}

pub struct FilePage {
    is_head: bool,
    page_num: u32,
    pager: Arc<Mutex<Pager>>,
}

#[derive(Debug)]
pub enum ReadResult {
    Normal(Record),
    Partial(Vec<u8>, usize),
}

impl FilePage {
    pub fn new(is_head: bool, page_num: u32, pager: Arc<Mutex<Pager>>) -> Self {
        Self {
            is_head,
            page_num,
            pager,
        }
    }

    pub fn get_partial_record(&self, len: usize) -> Vec<u8> {
        let mut pager = self.pager.lock().unwrap();
        let page = pager.get_page(self.page_num as usize).unwrap();
        page.read_buf_at(FilePageHeader::size(), len).to_vec()
    }

    /// ### Safety: Must ensure that `start` is correct
    pub unsafe fn read_record_at(&self, start: usize) -> ReadResult {
        let mut pager = self.pager.lock().unwrap();
        let page = pager.get_page(self.page_num as usize).unwrap();
        let len = page.read_val_at::<u32>(start) - size_of::<u32>() as u32;
        println!("start: {}, len: {}", start, len);
        if len as usize + start < PAGE_SIZE - size_of::<u32>() {
            let buf = page
                .read_buf_at(start + size_of::<u32>(), len as usize)
                .to_vec();
            ReadResult::Normal(Record { buf })
        } else {
            let keep = PAGE_SIZE - start - size_of::<u32>();
            let buf = page
                .read_buf_at(start + size_of::<u32>(), keep as usize)
                .to_vec();
            let remain = len as usize - keep;
            println!("keep: {}", keep);
            println!("remain: {}", remain);
            ReadResult::Partial(buf, remain)
        }
    }

    pub fn set_next(&mut self, next: u32) {
        let mut pager = self.pager.lock().unwrap();
        let page = pager.get_page(self.page_num as usize).unwrap();
        let header = FilePageHeader::read_from(self.is_head, page.clone());
        let page_header = FilePageHeader {
            free_space_start: header.free_space_start,
            next,
        };
        page_header.write_to(self.is_head, page);
    }

    pub fn next(&self) -> Option<u32> {
        let mut pager = self.pager.lock().unwrap();
        let page = pager.get_page(self.page_num as usize).unwrap();
        let header = FilePageHeader::read_from(self.is_head, page);
        let next = header.next;
        if next == 0 {
            return None;
        }
        Some(next)
    }

    pub fn insert_spilled(&mut self, spilled: &[u8]) {
        let mut pager = self.pager.lock().unwrap();
        let page = pager.get_page(self.page_num as usize).unwrap();
        let header = FilePageHeader::read_from(self.is_head, page.clone());
        let offset = header.free_space_start;

        let start = offset;
        page.write_buf_at(start as usize, spilled);
        let page_header = FilePageHeader {
            free_space_start: start + spilled.len() as u32,
            next: 0,
        };
        page_header.write_to(false, page.clone());
    }

    pub fn insert(&mut self, record: &Record) -> InsertResult {
        let mut pager = self.pager.lock().unwrap();
        let page = pager.get_page(self.page_num as usize).unwrap();
        let header = FilePageHeader::read_from(self.is_head, page.clone());
        let offset = header.free_space_start;

        let start = offset as usize;
        let end = start + record.size();
        println!("start: {}, end: {}", start, end);

        if record.buf.len() > PAGE_SIZE - 8 {
            panic!(
                "record too large, must be less than {} bytes",
                PAGE_SIZE - 8
            );
        }
        if start >= PAGE_SIZE - 8 {
            // create new page for the record
            todo!();
            return InsertResult::OutOfSpace;
        }
        if end < PAGE_SIZE {
            // record can be inserted in a single page
            let record_buf = record.serialize();
            unsafe {
                page.write_val_at::<u32>(start as usize, record.size() as u32);
                page.write_buf_at(start as usize + size_of::<u32>(), record_buf);
            }
            let page_header = FilePageHeader {
                free_space_start: end as u32,
                next: 0,
            };
            page_header.write_to(false, page.clone());
            return InsertResult::Normal;
        }
        // record cannot be inserted in a single page and should be spilled
        let kept_size = PAGE_SIZE - start - size_of::<u32>();
        let record_buf = record.serialize();
        unsafe {
            page.write_val_at::<u32>(start as usize, record.size() as u32);
        }
        page.write_buf_at(
            start as usize + size_of::<u32>(),
            &record_buf[..kept_size as usize],
        );
        let page_header = FilePageHeader {
            free_space_start: PAGE_SIZE as u32,
            next: 0,
        };
        page_header.write_to(self.is_head, page.clone());
        drop(pager);
        return InsertResult::Spill(record_buf.len() - kept_size as usize);
    }
}
