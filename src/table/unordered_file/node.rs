use std::{
    mem::size_of,
    sync::{Arc, Mutex},
};

use ltp_rust_db_page::{page::PAGE_SIZE, pager::Pager};

use crate::table::{unordered_file::header::FilePageHeader, cell::Cell};

use super::header::FileHeader;

pub enum InsertResult {
    Normal,
    Spill(usize),
    OutOfSpace,
}

pub struct Node {
    is_head: bool,
    page_num: u32,
    pager: Arc<Mutex<Pager>>,
}

#[derive(Debug)]
pub enum ReadResult {
    Normal(Cell),
    Partial(Vec<u8>, usize),
}

impl Node {
    pub fn new(is_head: bool, page_num: u32, pager: Arc<Mutex<Pager>>) -> Self {
        Self {
            is_head,
            page_num,
            pager,
        }
    }

    pub fn init(is_head: bool, page_num: u32, pager: Arc<Mutex<Pager>>) -> Self {
        let pager_clone = pager.clone();
        let mut pager = pager.lock().unwrap();
        let page = pager.get_page(page_num as usize).unwrap();
        let header_size = if is_head {
            FilePageHeader::size() + FileHeader::size()
        } else {
            FilePageHeader::size()
        };
        let header = FilePageHeader::new(0, header_size as u32);
        header.write_to(is_head, page);
        Self {
            is_head,
            page_num,
            pager: pager_clone,
        }
    }

    pub fn free_start(&self) -> u32 {
        let mut pager = self.pager.lock().unwrap();
        let page = pager.get_page(self.page_num as usize).unwrap();
        let header = FilePageHeader::read_from(self.is_head, page);
        header.free_space_start
    }

    pub fn set_free_start(&mut self, free_start: u32) {
        let mut pager = self.pager.lock().unwrap();
        let page = pager.get_page(self.page_num as usize).unwrap();
        let mut header = FilePageHeader::read_from(self.is_head, page.clone());
        header.free_space_start = free_start;
        header.write_to(self.is_head, page);
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
        let payload_len = page.read_val_at::<u32>(start) - size_of::<u32>() as u32;
        println!("start: {}, len: {}", start, payload_len);
        if payload_len as usize + start < PAGE_SIZE - size_of::<u32>() {
            let buf = page
                .read_buf_at(start + size_of::<u32>(), payload_len as usize)
                .to_vec();
            ReadResult::Normal(Cell { buf })
        } else {
            let keep = PAGE_SIZE - start - size_of::<u32>();
            let buf = page
                .read_buf_at(start + size_of::<u32>(), keep as usize)
                .to_vec();

            let remain = payload_len as usize - keep;
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
        let offset = size_of::<FilePageHeader>();

        let start = offset;
        page.write_buf_at(start as usize, spilled);
        let page_header = FilePageHeader {
            free_space_start: (offset + spilled.len()) as u32,
            next: 0,
        };
        page_header.write_to(false, page.clone());
    }

    pub fn insert(&mut self, record: &Cell) -> InsertResult {
        let mut pager = self.pager.lock().unwrap();
        let page = pager.get_page(self.page_num as usize).unwrap();
        let header = FilePageHeader::read_from(self.is_head, page.clone());
        let offset = header.free_space_start;

        let start = offset as usize;
        let end = start + record.size();
        println!("start: {}, end: {}", start, end);

        if record.buf.len() >= PAGE_SIZE - 12 {
            panic!(
                "record too large, must be less than {} bytes",
                PAGE_SIZE - 12
            );
        }
        if start >= PAGE_SIZE - 4 {
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
        } else {
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
            drop(pager);
            self.set_free_start(PAGE_SIZE as u32);
            return InsertResult::Spill(kept_size);
        }
    }
}

#[cfg(test)]
mod tests {
    use std::{
        fs::remove_file,
        sync::{Arc, Mutex},
    };

    use ltp_rust_db_page::{page::PAGE_SIZE, pager::Pager};

    use crate::table::{unordered_file::{node::Node, header::FilePageHeader}, cell::Cell};

    use super::{InsertResult, ReadResult};

    #[test]
    fn next() {
        let pager = Arc::new(Mutex::new(Pager::init("node_next")));
        let pager_clone = pager.clone();
        let mut pager = pager.lock().unwrap();
        let new_page = pager.get_free_page().unwrap();
        let new_page2 = pager.get_free_page().unwrap();
        drop(pager);
        let mut root = Node::new(true, new_page as u32, pager_clone.clone());

        root.set_next(new_page2 as u32);
        assert_eq!(root.next(), Some(new_page2 as u32));
        remove_file("node_next").unwrap();
    }

    #[test]
    fn insert() {
        let pager = Arc::new(Mutex::new(Pager::init("node_next")));
        let pager_clone = pager.clone();
        let mut pager = pager.lock().unwrap();
        let new_page = pager.get_free_page().unwrap();
        drop(pager);
        let mut root = Node::init(true, new_page as u32, pager_clone.clone());
        root.insert(&Cell::new(vec![1, 2, 3]));
        assert_eq!(root.free_start(), FilePageHeader::size() as u32 + 4 + 3);
        let rs = unsafe { root.read_record_at(8) };
        match rs {
            ReadResult::Normal(record) => {
                assert_eq!(record.buf, vec![1, 2, 3]);
            }
            ReadResult::Partial(_, _) => {
                panic!("should be normal");
            }
        }
    }

    #[test]
    fn insert_and_spilled() {
        let pager = Arc::new(Mutex::new(Pager::init("node_next")));
        let pager_clone = pager.clone();
        let mut pager = pager.lock().unwrap();
        let new_page = pager.get_free_page().unwrap();
        drop(pager);
        let mut root = Node::init(true, new_page as u32, pager_clone.clone());
        let buf = vec![0xa; 200];
        root.insert(&Cell::new(buf));
        let start1 = root.free_start();
        let buf = vec![0xff; 4083];
        let rs = root.insert(&Cell::new(buf));
        assert_eq!(root.free_start(), PAGE_SIZE as u32);
        match rs {
            InsertResult::Spill(start_remain) => {
                assert_eq!(start_remain, 3880);
                let rs = unsafe { root.read_record_at(start1 as usize) };
                match rs {
                    ReadResult::Partial(mut initial, remain) => {
                        let start = 4083 - remain;
                        assert_eq!(initial.len(), 3880);
                        assert_eq!(start, 3880);
                        let buf = vec![0xff; 4083];
                        initial.extend(buf.clone()[start..].iter());
                        assert_eq!(initial, buf);
                    }
                    _ => {
                        panic!("should be partial");
                    }
                }
            }
            _ => {
                panic!("should be spilled");
            }
        }
    }

    #[test]
    fn insert_spilled() {
        let pager = Arc::new(Mutex::new(Pager::init("node_next")));
        let pager_clone = pager.clone();
        let mut pager = pager.lock().unwrap();
        let new_page = pager.get_free_page().unwrap();
        drop(pager);

        let mut node = Node::init(false, new_page as u32, pager_clone.clone());

        let buf = vec![0xa; 200];
        node.insert_spilled(&buf);
        let page = pager_clone
            .lock()
            .unwrap()
            .get_page(new_page as usize)
            .unwrap();
        let read_buf = page.read_buf_at(8, 200);
        assert_eq!(node.free_start(), 208);
        assert_eq!(read_buf, buf);

        let buf = node.get_partial_record(200);
        assert_eq!(buf, vec![0xa; 200]);
    }
}
