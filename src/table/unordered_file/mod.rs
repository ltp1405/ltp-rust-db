mod header;
mod node;
pub mod cell;

use std::sync::{Arc, Mutex};

use ltp_rust_db_page::page::PAGE_SIZE;
use ltp_rust_db_page::pager::Pager;

use self::header::{FileHeader, FilePageHeader};
use self::node::{InsertResult, Node, ReadResult};
use self::cell::Cell;

pub struct Cursor {
    pager: Arc<Mutex<Pager>>,
    page_num: u32,
    offset: usize,
    at_head: bool,
}

impl Cursor {
    fn new(first_page_num: u32, pager: Arc<Mutex<Pager>>) -> Self {
        Self {
            pager,
            page_num: first_page_num,
            offset: FilePageHeader::size() + FileHeader::size(),
            at_head: true,
        }
    }

    pub fn read(&mut self) -> Cell {
        let filepage = Node::new(self.at_head, self.page_num, self.pager.clone());
        let rs = unsafe { filepage.read_record_at(self.offset) };
        match rs {
            ReadResult::Normal(record) => record,
            ReadResult::Partial(mut initial, remain) => {
                let page = filepage.next().unwrap();
                drop(filepage);
                let filepage = Node::new(self.at_head, page, self.pager.clone());
                let remain = filepage.get_partial_record(remain);
                println!("remain: {:?}", remain);
                initial.extend(remain);
                Cell::new(initial)
            }
        }
    }

    pub fn next(&mut self) {
        let mut pager = self.pager.lock().unwrap();
        let page = pager.get_page(self.page_num as usize).unwrap();
        let len = unsafe { page.read_val_at::<u32>(self.offset) };
        let next_offset = self.offset + len as usize;
        if next_offset < PAGE_SIZE {
            self.offset = next_offset;
        } else {
            let page_header = FilePageHeader::read_from(false, page.clone());
            self.page_num = page_header.next;
            if self.page_num == 0 {
                panic!("No next page");
            }
            println!("next_offset: {}", next_offset);
            self.offset = next_offset - PAGE_SIZE + FilePageHeader::size();
            self.at_head = false;
        }
        println!("move to offset: {} - page: {}", self.offset, self.page_num);
    }
}

/// A `File` which only contain records from one `Table`
pub struct File {
    pager: Arc<Mutex<Pager>>,
    pub first_page_num: u32,
}

impl File {
    pub fn init(pager: Arc<Mutex<Pager>>) -> Self {
        let page_num = pager.lock().unwrap().get_free_page().unwrap();
        let file = File {
            first_page_num: page_num as u32,
            pager,
        };
        {
            let mut pager = file.pager.lock().unwrap();
            let page = pager.get_page(page_num).unwrap();
            let page_header = FilePageHeader {
                free_space_start: FilePageHeader::size() as u32,
                next: 0,
            };
            page_header.write_to(true, page.clone());
        }
        file
    }

    pub fn cursor(&self) -> Cursor {
        Cursor::new(self.first_page_num, self.pager.clone())
    }

    pub fn insert(&mut self, record: Cell) {
        // Traverse to the last page
        // If the last page is full, allocate a new page
        // Write the record to the last page
        let mut node = Node::new(true, self.first_page_num, self.pager.clone());
        let mut next = node.next();
        let mut first_page = true;
        loop {
            match next {
                Some(page_num) => {
                    node = Node::new(false, page_num, self.pager.clone());
                    next = node.next();
                    first_page = false;
                }
                None => break,
            }
        }
        let rs = node.insert(&record);
        match rs {
            InsertResult::Spill(remain_start) => {
                let new_page = self.pager.lock().unwrap().get_free_page().unwrap();
                let mut new_node = Node::init(first_page, new_page as u32, self.pager.clone());
                let spilled_record = &record.buf[remain_start..];
                new_node.insert_spilled(&spilled_record);

                node.set_next(new_page as u32);
            }
            InsertResult::OutOfSpace => {
                panic!("Out of space");
                let new_page = self.pager.lock().unwrap().get_free_page().unwrap();
                let mut new_filepage = Node::init(true, new_page as u32, self.pager.clone());
                new_filepage.insert(&record);
            }
            _ => {}
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs::remove_file;
    use std::mem::size_of;

    #[test]
    fn simple_read() {
        let pager = Arc::new(Mutex::new(Pager::init("test_simple_read")));
        let mut file = File::init(pager);
        let record = Cell::new(vec![1, 2, 3]);
        file.insert(record);
        let mut cursor = file.cursor();
        let record = cursor.read();
        assert_eq!(record.buf, vec![1, 2, 3]);
        remove_file("test_simple_read").unwrap();
    }

    #[test]
    fn complete_read() {
        let pager = Arc::new(Mutex::new(Pager::init("test_complete_read")));
        let mut file = File::init(pager);
        let record = Cell::new([1; 2000].to_vec());
        file.insert(record);
        let record2 = Cell::new([2; 2500].to_vec());
        file.insert(record2);
        let record3 = Cell::new([3; 3000].to_vec());
        file.insert(record3);
        let record4 = Cell::new([4; 3500].to_vec());
        file.insert(record4);
        let record5 = Cell::new([5; 4000].to_vec());
        file.insert(record5);
        let mut cursor = file.cursor();
        assert_eq!(cursor.offset, 8);
        let record = cursor.read();
        assert_eq!(record.buf, vec![1; 2000]);

        cursor.next();
        assert_eq!(cursor.offset, 2000 + 12);
        let record = cursor.read();
        assert_eq!(record.size(), 2504);
        assert_eq!(record.buf, vec![2; 2500]);

        cursor.next();
        assert_eq!(cursor.offset, 428);
        let record = cursor.read();
        assert_eq!(record.buf.len(), 3000);
        assert_eq!(record.buf, vec![3; 3000]);

        cursor.next();
        assert_eq!(cursor.offset, 3432);
        let record = cursor.read();
        assert_eq!(record.buf.len(), 3500);
        assert_eq!(record.buf, vec![4; 3500]);

        cursor.next();
        assert_eq!(cursor.offset, 2848);
        let record = cursor.read();
        assert_eq!(record.buf.len(), 4000);
        assert_eq!(record.buf, vec![5; 4000]);

        remove_file("test_complete_read").unwrap();
    }

    #[test]
    fn simple_insert() {
        let pager = Arc::new(Mutex::new(Pager::init("test_simple_insert")));
        let mut file = File::init(pager);
        let record = Cell::new(vec![1, 2, 3]);
        file.insert(record);
        let record2 = Cell::new(vec![4, 5, 6, 7, 8, 9]);
        file.insert(record2);

        let page = file.pager.lock().unwrap().get_page(0).unwrap();
        let len = unsafe { page.read_val_at::<u32>(FileHeader::size() + FilePageHeader::size()) };
        assert_eq!(len, 4 + 3);
        let buf = page.read_buf_at(
            FilePageHeader::size() + FileHeader::size() + size_of::<u32>(),
            3,
        );
        assert_eq!(buf, vec![1, 2, 3]);
        let buf = page.read_buf_at(
            FilePageHeader::size() + FileHeader::size() + size_of::<u32>() * 2 + 3,
            6,
        );
        let len = unsafe {
            page.read_val_at::<u32>(
                FileHeader::size() + FilePageHeader::size() + size_of::<u32>() + 3,
            )
        };
        assert_eq!(len, 4 + 6);
        assert_eq!(buf, vec![4, 5, 6, 7, 8, 9]);
        remove_file("test_simple_insert").unwrap();
    }

    #[test]
    fn complete_insert() {
        let pager = Arc::new(Mutex::new(Pager::init("test_complete_insert")));
        let mut file = File::init(pager);
        let record = Cell::new([1; 2000].to_vec());
        file.insert(record);
        let record2 = Cell::new([2; 2500].to_vec());
        file.insert(record2);
        let record3 = Cell::new([3; 3000].to_vec());
        file.insert(record3);
        let record4 = Cell::new([4; 3500].to_vec());
        file.insert(record4);
        let record5 = Cell::new([5; 4000].to_vec());
        file.insert(record5);
        let page = file
            .pager
            .lock()
            .unwrap()
            .get_page(file.first_page_num as usize)
            .unwrap();
        let buf = page.read_buf_at(
            FilePageHeader::size() + FileHeader::size() + size_of::<u32>(),
            2000,
        );
        assert_eq!(buf, vec![1; 2000]);
        let offset = FilePageHeader::size() + FileHeader::size() + size_of::<u32>() + 2000;
        let len = unsafe { page.read_val_at::<u32>(offset) };
        let buf = page.read_buf_at(
            offset + size_of::<u32>(),
            PAGE_SIZE - offset - size_of::<u32>(),
        );
        assert_eq!(len, 2504);
        assert_eq!(buf, vec![2; PAGE_SIZE - offset - size_of::<u32>()]);
        remove_file("test_complete_insert").unwrap();
    }
}
