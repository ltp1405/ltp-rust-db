mod filepage;
mod header;
pub mod record;

use std::mem::size_of;
use std::sync::{Arc, Mutex};

use ltp_rust_db_page::page::{Page, PAGE_SIZE};
use ltp_rust_db_page::pager::Pager;

use self::filepage::{FilePage, InsertResult, ReadResult};
use self::header::{FileHeader, FilePageHeader};
use self::record::Record;

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

    pub fn read(&mut self) -> Record {
        let filepage = FilePage::new(self.at_head, self.page_num, self.pager.clone());
        let rs = unsafe { filepage.read_record_at(self.offset) };
        match rs {
            ReadResult::Normal(record) => record,
            ReadResult::Partial(mut initial, remain) => {
                let page = filepage.next().unwrap();
                drop(filepage);
                let filepage = FilePage::new(self.at_head, page, self.pager.clone());
                let remain = filepage.get_partial_record(remain);
                println!("remain: {:?}", remain);
                panic!();
                initial.extend(remain);
                Record::new(initial)
            }
        }
    }

    pub fn next(&mut self) {
        let mut pager = self.pager.lock().unwrap();
        let page = pager.get_page(self.page_num as usize).unwrap();
        let len = unsafe { page.read_val_at::<u32>(self.offset) };
        println!("current offset: {}", self.offset);
        println!("len: {}", len);
        let next_offset = self.offset + len as usize;
        println!("next_offset: {}", next_offset);
        if next_offset < PAGE_SIZE - size_of::<u32>() {
            self.offset = next_offset;
        } else {
            let page_header = FilePageHeader::read_from(true, page.clone());
            self.page_num = page_header.next;
            self.offset = next_offset - (PAGE_SIZE - FileHeader::size());
            self.at_head = false;
        }
    }
}

/// A `File` which only contain records from one `Table`
pub struct File {
    pager: Arc<Mutex<Pager>>,
    pub first_page_num: u32,
}

impl File {
    pub fn init(page_num: usize, pager: Arc<Mutex<Pager>>) -> Self {
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

    pub fn insert(&mut self, record: Record) {
        // Traverse to the last page
        // If the last page is full, allocate a new page
        // Write the record to the last page
        let mut filepage = FilePage::new(true, self.first_page_num, self.pager.clone());
        let mut next = filepage.next();
        let mut first_page = true;
        loop {
            match next {
                Some(page_num) => {
                    filepage = FilePage::new(false, page_num, self.pager.clone());
                    next = filepage.next();
                    first_page = false;
                }
                None => break,
            }
        }
        let rs = filepage.insert(&record);
        match rs {
            InsertResult::Spill(remain_size) => {
                let new_page = self.pager.lock().unwrap().get_free_page().unwrap();
                filepage.set_next(new_page as u32);
                let mut new_filepage =
                    FilePage::new(first_page, new_page as u32, self.pager.clone());
                let spilled_record = &record.buf[record.buf.len() - remain_size as usize..];
                new_filepage.insert_spilled(&spilled_record)
            }
            InsertResult::OutOfSpace => {
                let new_page = self.pager.lock().unwrap().get_free_page().unwrap();
                let mut new_filepage = FilePage::new(true, new_page as u32, self.pager.clone());
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

    #[test]
    fn simple_read() {
        let pager = Arc::new(Mutex::new(Pager::init("test_simple_read")));
        let mut file = File::init(1, pager);
        let record = Record::new(vec![1, 2, 3]);
        file.insert(record);
        let mut cursor = file.cursor();
        let record = cursor.read();
        assert_eq!(record.buf, vec![1, 2, 3]);
        remove_file("test_simple_read").unwrap();
    }

    #[test]
    fn complete_read() {
        let pager = Arc::new(Mutex::new(Pager::init("test_complete_insert")));
        let free_page = pager.lock().unwrap().get_free_page().unwrap();
        let mut file = File::init(free_page, pager);
        let record = Record::new([1; 2000].to_vec());
        file.insert(record);
        let record2 = Record::new([2; 2500].to_vec());
        file.insert(record2);
        let record3 = Record::new([3; 3000].to_vec());
        file.insert(record3);
        let record4 = Record::new([4; 3500].to_vec());
        file.insert(record4);
        let record5 = Record::new([5; 4000].to_vec());
        file.insert(record5);
        let mut cursor = file.cursor();
        assert_eq!(cursor.offset, 8);
        let record = cursor.read();
        assert_eq!(record.buf, vec![1; 2000]);

        cursor.next();
        assert_eq!(cursor.offset, 2000 + 12);
        let record = cursor.read();
        assert_eq!(record.buf.len(), 2500);
        assert_eq!(record.buf, vec![2; 2500]);

        assert_eq!(cursor.offset, 2000 + 12 + 2500 - PAGE_SIZE - 8);

        remove_file("test_complete_insert").unwrap();
    }

    #[test]
    fn simple_insert() {
        let pager = Arc::new(Mutex::new(Pager::init("test_simple_insert")));
        let mut file = File::init(1, pager);
        let record = Record::new(vec![1, 2, 3]);
        file.insert(record);
        let record2 = Record::new(vec![4, 5, 6, 7, 8, 9]);
        file.insert(record2);

        let page = file.pager.lock().unwrap().get_page(1).unwrap();
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
    fn test_insert_spill() {
        let pager = Arc::new(Mutex::new(Pager::init("test_insert_spill")));
        let free_page = pager.lock().unwrap().get_free_page().unwrap();
        let mut file = File::init(free_page, pager);
        let record = Record::new([1; 2000].to_vec());
        file.insert(record);
        let record2 = Record::new([2; 2500].to_vec());
        file.insert(record2);

        let page = file.pager.lock().unwrap().get_page(0).unwrap();
        let buf = page.read_buf_at(
            FilePageHeader::size() + FileHeader::size() + size_of::<u32>(),
            2000,
        );
        assert_eq!(buf, vec![1; 2000]);
        let offset = FilePageHeader::size() + FileHeader::size() + size_of::<u32>() * 2 + 2000;
        let buf = page.read_buf_at(offset, PAGE_SIZE - offset);
        assert_eq!(buf, vec![2; PAGE_SIZE - offset]);
        let page = file.pager.lock().unwrap().get_page(2).unwrap();
        let buf = page.read_buf_at(
            FilePageHeader::size() + FileHeader::size() + size_of::<u32>(),
            offset + 2500 - PAGE_SIZE - FileHeader::size(),
        );
        assert_eq!(buf, vec![2; offset + 2500 - PAGE_SIZE - FileHeader::size()]);
        remove_file("test_insert_spill").unwrap();
    }

    #[test]
    fn complete_insert() {
        let pager = Arc::new(Mutex::new(Pager::init("test_complete_insert")));
        let free_page = pager.lock().unwrap().get_free_page().unwrap();
        let mut file = File::init(free_page, pager);
        let record = Record::new([1; 2000].to_vec());
        file.insert(record);
        let record2 = Record::new([2; 2500].to_vec());
        file.insert(record2);
        let record3 = Record::new([3; 3000].to_vec());
        file.insert(record3);
        let record4 = Record::new([4; 3500].to_vec());
        file.insert(record4);
        let record5 = Record::new([5; 4000].to_vec());
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
        let buf = page.read_buf_at(offset + size_of::<u32>(), PAGE_SIZE - offset - size_of::<u32>());
        assert_eq!(len, 2504);
        assert_eq!(buf, vec![2; PAGE_SIZE - offset - size_of::<u32>()]);
        let len = unsafe { page.read_val_at::<u32>(offset) };
        remove_file("test_complete_insert").unwrap();
    }

    #[test]
    fn cursor_read() {
        let pager = Arc::new(Mutex::new(Pager::init("test_cursor_read")));
        let free_page = pager.lock().unwrap().get_free_page().unwrap();
        let mut file = File::init(free_page, pager);
        let record = Record::new([1; 2000].to_vec());
        file.insert(record);
        let record2 = Record::new([2; 2500].to_vec());
        file.insert(record2);
        let record3 = Record::new([3; 3000].to_vec());
        file.insert(record3);

        let mut cursor = file.cursor();
        let record = cursor.next();
        remove_file("test_cursor_read").unwrap();
    }
}
