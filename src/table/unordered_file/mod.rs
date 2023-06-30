pub mod record;

use std::mem::size_of;
use std::sync::{Arc, Mutex};

use disk::Disk;
use ltp_rust_db_page::page::{Page, PAGE_SIZE};
use ltp_rust_db_page::pager::Pager;

use self::record::Record;

struct FileHeader {}

impl FileHeader {
    const fn size() -> usize {
        0
    }

    fn new() -> Self {
        todo!()
    }

    fn read_from(page: Page) -> Self {
        todo!()
    }

    fn write_to(&self, page: Page) {
        todo!()
    }
}

pub struct Cursor {
    pager: Arc<Mutex<Pager>>,
    page_num: u32,
    offset: u32,
}

impl Cursor {
    fn new(first_page_num: u32, pager: Arc<Mutex<Pager>>) -> Self {
        Self {
            pager,
            page_num: first_page_num,
            offset: FilePageHeader::size() as u32 + FileHeader::size() as u32,
        }
    }

    pub fn read(&self) -> Record {
        let page = self
            .pager
            .lock()
            .unwrap()
            .get_page(self.page_num as usize)
            .unwrap();
        let mut buf = Vec::new();
        let record_len = unsafe { page.read_val_at::<u32>(self.offset as usize) };
        let span = record_len + size_of::<u32>() as u32 + self.offset;
        let (offset, spilled_pages) = if span >= PAGE_SIZE as u32 {
            let span = span - PAGE_SIZE as u32;
            let spilled_pages = span as usize / (PAGE_SIZE - FilePageHeader::size());
            (span as usize - spilled_pages * PAGE_SIZE, 1 + spilled_pages)
        } else {
            (span as usize, 0)
        };
        if spilled_pages == 0 {
            let record =
                page.read_buf_at(self.offset as usize + size_of::<u32>(), record_len as usize);
            buf.extend_from_slice(record);
            return Record { buf };
        } else {
        }
    }

    pub fn next(&mut self) {
        let page = self
            .pager
            .lock()
            .unwrap()
            .get_page(self.page_num as usize)
            .unwrap();
        let record_len = unsafe { page.read_val_at::<u32>(self.offset as usize) };
        let span = record_len + size_of::<u32>() as u32 + self.offset;
        let (offset, spilled_pages) = if span >= PAGE_SIZE as u32 {
            let span = span - PAGE_SIZE as u32;
            let spilled_pages = span as usize / (PAGE_SIZE - FilePageHeader::size());
            (span as usize - spilled_pages * PAGE_SIZE, 1 + spilled_pages)
        } else {
            (span as usize, 0)
        };
        self.offset = offset as u32;
        for _ in 0..spilled_pages {
            let page_header = FilePageHeader::read_from(false, page.clone());
            self.page_num = page_header.next;
        }
    }
}

struct FilePageHeader {
    free_space_start: u32,
    next: u32,
}

impl FilePageHeader {
    fn new() -> Self {
        todo!()
    }

    const fn size() -> usize {
        size_of::<u32>() * 2
    }

    fn read_from(first_page: bool, page: Page) -> Self {
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

    fn write_to(&self, first_page: bool, disk: Page) {
        let offset = if first_page { FileHeader::size() } else { 0 };
        unsafe {
            disk.write_val_at(offset, self.free_space_start);
            disk.write_val_at(offset + size_of::<u32>(), self.next);
        }
    }
}

/// A `File` which only contain records from one `Table`
pub struct File {
    pager: Arc<Mutex<Pager>>,
    pub first_page_num: u32,
}

pub enum InsertResult {
    Normal,
    Spill(usize),
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

    pub fn insert_spilled_record(&mut self, page_num: usize, spilled: &[u8]) {
        let page = self.pager.lock().unwrap().get_page(page_num).unwrap();
        let page_header = FilePageHeader::read_from(false, page.clone());
        let start = page_header.free_space_start;
        let end = start + spilled.len() as u32;
        if end < PAGE_SIZE as u32 {
            page.write_buf_at(start as usize, spilled);
            let page_header = FilePageHeader {
                free_space_start: end,
                next: 0,
            };
            page_header.write_to(false, page.clone());
            return;
        }
        let new_page = self.pager.lock().unwrap().get_free_page().unwrap();
        self.insert_spilled_record(new_page, spilled[PAGE_SIZE..].as_ref());
    }

    pub fn insert_record_at(&mut self, offset: u32, record: Record) {
        let mut pager = self.pager.lock().unwrap();
        let page = pager.get_page(self.first_page_num as usize).unwrap();

        let start = offset;
        if start == PAGE_SIZE as u32 {
            panic!("HERE");
            let record_buf = record.serialize();
            unsafe {
                page.write_val_at::<u32>(start as usize, record_buf.len() as u32);
                page.write_buf_at(start as usize + size_of::<u32>(), record_buf);
            }
            let page_header = FilePageHeader {
                free_space_start: start + record_buf.len() as u32 + size_of::<u32>() as u32,
                next: 0,
            };
            page_header.write_to(false, page.clone());
            let new_page = pager.get_free_page().unwrap();
            println!("{:?}", new_page);
            let page_header = FilePageHeader {
                free_space_start: PAGE_SIZE as u32,
                next: new_page as u32,
            };
            page_header.write_to(false, page.clone());

            let new_page_header = FilePageHeader {
                free_space_start: size_of::<FilePageHeader>() as u32,
                next: 0,
            };

            let new_page = pager.get_page(new_page).unwrap();
            new_page_header.write_to(false, new_page.clone());
            return;
        }
        let end = start + record.buf.len() as u32 + size_of::<u32>() as u32;
        println!("start: {}, end: {}", start, end);
        if end < PAGE_SIZE as u32 {
            let record_buf = record.serialize();
            unsafe {
                page.write_val_at::<u32>(start as usize, record_buf.len() as u32);
                page.write_buf_at(start as usize + size_of::<u32>(), record_buf);
            }
            let page_header = FilePageHeader {
                free_space_start: end,
                next: 0,
            };
            page_header.write_to(false, page.clone());
            return;
        }

        let kept_size = PAGE_SIZE as u32 - start - size_of::<u32>() as u32;
        let record_buf = record.serialize();
        unsafe {
            page.write_val_at::<u32>(start as usize, kept_size);
            page.write_buf_at(
                start as usize + size_of::<u32>(),
                &record_buf[..kept_size as usize],
            );
        }
        let new_page = pager.get_free_page().unwrap();
        drop(pager);
        self.insert_spilled_record(new_page, &record_buf[kept_size as usize..]);
    }

    pub fn insert(&mut self, record: Record) {
        // Traverse to the last page
        // If the last page is full, allocate a new page
        // Write the record to the last page
        let mut pager = self.pager.lock().unwrap();
        let first_page = pager.get_page(self.first_page_num as usize).unwrap();
        let mut page = first_page;
        let mut page_header = FilePageHeader::read_from(true, page.clone());
        let mut page_num = self.first_page_num;
        let page = loop {
            if page_header.next == 0 {
                break page;
            }
            page_num = page_header.next;
            println!("page_num: {:?}", page_num);
            page = pager.get_page(page_header.next as usize).unwrap();
            page_header = FilePageHeader::read_from(true, page.clone());
        };
        drop(pager);
        if page_num == self.first_page_num {
            let page_header = FilePageHeader::read_from(true, page.clone());
            let start = page_header.free_space_start;
            println!("start: {:?}", start);
            self.insert_record_at(start, record);
        } else {
            let page_header = FilePageHeader::read_from(false, page.clone());
            let start = page_header.free_space_start;
            println!("start: {:?}", start);
            self.insert_record_at(start, record);
        }
    }

    fn all_record(&self) -> Vec<Record> {
        let mut pager = self.pager.lock().unwrap();
        let mut page = pager.get_page(self.first_page_num as usize).unwrap();
        let mut page_header = FilePageHeader::read_from(true, page.clone());
        let mut page_num = self.first_page_num;
        let mut records = Vec::new();
        loop {
            let page_header = FilePageHeader::read_from(false, page.clone());
            let mut offset = size_of::<FilePageHeader>() as u32;
            while offset < page_header.free_space_start {
                let record_size = unsafe { page.read_val_at::<u32>(offset as usize) };
                let record_buf =
                    page.read_buf_at(offset as usize + size_of::<u32>(), record_size as usize);
                let record = Record::deserialize(record_buf);
                records.push(record);
                offset += record_size + size_of::<u32>() as u32;
            }
            if page_header.next == 0 {
                break;
            }
            page_num = page_header.next;
            page = pager.get_page(page_header.next as usize).unwrap();
        }
        records
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs::remove_file;

    #[test]
    fn test_insert_simple() {
        let pager = Arc::new(Mutex::new(Pager::init("test_simple_insert")));
        let mut file = File::init(1, pager);
        let record = Record::new(vec![1, 2, 3]);
        file.insert(record);
        let record2 = Record::new(vec![4, 5, 6]);
        file.insert(record2);

        let page = file.pager.lock().unwrap().get_page(1).unwrap();
        println!("{:?}", page);
        println!("{:?}", file.all_record());
        remove_file("test_simple_insert").unwrap();
        panic!()
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
        println!("{:?}", page);
        let page = file.pager.lock().unwrap().get_page(2).unwrap();
        println!("{:?}", page);
        remove_file("test_insert_spill").unwrap();
        panic!()
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
        let record6 = Record::new([6; 4500].to_vec());
        file.insert(record6);
        let record7 = Record::new([7; 5000].to_vec());
        file.insert(record7);
        let record8 = Record::new([8; 5500].to_vec());
        file.insert(record8);
        let record9 = Record::new([9; 6000].to_vec());
        file.insert(record9);
        let record10 = Record::new([10; 6500].to_vec());
        file.insert(record10);
        let record11 = Record::new([11; 7000].to_vec());
        file.insert(record11);
        let record12 = Record::new([12; 7500].to_vec());
        file.insert(record12);
        let record13 = Record::new([13; 8000].to_vec());
        file.insert(record13);
        let record14 = Record::new([14; 8500].to_vec());
        file.insert(record14);
        let record15 = Record::new([15; 9000].to_vec());
        file.insert(record15);
        let record16 = Record::new([16; 9500].to_vec());
        file.insert(record16);
        let record17 = Record::new([17; 10000].to_vec());
        file.insert(record17);
        let record18 = Record::new([18; 10500].to_vec());
        file.insert(record18);
        let record19 = Record::new([19; 11000].to_vec());

        let page = file
            .pager
            .lock()
            .unwrap()
            .get_page(file.first_page_num as usize)
            .unwrap();
        panic!();
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
    }
}
