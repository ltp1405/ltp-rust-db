use std::mem::size_of;

use crate::buffer_manager::BufferManager;

use super::{
    cell::{Cell, PayloadReadResult},
    header::{FileHeader, FileNodeHeader},
    node::Node,
};

pub struct Cursor<'a, const BLOCKSIZE: usize, const CAPACITY: usize, const MEMORY_CAPACITY: usize> {
    head_number: std::cell::Cell<u32>,
    block_number: std::cell::Cell<u32>,
    offset: std::cell::Cell<usize>,
    cell_count: u64,
    at_head: std::cell::Cell<bool>,
    cur_cell: std::cell::Cell<u64>,
    buffer_manager: &'a BufferManager<'a, BLOCKSIZE, CAPACITY, MEMORY_CAPACITY>,
}

impl<'a, const BLOCKSIZE: usize, const CAPACITY: usize, const MEMORY_CAPACITY: usize> Iterator
    for Cursor<'a, BLOCKSIZE, CAPACITY, MEMORY_CAPACITY>
{
    type Item = Vec<u8>;
    fn next(&mut self) -> Option<Self::Item> {
        self.cur_cell.set(self.cur_cell.get() + 1);
        if self.cur_cell.get() > self.cell_count {
            return None;
        }
        self.skip_delete();
        let cell = { self.read() };
        self.advance();
        cell
    }
}

impl<'a, const BLOCKSIZE: usize, const CAPACITY: usize, const MEMORY_CAPACITY: usize>
    Cursor<'a, BLOCKSIZE, CAPACITY, MEMORY_CAPACITY>
{
    pub fn new(
        cell_count: u64,
        head_block_number: u32,
        buffer_manager: &'a BufferManager<'a, BLOCKSIZE, CAPACITY, MEMORY_CAPACITY>,
    ) -> Self {
        Self {
            head_number: std::cell::Cell::new(head_block_number),
            cur_cell: std::cell::Cell::new(0),
            cell_count,
            block_number: std::cell::Cell::new(head_block_number),
            offset: std::cell::Cell::new(FileNodeHeader::size() + FileHeader::size()),
            at_head: std::cell::Cell::new(true),
            buffer_manager,
        }
    }

    pub fn read(&self) -> Option<Vec<u8>> {
        let page = self.buffer_manager.get_page(self.block_number.get());
        let node: Node<BLOCKSIZE, CAPACITY, MEMORY_CAPACITY> =
            Node::from_page(self.at_head.get(), page);
        let rs = unsafe { node.read_record_at(self.offset.get()) }?;
        match rs.payload() {
            PayloadReadResult::InPage { payload } => Some(payload.to_vec()),
            PayloadReadResult::InOverflow {
                initial_payload,
                remain,
            } => {
                let mut payload = initial_payload.to_vec();
                let next_block = node.next().unwrap();
                let page = self.buffer_manager.get_page(next_block);
                let node: Node<BLOCKSIZE, CAPACITY, MEMORY_CAPACITY> = Node::from_page(false, page);
                let remain = node.read_partial_record(remain);
                payload.extend(remain);
                Some(payload)
            }
        }
    }

    pub fn skip_delete(&self) {
        loop {
            let page = self.buffer_manager.get_page(self.block_number.get());
            let node: Node<BLOCKSIZE, CAPACITY, MEMORY_CAPACITY> =
                Node::from_page(self.at_head.get(), page);
            let rs = unsafe { node.read_record_at(self.offset.get()) }.unwrap();
            if !rs.is_delete() {
                return;
            }
            self.advance();
        }
    }

    pub fn advance(&self) {
        let page = self.buffer_manager.get_page(self.block_number.get());
        let cell = unsafe { Cell::new(self.offset.get(), &page) };
        let cell = match cell {
            Some(cell) => cell,
            None => return,
        };
        let next_offset = self.offset.get() + cell.payload_size() + Cell::header_size();
        if next_offset <= BLOCKSIZE - Cell::header_size() {
            self.offset.set(next_offset);
        } else if next_offset >= BLOCKSIZE - Cell::header_size() && next_offset < BLOCKSIZE {
            let page_header = FileNodeHeader::read_from(self.at_head.get(), page.as_ref());
            self.block_number.set(page_header.next);
            self.offset.set(FileNodeHeader::size());
            self.at_head.set(false);
        } else {
            let page_header = FileNodeHeader::read_from(self.at_head.get(), page.as_ref());
            self.block_number.set(page_header.next);
            if self.block_number.get() == 0 {
                panic!("No next page");
            }
            self.offset
                .set(next_offset - BLOCKSIZE + FileNodeHeader::size());
            self.at_head.set(false);
        }
    }

    pub fn delete(&self) {
        let page = self.buffer_manager.get_page(self.block_number.get());
        let mut node = Node::from_page(self.at_head.get(), page);
        unsafe {
            node.delete_record_at(self.offset.get());
        }
        let head_page = self.buffer_manager.get_page(self.head_number.get());
        let mut head = Node::from_page(true, head_page);
        head.set_cell_count(head.cell_count() - 1);
    }
}

#[cfg(test)]
mod tests {
    use disk::Disk;

    use crate::{buffer_manager::BufferManager, disk_manager::DiskManager, unordered_file::File};

    #[test]
    fn basic_insert_delete() {
        let disk = Disk::<512, 65536>::create("cursor::basic_insert_delete").unwrap();
        let disk_manager = DiskManager::init(&disk);
        const MEMORY_SIZE: usize = 512 * 16;
        let memory = vec![0; MEMORY_SIZE];
        let buffer_manager: BufferManager<512, 65536, MEMORY_SIZE> =
            BufferManager::init(&memory, &disk);
        let file = File::init(&disk_manager, &buffer_manager);
        let records = vec![[0x2; 51].to_vec(), [0x3; 200].to_vec(), [0x4; 412].to_vec()];
        file.insert(&records[0]);
        file.insert(&records[1]);
        file.insert(&records[2]);
        let cursor = file.cursor();
        cursor.advance();
        cursor.delete();
        let cursor = file.cursor();
        let mut iter = cursor.into_iter();
        assert_eq!(iter.next().unwrap(), records[0]);
        assert_eq!(iter.next().unwrap(), records[2]);

        let cursor = file.cursor();
        cursor.delete();
        let cursor = file.cursor();
        let mut iter = cursor.into_iter();
        assert_eq!(iter.next().unwrap(), records[2]);

        let cursor = file.cursor();
        cursor.delete();
        let cursor = file.cursor();
        let mut iter = cursor.into_iter();
        assert_eq!(iter.next(), None);
    }

    #[test]
    fn basic() {
        let disk = Disk::<512, 65536>::create("basic_cursor_test").unwrap();
        let disk_manager = DiskManager::init(&disk);
        const MEMORY_SIZE: usize = 512 * 16;
        let memory = vec![0; MEMORY_SIZE];
        let buffer_manager: BufferManager<512, 65536, MEMORY_SIZE> =
            BufferManager::init(&memory, &disk);
        let file = File::init(&disk_manager, &buffer_manager);
        let records = vec![
            [0x2; 51].to_vec(),
            [0x2; 200].to_vec(),
            [0x2; 412].to_vec(),
            [0x1; 17].to_vec(),
            [0x1; 17].to_vec(),
            [0x2; 51].to_vec(),
            [0x1; 17].to_vec(),
        ];
        for record in records.clone() {
            file.insert(&record)
        }

        for (i, record) in file.cursor().enumerate() {
            assert_eq!(records[i], record);
        }
    }
}
