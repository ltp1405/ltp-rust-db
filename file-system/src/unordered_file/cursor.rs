use std::mem::size_of;

use crate::buffer_manager::BufferManager;

use super::{
    cell::Cell,
    header::{FileHeader, FileNodeHeader},
    node::{Node, ReadResult},
};

pub struct Cursor<'a, const BLOCKSIZE: usize, const CAPACITY: usize, const MEMORY_CAPACITY: usize> {
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
    type Item = Cell;
    fn next(&mut self) -> Option<Self::Item> {
        self.cur_cell.set(self.cur_cell.get() + 1);
        if self.cur_cell.get() > self.cell_count {
            return None;
        }
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
            cur_cell: std::cell::Cell::new(0),
            cell_count,
            block_number: std::cell::Cell::new(head_block_number),
            offset: std::cell::Cell::new(FileNodeHeader::size() + FileHeader::size()),
            at_head: std::cell::Cell::new(true),
            buffer_manager,
        }
    }

    pub fn read(&self) -> Option<Cell> {
        let page = self.buffer_manager.get_page(self.block_number.get());
        let node: Node<BLOCKSIZE, CAPACITY, MEMORY_CAPACITY> =
            Node::from_page(self.at_head.get(), page);
        let rs = unsafe { node.read_record_at(self.offset.get()) };
        match rs {
            ReadResult::EndOfFile => None,
            ReadResult::Normal(record) => Some(record),
            ReadResult::Partial {
                initial_payload: mut initial,
                remain,
            } => {
                let next_block = node.next().unwrap();
                let page = self.buffer_manager.get_page(next_block);
                let node: Node<BLOCKSIZE, CAPACITY, MEMORY_CAPACITY> = Node::from_page(false, page);
                let remain = node.get_partial_record(remain);
                initial.extend(remain);
                Some(Cell::new(initial))
            }
        }
    }

    pub fn advance(&self) {
        let page = self.buffer_manager.get_page(self.block_number.get());
        let is_deleted = u8::from_be_bytes(
            page.as_ref()[self.offset.get()..self.offset.get() + size_of::<u8>()]
                .try_into()
                .unwrap(),
        ) != 0;
        let len = u32::from_be_bytes(
            page.as_ref()[self.offset.get()..self.offset.get() + size_of::<u32>()]
                .try_into()
                .unwrap(),
        );
        assert!(len > 0 && len < BLOCKSIZE as u32, "len: {}", len);
        let next_offset = self.offset.get() + len as usize;
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
        let mut node: Node<'_, BLOCKSIZE, CAPACITY, MEMORY_CAPACITY> =
            Node::from_page(self.at_head.get(), page);
        unsafe {
            node.delete_record_at(self.offset.get());
        }
    }
}
