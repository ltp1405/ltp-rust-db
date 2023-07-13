use std::mem::size_of;

use disk::Disk;

use crate::free_space_manager::FreeSpaceManager;

use super::{
    cell::Cell,
    header::{FileHeader, FilePageHeader},
    node::{Node, ReadResult},
};

#[derive(Debug)]
pub struct Cursor<const BLOCKSIZE: usize, const CAPACITY: usize> {
    block_number: u32,
    offset: usize,
    cell_count: u64,
    at_head: bool,
    cur_cell: u64,
    disk: Disk<BLOCKSIZE, CAPACITY>,
    disk_manager: FreeSpaceManager<BLOCKSIZE, CAPACITY>,
}

impl<const BLOCKSIZE: usize, const CAPACITY: usize> Iterator for Cursor<BLOCKSIZE, CAPACITY> {
    type Item = Cell;

    fn next(&mut self) -> Option<Self::Item> {
        self.cur_cell += 1;
        if self.cur_cell > self.cell_count {
            return None;
        }
        let cell = self.read();
        self._next();
        cell
    }
}

impl<const BLOCKSIZE: usize, const CAPACITY: usize> Cursor<BLOCKSIZE, CAPACITY> {
    pub fn new(
        cell_count: u64,
        head_block_number: u32,
        disk: &Disk<BLOCKSIZE, CAPACITY>,
        disk_manager: &FreeSpaceManager<BLOCKSIZE, CAPACITY>,
    ) -> Self {
        Self {
            cur_cell: 0,
            cell_count,
            block_number: head_block_number,
            offset: FilePageHeader::size() + FileHeader::size(),
            at_head: true,
            disk: disk.clone(),
            disk_manager: disk_manager.clone(),
        }
    }

    pub fn read(&mut self) -> Option<Cell> {
        let node = Node::read_from_disk(
            self.at_head,
            self.block_number,
            &self.disk,
            &self.disk_manager,
        );
        let block = self.disk.read_block(2 as usize).unwrap();
        println!("block: {:?}", block);
        println!("block_number: {:?}", self.block_number);
        println!("offset: {:?}", self.offset);
        let rs = unsafe { node.read_record_at(self.offset) };
        match rs {
            ReadResult::EndOfFile => None,
            ReadResult::Normal(record) => Some(record),
            ReadResult::Partial(mut initial, remain) => {
                let next_block = node.next().unwrap();
                let block = Node::read_from_disk(false, next_block, &self.disk, &self.disk_manager);
                let remain = block.get_partial_record(remain);
                initial.extend(remain);
                Some(Cell::new(initial))
            }
        }
    }

    pub fn _next(&mut self) {
        let block = self.disk.read_block(self.block_number as usize).unwrap();
        let len = u32::from_be_bytes(
            block.as_slice()[self.offset..self.offset + size_of::<u32>()]
                .try_into()
                .unwrap(),
        );
        assert!(len > 0 && len < BLOCKSIZE as u32);
        let next_offset = self.offset + len as usize;
        if next_offset <= BLOCKSIZE - size_of::<u32>() {
            self.offset = next_offset;
        } else if next_offset > BLOCKSIZE - size_of::<u32>() && next_offset < BLOCKSIZE {
            let page_header = FilePageHeader::read_from(self.at_head, block.as_ref());
            self.block_number = page_header.next;
            self.offset = FilePageHeader::size();
            self.at_head = false;
        } else {
            let page_header = FilePageHeader::read_from(self.at_head, block.as_ref());
            self.block_number = page_header.next;
            if self.block_number == 0 {
                panic!("No next page");
            }
            self.offset = next_offset - BLOCKSIZE + FilePageHeader::size();
            self.at_head = false;
        }
    }
}
