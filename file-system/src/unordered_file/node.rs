use std::mem::size_of;

use disk::Disk;

use crate::free_space_manager::FreeSpaceManager;

use super::{
    cell::Cell,
    header::{FileHeader, FilePageHeader},
};

#[derive(Debug)]
pub enum InsertResult {
    Normal,
    Spill(usize),
    OutOfSpace,
}

pub struct Node<const BLOCKSIZE: usize, const CAPACITY: usize> {
    dirty: bool,
    is_head: bool,
    block_number: u32,
    header: FilePageHeader,
    pub buffer: Box<[u8; BLOCKSIZE]>,
    disk: Disk<BLOCKSIZE, CAPACITY>,
    disk_manager: FreeSpaceManager<BLOCKSIZE, CAPACITY>,
}

#[derive(Debug)]
pub enum ReadResult {
    EndOfFile,
    Normal(Cell),
    Partial(Vec<u8>, usize),
}

impl<const BLOCKSIZE: usize, const CAPACITY: usize> Node<BLOCKSIZE, CAPACITY> {
    pub fn read_from_disk(
        is_head: bool,
        block_number: u32,
        disk: &Disk<BLOCKSIZE, CAPACITY>,
        disk_manager: &FreeSpaceManager<BLOCKSIZE, CAPACITY>,
    ) -> Self {
        let buffer = disk.read_block(block_number as usize).unwrap();
        let header = FilePageHeader::read_from(is_head, buffer.as_ref());
        Self {
            header,
            dirty: false,
            is_head,
            buffer,
            block_number,
            disk: disk.clone(),
            disk_manager: disk_manager.clone(),
        }
    }

    pub fn new(
        is_head: bool,
        block_number: u32,
        disk: &Disk<BLOCKSIZE, CAPACITY>,
        disk_manager: &FreeSpaceManager<BLOCKSIZE, CAPACITY>,
    ) -> Self {
        let header_size = if is_head {
            FilePageHeader::size() + FileHeader::size()
        } else {
            FilePageHeader::size()
        };
        let header = FilePageHeader::new(0, header_size as u32);
        let mut buffer = Box::new([0; BLOCKSIZE]);
        header.write_to(is_head, buffer.as_mut());
        Self {
            header,
            buffer,
            dirty: true,
            is_head,
            block_number,
            disk: disk.clone(),
            disk_manager: disk_manager.clone(),
        }
    }

    pub fn free_start(&self) -> u32 {
        let header = FilePageHeader::read_from(self.is_head, self.buffer.as_ref());
        header.free_space_start
    }

    pub fn set_free_start(&mut self, free_start: u32) {
        self.dirty = true;
        let mut header = FilePageHeader::read_from(self.is_head, self.buffer.as_ref());
        header.free_space_start = free_start;
        header.write_to(self.is_head, self.buffer.as_mut());
    }

    pub fn set_tail(&mut self, block_number: u32) {
        self.dirty = true;
        if !self.is_head {
            panic!("set_tail called on non-head node");
        }
        let mut header = FileHeader::read_from(self.buffer.as_ref());
        header.tail_page_num = block_number;
        header.write_to(self.buffer.as_mut());
    }

    pub fn tail_page(&self) -> u32 {
        if !self.is_head {
            panic!("tail_page called on non-head node");
        }
        let header = FileHeader::read_from(self.buffer.as_ref());
        header.tail_page_num
    }

    pub fn set_cell_count(&mut self, count: u64) {
        self.dirty = true;
        if !self.is_head {
            panic!("set_cell_count called on non-head node");
        }
        let mut header = FileHeader::read_from(self.buffer.as_ref());
        header.cell_count = count;
        header.write_to(self.buffer.as_mut());
    }

    pub fn cell_count(&self) -> u64 {
        if !self.is_head {
            panic!("cell_count called on non-head node");
        }
        let header = FileHeader::read_from(self.buffer.as_ref());
        header.cell_count
    }

    pub fn get_partial_record(&self, len: usize) -> Vec<u8> {
        let range = FilePageHeader::size()..FilePageHeader::size() + len;
        self.buffer.as_slice()[range].to_vec()
    }

    /// ### Safety: Must ensure that `start` is correct
    pub unsafe fn read_record_at(&self, start: usize) -> ReadResult {
        let size = u32::from_be_bytes(
            self.buffer.as_ref()[start..start + size_of::<u32>()]
                .try_into()
                .unwrap(),
        );
        if size == 0 {
            return ReadResult::EndOfFile;
        }
        let payload_len = size - size_of::<u32>() as u32;
        let start = start + size_of::<u32>();
        println!("start: {}, payload_len: {}", start, payload_len);
        if payload_len as usize + start < BLOCKSIZE {
            let buf = self.buffer[start..start + payload_len as usize].to_vec();
            ReadResult::Normal(Cell { buf })
        } else {
            let keep = BLOCKSIZE - start;
            let buf = self.buffer[start..start + keep as usize].to_vec();

            assert!(
                payload_len >= keep as u32,
                "payload_len: {}, keep: {}",
                payload_len,
                keep
            );
            let remain = payload_len as usize - keep;
            ReadResult::Partial(buf, remain)
        }
    }

    pub fn set_next(&mut self, next: u32) {
        self.dirty = true;
        let header = FilePageHeader::read_from(self.is_head, self.buffer.as_ref());
        let page_header = FilePageHeader {
            free_space_start: header.free_space_start,
            next,
        };
        page_header.write_to(self.is_head, self.buffer.as_mut());
    }

    pub fn next(&self) -> Option<u32> {
        let header = FilePageHeader::read_from(self.is_head, self.buffer.as_ref());
        let next = header.next;
        if next == 0 {
            return None;
        }
        Some(next)
    }

    pub fn insert_spilled(&mut self, spilled: &[u8]) {
        self.dirty = true;
        let offset = FilePageHeader::size();

        let start = offset;
        self.buffer.as_mut()[start..start + spilled.len()].copy_from_slice(spilled);
        let page_header = FilePageHeader {
            free_space_start: (start + spilled.len()) as u32,
            next: 0,
        };
        page_header.write_to(false, self.buffer.as_mut());
    }

    pub fn insert(&mut self, cell: &Cell) -> InsertResult {
        self.dirty = true;
        let offset = self.free_start();

        let start = offset as usize;
        let end = start + cell.size();

        if cell.buf.len() >= BLOCKSIZE - 28 {
            panic!(
                "record too large, must be less than {} bytes",
                BLOCKSIZE - 28
            );
        }
        if BLOCKSIZE - start < size_of::<u32>() {
            // Not enough space to store the record size
            // This record should be stored in a new page
            return InsertResult::OutOfSpace;
        }
        if end < BLOCKSIZE {
            // record can be inserted in a single page
            let record_buf = cell.serialize();
            let record_size = cell.size() as u32;
            self.buffer[start..start + size_of::<u32>()]
                .copy_from_slice(record_size.to_be_bytes().as_ref());
            self.buffer[start + size_of::<u32>()..start + record_size as usize]
                .copy_from_slice(record_buf.as_ref());
            let page_header = FilePageHeader {
                free_space_start: end as u32,
                next: 0,
            };
            page_header.write_to(self.is_head, self.buffer.as_mut());
            return InsertResult::Normal;
        } else {
            // record cannot be inserted in a single page and should be spilled
            let kept_size = BLOCKSIZE - start - size_of::<u32>();
            let cell_buf = cell.serialize();
            let cell_size = cell.size() as u32;
            self.buffer[start..start + size_of::<u32>()]
                .copy_from_slice(cell_size.to_be_bytes().as_ref());
            let start = start + size_of::<u32>();
            self.buffer.as_mut()[start..start + kept_size as usize]
                .copy_from_slice(&cell_buf[..kept_size as usize]);
            self.set_free_start(BLOCKSIZE as u32);
            return InsertResult::Spill(kept_size);
        }
    }
}

impl<const BLOCKSIZE: usize, const CAPACITY: usize> Drop for Node<BLOCKSIZE, CAPACITY> {
    fn drop(&mut self) {
        if self.dirty {
            self.disk
                .write_block(self.block_number as usize, self.buffer.as_ref())
                .unwrap();
        }
    }
}

#[cfg(test)]
mod tests {
    use disk::Disk;

    use crate::{
        free_space_manager::FreeSpaceManager,
        unordered_file::{
            cell::Cell,
            header::FilePageHeader,
            node::{InsertResult, Node, ReadResult},
        },
    };

    #[test]
    fn next() {
        let disk = Disk::<512, 65536>::create("node_next").unwrap();
        let disk_manager = FreeSpaceManager::init(&disk);
        let block1 = disk_manager.allocate().unwrap();
        let block2 = disk_manager.allocate().unwrap();
        let mut root = Node::new(true, block1, &disk, &disk_manager);

        root.set_next(block2 as u32);
        assert_eq!(root.next(), Some(block2 as u32));
    }

    #[test]
    fn insert_spilled() {
        let disk = Disk::<512, 4096>::create("node_insert_spilled").unwrap();
        let disk_manager = FreeSpaceManager::init(&disk);
        let block1 = disk_manager.allocate().unwrap();
        let block2 = disk_manager.allocate().unwrap();
        let mut node = Node::new(true, block1, &disk, &disk_manager);
        let buf = vec![0xa; 400];
        node.insert(&Cell { buf });
        let buf = vec![0xa; 200];
        let rs = node.insert(&Cell { buf });
        let mut node2 = Node::new(false, block2, &disk, &disk_manager);
        match rs {
            InsertResult::Spill(start) => {
                assert_eq!(start, 80);
                node2.insert_spilled(&[0xa; 200][80..])
            }
            _ => panic!("should be spilled"),
        }
        let rs = unsafe { node.read_record_at(428) };
        match rs {
            ReadResult::Partial(initial, remain) => {
                assert_eq!(initial.len(), 80);
                assert_eq!(remain, 120);
            }
            _ => panic!("should be partial"),
        }
        let buf = vec![0xff; 200];
        let rs = node2.insert(&Cell { buf });
        println!("{:?}", node2.buffer.as_ref());
        match rs {
            InsertResult::Normal => {}
            _ => panic!("should be normal"),
        }
        let rs = unsafe { node2.read_record_at(120 + 8) };
        match rs {
            ReadResult::Normal(cell) => assert_eq!(cell.buf.len(), 200),
            _ => panic!("should be normal"),
        }
    }
}
