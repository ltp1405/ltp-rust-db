use buffer_manager::Page;

use super::{
    cell::{self, Cell, CellMut},
    header::{FileHeader, FileNodeHeader},
};

#[derive(Debug)]
pub enum InsertResult<'a> {
    /// new size
    Normal(usize),
    /// remain payload, kept size
    Spill(&'a [u8], usize),
    /// payload
    OutOfSpace(&'a [u8]),
}

pub struct Node<
    'a,
    const BLOCKSIZE: usize,
    const DISK_CAPACITY: usize,
    const MEMORY_CAPACITY: usize,
> {
    pub is_head: bool,
    pub page: Page<'a, BLOCKSIZE, DISK_CAPACITY, MEMORY_CAPACITY>,
}

impl<'a, const BLOCKSIZE: usize, const DISK_CAPACITY: usize, const MEMORY_CAPACITY: usize>
    Node<'a, BLOCKSIZE, DISK_CAPACITY, MEMORY_CAPACITY>
{
    pub fn from_page(
        is_head: bool,
        page: Page<'a, BLOCKSIZE, DISK_CAPACITY, MEMORY_CAPACITY>,
    ) -> Self {
        Self { is_head, page }
    }

    pub fn new(
        is_head: bool,
        mut page: Page<'a, BLOCKSIZE, DISK_CAPACITY, MEMORY_CAPACITY>,
    ) -> Self {
        let header_size = if is_head {
            FileNodeHeader::size() + FileHeader::size()
        } else {
            FileNodeHeader::size()
        };
        let header = FileNodeHeader::new(0, header_size as u32);
        header.write_to(is_head, &mut page);
        Self { is_head, page }
    }

    pub fn free_start(&self) -> u32 {
        let header = FileNodeHeader::read_from(self.is_head, self.page.as_ref());
        header.free_space_start
    }

    pub fn set_free_start(&mut self, free_start: u32) {
        let mut header = FileNodeHeader::read_from(self.is_head, self.page.as_ref());
        header.free_space_start = free_start;
        header.write_to(self.is_head, self.page.as_mut());
    }

    pub fn set_tail(&mut self, block_number: u32) {
        if !self.is_head {
            panic!("set_tail called on non-head node");
        }
        let mut header = FileHeader::read_from(self.page.as_ref());
        header.tail_page_num = block_number;
        header.write_to(self.page.as_mut());
    }

    pub fn tail_page(&self) -> u32 {
        if !self.is_head {
            panic!("tail_page called on non-head node");
        }
        let header = FileHeader::read_from(self.page.as_ref());
        header.tail_page_num
    }

    pub fn set_cell_count(&mut self, count: u64) {
        if !self.is_head {
            panic!("set_cell_count called on non-head node");
        }
        let mut header = FileHeader::read_from(self.page.as_ref());
        header.cell_count = count;
        header.write_to(self.page.as_mut());
    }

    pub fn cell_count(&self) -> u64 {
        if !self.is_head {
            panic!("cell_count called on non-head node");
        }
        let header = FileHeader::read_from(self.page.as_ref());
        header.cell_count
    }

    pub fn read_partial_record(&self, len: usize) -> Vec<u8> {
        // Partial record is always at the start of the block, after the header
        let range = FileNodeHeader::size()..FileNodeHeader::size() + len;
        self.page.as_ref()[range].to_vec()
    }

    /// ### Safety: Must ensure that `start` is correct
    pub unsafe fn read_record_at(&self, start: usize) -> Option<Cell> {
        let cell = Cell::new(start, &self.page)?;
        Some(cell)
    }

    pub fn set_next(&mut self, next: u32) {
        let header = FileNodeHeader::read_from(self.is_head, self.page.as_ref());
        let page_header = FileNodeHeader {
            free_space_start: header.free_space_start,
            next,
        };
        page_header.write_to(self.is_head, self.page.as_mut());
    }

    pub fn next(&self) -> Option<u32> {
        let header = FileNodeHeader::read_from(self.is_head, self.page.as_ref());
        let next = header.next;
        if next == 0 {
            return None;
        }
        Some(next)
    }

    pub fn insert_spilled(&mut self, spilled: &[u8]) {
        let offset = FileNodeHeader::size();

        let start = offset;
        self.page.as_mut()[start..start + spilled.len()].copy_from_slice(spilled);
        let page_header = FileNodeHeader {
            free_space_start: (start + spilled.len()) as u32,
            next: 0,
        };
        page_header.write_to(false, self.page.as_mut());
    }

    pub unsafe fn delete_record_at(&mut self, start: usize) {
        CellMut::new(start, self.page.as_mut()).set_delete(true);
    }

    pub fn insert(&mut self, payload: &'a [u8]) -> InsertResult {
        let offset = self.free_start() as usize;
        let insert_rs = cell::insert_cell(self.page.as_mut(), offset, payload);
        return match insert_rs {
            InsertResult::Normal(end) => {
                self.set_free_start(end as u32);
                insert_rs
            }
            InsertResult::Spill(_remain, _kept_size) => {
                self.set_free_start(BLOCKSIZE as u32);
                insert_rs
            }
            InsertResult::OutOfSpace(_payload) => insert_rs,
        };
    }
}

#[cfg(test)]
mod tests {
    use disk::Disk;

    use crate::unordered_file::node::Node;
    use buffer_manager::BufferManager;
    use disk_manager::DiskManager;

    #[test]
    fn next() {
        let disk = Disk::<512, 65536>::create("node_next").unwrap();
        let disk_manager = DiskManager::init(&disk);
        let block1 = disk_manager.allocate().unwrap();
        let block2 = disk_manager.allocate().unwrap();
        const MEMORY_SIZE: usize = 512 * 16;
        let memory = vec![0; MEMORY_SIZE];
        let buffer_manager: BufferManager<512, 65536, MEMORY_SIZE> =
            BufferManager::init(&memory, &disk);
        let mut root: Node<'_, 512, 65536, MEMORY_SIZE> =
            Node::new(true, buffer_manager.get_page(block1));

        root.set_next(block2 as u32);
        assert_eq!(root.next(), Some(block2 as u32));
    }

    #[test]
    fn insert_spilled() {
        let disk = Disk::<512, 65536>::create("node_insert_spilled").unwrap();
        let disk_manager = DiskManager::init(&disk);
        let block1 = disk_manager.allocate().unwrap();
        let block2 = disk_manager.allocate().unwrap();
        const MEMORY_SIZE: usize = 512 * 16;
        let memory = vec![0; MEMORY_SIZE];
        let buffer_manager: BufferManager<512, 65536, MEMORY_SIZE> =
            BufferManager::init(&memory, &disk);
        let buf = vec![0xa; 400];
        let buf2 = vec![0xa; 400];
        let mut node: Node<'_, 512, 65536, MEMORY_SIZE> =
            Node::new(true, buffer_manager.get_page(block1));
        node.insert(&buf);
        let rs = node.insert(&buf2);
        let mut node2: Node<'_, 512, 65536, MEMORY_SIZE> =
            Node::new(false, buffer_manager.get_page(block1));
        // match rs {
        //     InsertResult::Spill(buf, start) => node2.insert_spilled(&buf[start..]),
        //     _ => panic!("should be spilled"),
        // }
        // let rs = unsafe { node.read_record_at(429) };
        // match rs {
        //     ReadResult::Partial(initial, remain) => {
        //         assert_eq!(initial.len(), 80);
        //         assert_eq!(remain, 120);
        //     }
        //     _ => panic!("should be partial"),
        // }
        // let buf = vec![0xff; 200];
        // let rs = node2.insert(Cell::new(buf));
        // println!("{:?}", node2.buffer.as_ref());
        // match rs {
        //     InsertResult::Normal => {}
        //     _ => panic!("should be normal"),
        // }
        // let rs = unsafe { node2.read_record_at(120 + 8) };
        // match rs {
        //     ReadResult::Normal(cell) => assert_eq!(cell.len(), 200),
        //     _ => panic!("should be normal"),
        // }
    }
}
