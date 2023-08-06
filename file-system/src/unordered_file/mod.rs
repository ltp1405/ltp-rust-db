pub mod cell;
pub mod cursor;
mod header;
mod node;

use disk::Disk;

use crate::{buffer_manager::BufferManager, disk_manager::DiskManager};

pub use cell::Cell;
pub use cursor::Cursor;
use header::FileHeader;
use node::{InsertResult, Node};

use self::header::FileNodeHeader;

/// A `File` which only contain records from one `Table`
/// Implemented as a linked list of page
pub struct File<'a, const BLOCKSIZE: usize, const CAPACITY: usize, const MEMORY_CAPACITY: usize> {
    disk_manager: &'a DiskManager<BLOCKSIZE, CAPACITY>,
    buffer_manager: &'a BufferManager<'a, BLOCKSIZE, CAPACITY, MEMORY_CAPACITY>,
    pub head_page_number: u32,
}

impl<'a, const BLOCKSIZE: usize, const CAPACITY: usize, const MEMORY_CAPACITY: usize>
    File<'a, BLOCKSIZE, CAPACITY, MEMORY_CAPACITY>
{
    pub fn init(
        disk_manager: &'a DiskManager<BLOCKSIZE, CAPACITY>,
        buffer_manager: &'a BufferManager<'a, BLOCKSIZE, CAPACITY, MEMORY_CAPACITY>,
    ) -> (Self, u32) {
        let new_page_number = disk_manager.allocate().unwrap();
        let new_page = buffer_manager.get_page(new_page_number);
        let file_header = FileHeader {
            cell_count: 0,
            tail_page_num: new_page_number as u32,
            head_page_num: new_page_number as u32,
        };
        file_header.write_to(new_page.buffer_mut());

        let page_header = FileNodeHeader {
            free_space_start: (FileNodeHeader::size() + FileHeader::size()) as u32,
            next: 0,
        };
        page_header.write_to(true, new_page.buffer_mut());
        (
            File {
                disk_manager,
                buffer_manager,
                head_page_number: new_page_number,
            },
            new_page_number,
        )
    }

    pub fn open(
        buffer_manager: &'a BufferManager<'a, BLOCKSIZE, CAPACITY, MEMORY_CAPACITY>,
        disk_manager: &'a DiskManager<BLOCKSIZE, CAPACITY>,
        first_page_num: u32,
    ) -> Self {
        File {
            disk_manager,
            buffer_manager,
            head_page_number: first_page_num,
        }
    }

    pub fn cursor(&'a self) -> Cursor<BLOCKSIZE, CAPACITY, MEMORY_CAPACITY> {
        let page = self.buffer_manager.get_page(self.head_page_number);
        let block: Node<'_, BLOCKSIZE, CAPACITY, MEMORY_CAPACITY> = Node::from_page(true, page);
        Cursor::new(
            block.cell_count(),
            self.head_page_number,
            self.buffer_manager,
        )
    }

    pub fn insert(&self, cell: Cell) {
        // Traverse to the last page
        // If the last page is full, allocate a new page
        // Write the cell to the last page
        let page = self.buffer_manager.get_page(self.head_page_number);
        let mut head: Node<'_, BLOCKSIZE, CAPACITY, MEMORY_CAPACITY> = Node::from_page(true, page);
        let first_block = head.tail_page() == self.head_page_number;

        let tail = self.buffer_manager.get_page(head.tail_page());
        let mut node: Node<'_, BLOCKSIZE, CAPACITY, MEMORY_CAPACITY> =
            Node::from_page(first_block, tail);
        let rs = node.insert(cell);
        match rs {
            InsertResult::Normal => {
                if first_block {
                    drop(head);
                    let count = node.cell_count() + 1;
                    node.set_cell_count(count);
                } else {
                    let count = head.cell_count() + 1;
                    head.set_cell_count(count);
                }
            }
            InsertResult::Spill(buf, remain_start) => {
                let new_block = self.disk_manager.allocate().unwrap();
                let new_page = self.buffer_manager.get_page(new_block);
                let mut new_node: Node<'_, BLOCKSIZE, CAPACITY, MEMORY_CAPACITY> =
                    Node::new(false, new_page);
                let spilled_cell = &buf[remain_start..];
                new_node.insert_spilled(&spilled_cell);

                // `node` and `head` is the same block
                if first_block {
                    drop(head);
                    node.set_next(new_block as u32);
                    node.set_tail(new_block as u32);
                    let count = node.cell_count() + 1;
                    node.set_cell_count(count);
                } else {
                    node.set_next(new_block as u32);
                    head.set_tail(new_block as u32);
                    let count = head.cell_count() + 1;
                    head.set_cell_count(count);
                }
            }
            InsertResult::OutOfSpace(cell) => {
                let new_block = self.disk_manager.allocate().unwrap();
                let new_page = self.buffer_manager.get_page(new_block);
                let mut new_node: Node<'_, BLOCKSIZE, CAPACITY, MEMORY_CAPACITY> =
                    Node::new(false, new_page);
                new_node.insert(cell);
                if first_block {
                    drop(head);
                    node.set_next(new_block as u32);
                    node.set_tail(new_block as u32);
                    let count = node.cell_count() + 1;
                    node.set_cell_count(count);
                } else {
                    node.set_next(new_block as u32);
                    head.set_tail(new_block as u32);
                    let count = head.cell_count() + 1;
                    head.set_cell_count(count);
                }
            }
        }
    }

    pub fn save(&self) {
        let current_page = self.buffer_manager.get_page(self.head_page_number);
        let current_node: Node<'_, BLOCKSIZE, CAPACITY, MEMORY_CAPACITY> =
            Node::from_page(true, current_page);
        let mut next_page_num = current_node.next();
        drop(current_node);
        self.buffer_manager
            .save_page(self.head_page_number)
            .unwrap();
        loop {
            if next_page_num.is_none() {
                break;
            }
            let next = next_page_num.unwrap();
            let next_page = self.buffer_manager.get_page(next);
            let next_node: Node<'_, BLOCKSIZE, CAPACITY, MEMORY_CAPACITY> =
                Node::from_page(false, next_page);
            self.buffer_manager.save_page(next).unwrap();
            next_page_num = next_node.next();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn simple_read() {
        const BLOCKSIZE: usize = 512;
        const CAPACITY: usize = 512 * 128;
        const MEMORY_CAPACITY: usize = 512 * 32;
        let disk = Disk::<BLOCKSIZE, CAPACITY>::create("test_simple_read").unwrap();
        let disk_manager = DiskManager::init(&disk);

        {
            let memory = [0; MEMORY_CAPACITY];
            let buffer_manager: BufferManager<'_, BLOCKSIZE, CAPACITY, MEMORY_CAPACITY> =
                BufferManager::init(&memory, &disk);
            let file = File::init(&disk_manager, &buffer_manager).0;
            let record = Cell::new(vec![1, 2, 3]);
            file.insert(record);
            let cell = file.cursor().next().unwrap();
            assert_eq!(cell.to_vec(), vec![1, 2, 3]);
            file.save();
        }
        {
            let memory = [0; MEMORY_CAPACITY];
            let buffer_manager: BufferManager<'_, BLOCKSIZE, CAPACITY, MEMORY_CAPACITY> =
                BufferManager::init(&memory, &disk);
            let file = File::open(&buffer_manager, &disk_manager, 1);
            let mut cursor = file.cursor();
            let record = cursor.next().unwrap();
            assert_eq!(record, Cell::new(vec![1, 2, 3]));
        }
    }

    #[test]
    fn edge_case() {
        const BLOCKSIZE: usize = 512;
        const CAPACITY: usize = 512 * 128;
        const MEMORY_CAPACITY: usize = 512 * 32;
        let memory = [0; MEMORY_CAPACITY];
        let disk = Disk::<BLOCKSIZE, CAPACITY>::create("edge_case").unwrap();
        let disk_manager = DiskManager::init(&disk);
        let buffer_manager: BufferManager<'_, BLOCKSIZE, CAPACITY, MEMORY_CAPACITY> =
            BufferManager::init(&memory, &disk);
        let mut file = File::init(&disk_manager, &buffer_manager);
    }

    #[test]
    fn complete_read() {
        let disk = Disk::<4096, 65536>::create("test_complete_read").unwrap();
        let disk_manager = DiskManager::init(&disk);
        // let mut file = File::init(&disk, &disk_manager);
    }

    #[test]
    fn random_insert_read() {
        let disk = Disk::<4096, 819200>::create("test_random_insert_read").unwrap();
        let disk_manager = DiskManager::init(&disk);
        // let mut file = File::init(&disk, &disk_manager);
        let mut rng = rand::thread_rng();
    }
}
