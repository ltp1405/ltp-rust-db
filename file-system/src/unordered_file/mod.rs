pub mod cell;
pub mod cursor;
mod header;
mod node;

use disk::Disk;

use crate::free_space_manager::FreeSpaceManager;

pub use cell::Cell;
pub use cursor::Cursor;
use header::{FileHeader, FilePageHeader};
use node::{InsertResult, Node};

/// A `File` which only contain records from one `Table`
/// Implemented as a linked list of page
pub struct File<const BLOCKSIZE: usize, const CAPACITY: usize> {
    memory: memory::PhysicalMemory<CAPACITY>,
    address: usize,
}

impl<const BLOCKSIZE: usize, const CAPACITY: usize> File<BLOCKSIZE, CAPACITY> {
    pub fn init(
        disk: &Disk<BLOCKSIZE, CAPACITY>,
        disk_manager: &FreeSpaceManager<BLOCKSIZE, CAPACITY>,
    ) -> Self {
        let new_block = disk_manager.allocate().unwrap();
        let mut block: [u8; BLOCKSIZE] = [0; BLOCKSIZE];
        let file_header = FileHeader {
            cell_count: 0,
            tail_page_num: new_block as u32,
            head_page_num: new_block as u32,
        };
        file_header.write_to(block.as_mut_slice());

        let page_header = FilePageHeader {
            free_space_start: (FilePageHeader::size() + FileHeader::size()) as u32,
            next: 0,
        };
        page_header.write_to(true, block.as_mut_slice());
        disk.write_block(new_block as usize, block.as_ref())
            .unwrap();
        File {
            disk: disk.clone(),
            disk_manager: disk_manager.clone(),
            first_page_num: new_block,
        }
    }

    pub fn open(
        disk: &Disk<BLOCKSIZE, CAPACITY>,
        disk_manager: &FreeSpaceManager<BLOCKSIZE, CAPACITY>,
        first_page_num: u32,
    ) -> Self {
        File {
            disk: disk.clone(),
            disk_manager: disk_manager.clone(),
            first_page_num,
        }
    }

    pub fn cursor(&self) -> Cursor<BLOCKSIZE, CAPACITY> {
        let block = Node::read_from_disk(true, self.first_page_num, &self.disk);
        Cursor::new(
            block.cell_count(),
            self.first_page_num,
            &self.disk,
            
        )
    }

    pub fn insert(&mut self, cell: Cell) {
        // Traverse to the last page
        // If the last page is full, allocate a new page
        // Write the cell to the last page
        let mut head =
            Node::read_from_disk(true, self.first_page_num, &self.disk);
        let first_block = head.tail_page() == self.first_page_num;

        let mut node = Node::read_from_disk(
            first_block,
            head.tail_page(),
            &self.disk,
            
        );
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
                let mut new_node =
                    Node::new(false, new_block as u32, &self.disk);
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
                let mut new_node =
                    Node::new(false, new_block as u32, &self.disk);
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
}

#[cfg(test)]
mod tests {
    use rand::Rng;

    use super::*;

    #[test]
    fn simple_read() {
        let disk = Disk::<512, 65536>::create("test_simple_read").unwrap();
        let disk_manager = FreeSpaceManager::init(&disk);
        let mut file = File::init(&disk, &disk_manager);
        let record = Cell::new(vec![1, 2, 3]);
        file.insert(record);
        let mut cursor = file.cursor();
        let record = cursor.read();
        let block = disk.read_block(file.first_page_num as usize).unwrap();
    }

    #[test]
    fn edge_case() {
        let disk = Disk::<512, 65536>::create("test_edge_case").unwrap();
        let disk_manager = FreeSpaceManager::init(&disk);
        let mut file = File::init(&disk, &disk_manager);
    }

    #[test]
    fn complete_read() {
        let disk = Disk::<4096, 65536>::create("test_complete_read").unwrap();
        let disk_manager = FreeSpaceManager::init(&disk);
        let mut file = File::init(&disk, &disk_manager);
    }

    #[test]
    fn random_insert_read() {
        let disk = Disk::<4096, 819200>::create("test_random_insert_read").unwrap();
        let disk_manager = FreeSpaceManager::init(&disk);
        let mut file = File::init(&disk, &disk_manager);
        let mut rng = rand::thread_rng();
    }
}
