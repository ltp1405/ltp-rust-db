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
    disk: Disk<BLOCKSIZE, CAPACITY>,
    disk_manager: FreeSpaceManager<BLOCKSIZE, CAPACITY>,
    pub first_page_num: u32,
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
        let block = Node::read_from_disk(true, self.first_page_num, &self.disk, &self.disk_manager);
        Cursor::new(
            block.cell_count(),
            self.first_page_num,
            &self.disk,
            &self.disk_manager,
        )
    }

    pub fn insert(&mut self, record: Cell) {
        // Traverse to the last page
        // If the last page is full, allocate a new page
        // Write the record to the last page
        let mut head =
            Node::read_from_disk(true, self.first_page_num, &self.disk, &self.disk_manager);
        let first_block = head.tail_page() == self.first_page_num;

        let mut node = Node::read_from_disk(
            first_block,
            head.tail_page(),
            &self.disk,
            &self.disk_manager,
        );
        let rs = node.insert(&record);
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
            InsertResult::Spill(remain_start) => {
                let new_block = self.disk_manager.allocate().unwrap();
                let mut new_node =
                    Node::new(false, new_block as u32, &self.disk, &self.disk_manager);
                let spilled_record = &record.buf[remain_start..];
                new_node.insert_spilled(&spilled_record);

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
            InsertResult::OutOfSpace => {
                let new_block = self.disk_manager.allocate().unwrap();
                let mut new_node =
                    Node::new(false, new_block as u32, &self.disk, &self.disk_manager);
                new_node.insert(&record);
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
        println!("{:?}", block);
        assert_eq!(record.unwrap().buf, vec![1, 2, 3]);
    }

    #[test]
    fn edge_case() {
        let disk = Disk::<512, 65536>::create("test_edge_case").unwrap();
        let disk_manager = FreeSpaceManager::init(&disk);
        let mut file = File::init(&disk, &disk_manager);
        let record = Cell::new([1; 480].to_vec());
        file.insert(record);
        let record2 = Cell::new([2; 200].to_vec());
        file.insert(record2);
        let mut cursor = file.cursor();
        let record = cursor.read();
        assert_eq!(record.unwrap().buf, vec![1; 480]);
        cursor._next();
        let record = cursor.read();
        assert_eq!(record.unwrap().buf, vec![2; 200]);
    }

    #[test]
    fn complete_read() {
        let disk = Disk::<4096, 65536>::create("test_complete_read").unwrap();
        let disk_manager = FreeSpaceManager::init(&disk);
        let mut file = File::init(&disk, &disk_manager);
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
        let record = cursor.read();
        assert_eq!(record.unwrap().buf, vec![1; 2000]);

        cursor._next();
        let record = cursor.read();
        assert_eq!(record.as_ref().unwrap().size(), 2504);
        assert_eq!(record.unwrap().buf, vec![2; 2500]);

        cursor._next();
        let record = cursor.read();
        assert_eq!(record.as_ref().unwrap().buf.len(), 3000);
        assert_eq!(record.unwrap().buf, vec![3; 3000]);

        cursor._next();
        let record = cursor.read();
        assert_eq!(record.as_ref().unwrap().buf.len(), 3500);
        assert_eq!(record.unwrap().buf, vec![4; 3500]);

        cursor._next();
        let record = cursor.read();
        assert_eq!(record.as_ref().unwrap().buf.len(), 4000);
        assert_eq!(record.unwrap().buf, vec![5; 4000]);
    }

    #[test]
    fn random_insert_read() {
        let disk = Disk::<4096, 819200>::create("test_random_insert_read").unwrap();
        let disk_manager = FreeSpaceManager::init(&disk);
        let mut file = File::init(&disk, &disk_manager);
        let mut rng = rand::thread_rng();
        let mut records = vec![];
        for _ in 0..100 {
            let size = rng.gen_range(1..4000);
            let record = Cell::new(vec![1; size]);
            file.insert(record.clone());
            records.push(record);
        }
        let cursor = file.cursor();
        for (i, cell) in cursor.into_iter().enumerate() {
            assert_eq!(cell.buf, records[i].buf);
        }
    }
}
