mod node;

use std::{
    fmt::Formatter,
    sync::{Arc, Mutex},
};

use crate::{buffer_manager::BufferManager, disk_manager::DiskManager};

use self::node::{InsertResult, Node, NodePointer, NodeType};

#[derive(Debug, PartialEq)]
pub struct RowAddress {
    page_number: u32,
    offset: u32,
}

impl RowAddress {
    pub fn new(page_number: u32, offset: u32) -> Self {
        Self {
            page_number,
            offset,
        }
    }

    pub fn page_number(&self) -> u32 {
        self.page_number
    }

    pub fn offset(&self) -> u32 {
        self.offset
    }
}

pub struct BTree<'a, const BLOCKSIZE: usize, const CAPACITY: usize, const MEMORY_CAPACITY: usize> {
    root_ptr: NodePointer,
    disk_manager: &'a DiskManager<BLOCKSIZE, CAPACITY>,
    buffer_manager: &'a BufferManager<'a, BLOCKSIZE, CAPACITY, MEMORY_CAPACITY>,
}

impl<'a, const BLOCKSIZE: usize, const CAPACITY: usize, const MEMORY_CAPACITY: usize>
    std::fmt::Debug for BTree<'a, BLOCKSIZE, CAPACITY, MEMORY_CAPACITY>
{
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let root = Node::from(self.buffer_manager, self.disk_manager, self.root_ptr);
        f.debug_struct("BTree").field("root", &root).finish()
    }
}

#[derive(Debug)]
pub struct KeyExistedError;

impl<'a, const BLOCKSIZE: usize, const CAPACITY: usize, const MEMORY_CAPACITY: usize>
    BTree<'a, BLOCKSIZE, CAPACITY, MEMORY_CAPACITY>
{
    pub fn init(
        buffer_manager: &'a BufferManager<'a, BLOCKSIZE, CAPACITY, MEMORY_CAPACITY>,
        disk_manager: &'a DiskManager<BLOCKSIZE, CAPACITY>,
    ) -> Self {
        let root = Node::new(NodeType::Leaf, buffer_manager, disk_manager);
        Self {
            root_ptr: root.page_number,
            disk_manager,
            buffer_manager,
        }
    }

    pub fn find_row_address(&self, key: &[u8]) -> Option<RowAddress> {
        let root = Node::from(self.buffer_manager, self.disk_manager, self.root_ptr);
        root.find_row_address(key)
    }

    pub fn insert(&mut self, key: &[u8], row_address: RowAddress) -> Result<(), KeyExistedError> {
        let root = Node::from(self.buffer_manager, self.disk_manager, self.root_ptr);
        self.root_ptr = {
            let result = root.node_insert(key, row_address);
            match result {
                InsertResult::Normal(node) => node.page_number,
                InsertResult::Splitted(key, left, right) => {
                    let new_node =
                        Node::new(NodeType::Interior, self.buffer_manager, self.disk_manager);
                    new_node.set_right_child(right.page_number);
                    let node = match new_node.interior_insert(&key, left.page_number, None) {
                        InsertResult::Normal(node) => node,
                        _ => unreachable!(),
                    };
                    node.page_number
                }
                InsertResult::KeyExisted(_key) => return Err(KeyExistedError),
            }
        };
        Ok(())
    }
}

#[test]
fn basic_insert() {
    use rand::Rng;
    env_logger::init();

    let mut rng = rand::thread_rng();

    const BLOCK_SIZE: usize = 512;
    const DISK_CAPACITY: usize = 512 * 512;
    const MEMORY_CAPACITY: usize = 512 * 16;

    let memory = [0; MEMORY_CAPACITY];
    let disk = disk::Disk::<BLOCK_SIZE, DISK_CAPACITY>::create("btree_basic_insert").unwrap();
    let buffer_manager: BufferManager<'_, BLOCK_SIZE, DISK_CAPACITY, MEMORY_CAPACITY> =
        BufferManager::init(&memory, &disk);
    let disk_manager = DiskManager::init(&disk);
    let mut btree = BTree::init(&buffer_manager, &disk_manager);
    let mut keys = Vec::new();
    for i in 0..1000 {
        let mut key: [u8; 100] = [0; 100];
        for j in 0..100 {
            key[j] = rng.gen::<u8>() % 128;
        }
        keys.push(key);
        if let Err(_) = btree.insert(&key, RowAddress::new(0, i as u32)) {
            continue;
        }
    }

    for i in 0..1000 {
        let row = btree.find_row_address(&keys[i]).unwrap();
        assert_eq!(row.page_number(), 0);
        assert_eq!(row.offset(), i as u32);
    }
}
