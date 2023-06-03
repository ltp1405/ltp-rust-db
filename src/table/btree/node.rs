use std::mem::size_of;

use crate::table::{page::Page, PAGE_SIZE, ROW_SIZE};

type LeafNodeKey = u32;
type ParentPointer = u32;
type CellsCount = u32;

/// Common Node Header Layout
/// (<offset>, <size>)
static NODE_TYPE: (usize, usize) = (0, size_of::<NodeType>());
/// (<offset>, <size>)
static IS_ROOT: (usize, usize) = (NODE_TYPE.1, size_of::<bool>());
/// (<offset>, <size>)
static PARENT_POINTER: (usize, usize) = (IS_ROOT.0 + IS_ROOT.1, size_of::<ParentPointer>());
/// (<offset>, <size>)
static COMMON_NODE_HEADER_SIZE: usize = NODE_TYPE.1 + PARENT_POINTER.1 + IS_ROOT.1;

/// Leaf Node Header Layout
/// (<offset>, <size>)
static LEAF_NODE_NUM_CELLS: (usize, usize) = (COMMON_NODE_HEADER_SIZE, size_of::<CellsCount>());
/// (<offset>, <size>)
static LEAF_NODE_HEADER_SIZE: usize = COMMON_NODE_HEADER_SIZE + LEAF_NODE_NUM_CELLS.1;

/// Leaf Node Body Layout
/// (<offset>, <size>)
static LEAF_NODE_KEY: (usize, usize) = (0, size_of::<LeafNodeKey>());
/// (<offset>, <size>)
static LEAF_NODE_VAL: (usize, usize) = (LEAF_NODE_KEY.1, ROW_SIZE);
static LEAF_NODE_CELL_SIZE: usize = LEAF_NODE_KEY.1 + LEAF_NODE_VAL.1;
static LEAF_NODE_SPACE_FOR_CELLS: usize = PAGE_SIZE - LEAF_NODE_HEADER_SIZE;
static LEAF_NODE_MAX_CELLS: usize = LEAF_NODE_SPACE_FOR_CELLS / LEAF_NODE_CELL_SIZE;

/// Each node of the btree is contained inside 1 page
pub struct Node<'a> {
    page: &'a mut Page,
}

impl<'a> Node<'a> {
    pub fn new(page: &'a mut Page) -> Self {
        Node { page }
    }

    pub fn read_node_type(&self) -> NodeType {
        let buffer_ptr = self.page.buffer.as_ptr() as *const NodeType;
        unsafe { buffer_ptr.add(NODE_TYPE.0).read() }
    }

    pub fn write_node_type(&mut self, node_type: NodeType) {
        let buffer_ptr = self.page.buffer.as_ptr() as *mut NodeType;
        unsafe { *buffer_ptr.add(NODE_TYPE.0) = node_type }
    }

    pub fn read_parent_pointer(&self) -> u32 {
        let buffer_ptr = self.page.buffer.as_ptr() as *const u32;
        unsafe { buffer_ptr.add(PARENT_POINTER.0).read() }
    }

    pub fn write_parent_pointer(&mut self, parent_pointer: u32) {
        let buffer_ptr = self.page.buffer.as_ptr() as *mut u32;
        unsafe { *buffer_ptr.add(PARENT_POINTER.0) = parent_pointer }
    }

    pub fn read_num_cells(&self) -> u32 {
        let buffer_ptr = self.page.buffer.as_ptr() as *const u32;
        unsafe { *buffer_ptr.add(LEAF_NODE_NUM_CELLS.0) }
    }

    pub fn write_num_cells(&mut self, num_cells: u32) {
        let buffer_ptr = self.page.buffer.as_mut_ptr() as *mut u32;
        unsafe { *buffer_ptr.add(LEAF_NODE_NUM_CELLS.0) = num_cells }
    }

    fn cell_ptr(&self, cell_num: u32) -> *const u8 {
        let buffer_ptr = self.page.buffer.as_ptr();
        unsafe { buffer_ptr.add(LEAF_NODE_HEADER_SIZE + (cell_num as usize * LEAF_NODE_CELL_SIZE)) }
    }

    fn cell_pos(&self, cell_num: u32) -> usize {
        LEAF_NODE_HEADER_SIZE + (cell_num as usize * LEAF_NODE_CELL_SIZE)
    }

    pub fn read_key(&self, cell_num: u32) -> u32 {
        let cell_ptr = self.cell_ptr(cell_num) as *const u32;
        unsafe { cell_ptr.read() }
    }

    pub fn write_key(&mut self, cell_num: u32, key: u32) {
        let cell_ptr = self.cell_ptr(cell_num) as *mut u32;
        unsafe { *cell_ptr = key };
    }

    fn read_val(&self, cell_num: u32) -> &[u8] {
        let val_start = self.cell_pos(cell_num) + LEAF_NODE_VAL.0;
        let val_end = val_start + LEAF_NODE_VAL.1;
        &self.page.buffer[val_start..val_end]
    }

    fn read_val_raw(&self, cell_num: u32) -> *const u8 {
        let val_start = self.cell_pos(cell_num) + LEAF_NODE_VAL.0;
        let val_end = val_start + LEAF_NODE_VAL.1;
        self.page.buffer[val_start..val_end].as_ptr()
    }

    fn write_val(&mut self, cell_num: u32, val: &[u8]) {
        let val_start = self.cell_pos(cell_num) + LEAF_NODE_VAL.0;
        let val_end = val_start + LEAF_NODE_VAL.1;
        let _ = &self.page.buffer[val_start..val_end].copy_from_slice(val);
    }

    pub fn insert(&mut self, cell_num: u32, key: u32, val: &[u8]) {
        if cell_num >= LEAF_NODE_MAX_CELLS as u32 {
            // TODO: Implement splitting
            todo!();
        }

        let num_cells = self.read_num_cells();
        if cell_num < num_cells {
            for i in (cell_num + 1..=num_cells).rev() {
                let key = self.read_key(i - 1);
                self.write_key(i, key);
                println!("Copy slot cell {} to cell {}", i, i - 1);
                let val_ptr = self.read_val_raw(i - 1) as *mut u8;
                let new_val_ptr = self.read_val_raw(i) as *mut u8;
                unsafe {
                    std::ptr::copy(val_ptr, new_val_ptr, LEAF_NODE_VAL.1);
                }
            }
        }

        self.write_key(cell_num, key);
        self.write_val(cell_num, val);

        self.write_num_cells(num_cells + 1);
    }

    pub fn get(&self, key: u32) -> &[u8] {
        todo!()
    }
}

#[repr(u8)]
#[derive(Debug, PartialEq)]
pub enum NodeType {
    Internal,
    Leaf,
}

#[cfg(test)]
mod node {
    use crate::table::{btree::node::NodeType, page::Page, ROW_SIZE};

    use super::{Node, LEAF_NODE_MAX_CELLS};

    #[test]
    fn node_type() {
        let mut page = Page::init();
        let mut node = Node::new(&mut page);
        node.write_node_type(NodeType::Leaf);
        assert_eq!(node.read_node_type(), NodeType::Leaf);
    }

    #[test]
    fn parent_pointer() {
        let mut page = Page::init();
        let mut node = Node::new(&mut page);
        node.write_parent_pointer(10);
        assert_eq!(node.read_parent_pointer(), 10);
    }

    #[test]
    fn cell_nums() {
        let mut page = Page::init();
        let mut node = Node::new(&mut page);
        node.write_num_cells(10);
        assert_eq!(node.read_num_cells(), 10);
    }

    #[test]
    fn write_key() {
        let mut page = Page::init();
        let mut node = Node::new(&mut page);
        node.write_key(0, 10);
        assert_eq!(node.read_key(0), 10);
    }

    #[test]
    fn basic_insert() {
        let mut page = Page::init();
        let mut node = Node::new(&mut page);
        let dummy_val = [12; ROW_SIZE];
        node.insert(0, 10, &dummy_val);
        node.insert(0, 20, &dummy_val);
        let inserted_key = node.read_key(0);
        assert_eq!(inserted_key, 20);
    }

    #[test]
    fn insert_at_the_back() {
        let mut page = Page::init();
        let mut node = Node::new(&mut page);
        let dummy_val1 = [1; ROW_SIZE];
        let dummy_val2 = [2; ROW_SIZE];
        let dummy_val3 = [3; ROW_SIZE];
        node.insert(0, 10, &dummy_val1);
        node.insert(1, 20, &dummy_val2);
        node.insert(2, 30, &dummy_val3);
        let inserted_key = node.read_key(0);
        assert_eq!(inserted_key, 10);
        let inserted_key = node.read_key(1);
        assert_eq!(inserted_key, 20);
        let inserted_key = node.read_key(2);
        assert_eq!(inserted_key, 30);
    }

    #[test]
    fn insert_at_the_start() {
        let mut page = Page::init();
        let mut node = Node::new(&mut page);
        let dummy_val1 = [1; ROW_SIZE];
        let dummy_val2 = [2; ROW_SIZE];
        let dummy_val3 = [3; ROW_SIZE];
        node.insert(0, 10, &dummy_val1);
        node.insert(0, 20, &dummy_val2);
        node.insert(0, 30, &dummy_val3);

        println!("{:?}", node.page.buffer);
        assert_eq!(node.read_num_cells(), 3);
        let inserted_key = node.read_key(0);
        assert_eq!(inserted_key, 30);
        let inserted_key = node.read_key(1);
        assert_eq!(inserted_key, 20);
        let inserted_key = node.read_key(2);
        assert_eq!(inserted_key, 10);
    }

    #[test]
    fn insert_in_the_middle() {
        let mut page = Page::init();
        let mut node = Node::new(&mut page);
        let dummy_val1 = [1; ROW_SIZE];
        let dummy_val2 = [2; ROW_SIZE];
        let dummy_val3 = [3; ROW_SIZE];
        node.insert(0, 10, &dummy_val1);
        node.insert(1, 20, &dummy_val2);
        node.insert(1, 30, &dummy_val3);

        assert_eq!(node.read_num_cells(), 3);
        let inserted_key = node.read_key(0);
        assert_eq!(inserted_key, 10);
        let inserted_key = node.read_key(1);
        assert_eq!(inserted_key, 30);
        let inserted_key = node.read_key(2);
        assert_eq!(inserted_key, 20);
    }

    #[test]
    #[should_panic]
    fn insert_over_limit() {
        let mut page = Page::init();
        let mut node = Node::new(&mut page);
        node.insert(LEAF_NODE_MAX_CELLS as u32, 0, &[0]);
    }
}
