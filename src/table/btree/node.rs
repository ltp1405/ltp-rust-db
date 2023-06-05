use std::mem::size_of;

use crate::page::{Page, PAGE_SIZE};
use crate::table::ROW_SIZE;

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

#[derive(Debug, PartialEq)]
pub enum Slot {
    /// Represent a slot which is not occupied by a key yet
    Hole(u32),
    /// Represent a slot which is occupied by a key
    Cell(u32),
}

impl<'a> Node<'a> {
    pub fn new(page: &'a mut Page) -> Self {
        Node { page }
    }

    pub fn read_node_type(&self) -> NodeType {
        unsafe { self.page.read_val_at(NODE_TYPE.0) }
    }

    pub fn write_node_type(&mut self, node_type: NodeType) {
        unsafe {
            self.page.write_val_at(NODE_TYPE.0, node_type);
        }
    }

    pub fn read_parent_pointer(&self) -> u32 {
        unsafe { self.page.read_val_at(PARENT_POINTER.0) }
    }

    pub fn write_parent_pointer(&mut self, parent_pointer: u32) {
        unsafe {
            self.page.write_val_at(PARENT_POINTER.0, parent_pointer);
        }
    }

    pub fn read_num_cells(&self) -> u32 {
        unsafe { self.page.read_val_at(LEAF_NODE_NUM_CELLS.0) }
    }

    pub fn write_num_cells(&mut self, num_cells: u32) {
        unsafe {
            self.page.write_val_at(LEAF_NODE_NUM_CELLS.0, num_cells);
        }
    }

    fn cell_ptr(&self, cell_num: u32) -> *const u8 {
        let buffer_ptr = self.page.as_ptr();
        unsafe { buffer_ptr.add(LEAF_NODE_HEADER_SIZE + (cell_num as usize * LEAF_NODE_CELL_SIZE)) }
    }

    fn cell_offset(&self, cell_num: u32) -> usize {
        LEAF_NODE_HEADER_SIZE + (cell_num as usize * LEAF_NODE_CELL_SIZE)
    }

    pub fn read_key(&self, cell_num: u32) -> u32 {
        println!("Key offset: {}", self.cell_offset(cell_num));
        unsafe { self.page.read_val_at(self.cell_offset(cell_num)) }
    }

    pub fn write_key(&mut self, cell_num: u32, key: u32) {
        println!("Key offset: {}", self.cell_offset(cell_num));
        unsafe {
            self.page.write_val_at(self.cell_offset(cell_num), key);
        }
    }

    fn read_val(&self, cell_num: u32) -> &[u8] {
        let val_start = self.cell_offset(cell_num) + LEAF_NODE_VAL.0;
        let val_end = val_start + LEAF_NODE_VAL.1;
        println!("{} - {}", val_start, val_end);
        &self.page[val_start..val_end]
    }

    fn read_val_raw(&self, cell_num: u32) -> *const u8 {
        let val_start = self.cell_offset(cell_num) + LEAF_NODE_VAL.0;
        let val_end = val_start + LEAF_NODE_VAL.1;
        self.page[val_start..val_end].as_ptr()
    }

    fn write_val(&mut self, cell_num: u32, val: &[u8]) {
        let val_start = self.cell_offset(cell_num) + LEAF_NODE_VAL.0;
        let val_end = val_start + LEAF_NODE_VAL.1;
        let _ = &self.page[val_start..val_end].copy_from_slice(val);
    }

    pub fn search(&self, search_key: u32) -> Slot {
        let num_cells = self.read_num_cells();
        if num_cells == 0 {
            return Slot::Hole(0);
        }
        let mut hi = num_cells;
        let mut lo = 0;
        loop {
            let mid = (lo + hi) / 2;
            let mid_key = self.read_key(mid);
            if search_key < mid_key {
                if mid == 0 {
                    return Slot::Hole(0);
                } else if search_key > self.read_key(mid - 1) {
                    return Slot::Hole(mid);
                }
                hi = mid;
            } else if search_key > mid_key {
                if mid == num_cells - 1 {
                    return Slot::Hole(num_cells);
                }
                lo = mid;
            } else {
                return Slot::Cell(mid);
            }
        }
    }

    pub fn insert(&mut self, key: u32, val: &[u8]) {
        let num_cells = self.read_num_cells();
        if num_cells >= LEAF_NODE_MAX_CELLS as u32 {
            // TODO: Implement splitting
            todo!();
        }
        let cell_num: u32 = if let Slot::Hole(hole) = self.search(key) {
            println!("{}", hole);
            hole
        } else {
            panic!("Key already inserted");
        };

        // if cell_num < num_cells {
        //     for i in (cell_num + 1..=num_cells).rev() {
        //         let key = self.read_key(i - 1);
        //         self.write_key(i, key);
        //         let val_ptr = self.read_val_raw(i - 1) as *mut u8;
        //         let new_val_ptr = self.read_val_raw(i) as *mut u8;
        //         unsafe {
        //             std::ptr::copy(val_ptr, new_val_ptr, LEAF_NODE_VAL.1);
        //         }
        //     }
        // }

        println!("{}", cell_num);
        self.write_key(cell_num, key);
        self.write_val(cell_num, val);
        assert_eq!(self.read_key(cell_num), key);

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
    use crate::{
        page::Page,
        table::{
            btree::node::{NodeType, Slot, LEAF_NODE_VAL},
            ROW_SIZE,
        },
    };

    use super::Node;

    #[test]
    fn write_val() {
        let mut page = Page::init();
        let mut node = Node::new(&mut page);
        let val = [0xff; ROW_SIZE];
        node.write_key(0, 10);
        node.write_val(0, &val);
        assert_eq!(node.read_val(0), val);
        assert_eq!(node.read_key(0), 10);
    }

    #[test]
    fn basic_search() {
        let mut page = Page::init();
        let mut node = Node::new(&mut page);
        node.write_key(0, 3);
        node.write_key(1, 9);
        node.write_key(2, 34);
        node.write_key(3, 57);
        node.write_num_cells(4);
        assert_eq!(node.search(9), Slot::Cell(1));
        assert_eq!(node.search(2), Slot::Hole(0));
        assert_eq!(node.search(6), Slot::Hole(1));
        assert_eq!(node.search(12), Slot::Hole(2));
        assert_eq!(node.search(50), Slot::Hole(3));
        assert_eq!(node.search(60), Slot::Hole(4));
    }

    #[test]
    fn basic_search2() {
        let mut page = Page::init();
        let mut node = Node::new(&mut page);
        node.write_key(0, 3);
        node.write_key(1, 9);
        node.write_key(2, 34);
        node.write_key(3, 57);
        node.write_key(4, 90);
        node.write_num_cells(5);
        assert_eq!(node.search(2), Slot::Hole(0));
        assert_eq!(node.search(6), Slot::Hole(1));
        assert_eq!(node.search(12), Slot::Hole(2));
        assert_eq!(node.search(50), Slot::Hole(3));
        assert_eq!(node.search(60), Slot::Hole(4));
        assert_eq!(node.search(100), Slot::Hole(5));
    }

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

    // #[test]
    fn simple_insert() {
        let mut page = Page::init();
        let mut node = Node::new(&mut page);
        let dummy_val = [0; ROW_SIZE];
        node.insert(10, &dummy_val);
        node.write_key(0, 10);
        // node.insert(20, &dummy_val);
        assert_eq!(node.read_key(0), 10);
        // assert_eq!(node.read_key(1), 20);
    }

    // #[test]
    fn insert() {
        let mut page = Page::init();
        let mut node = Node::new(&mut page);
        let mut val_list = vec![32523, 2, 12, 532, 32, 235];

        for v in &val_list {
            node.insert(*v, &[0; ROW_SIZE]);
        }

        val_list.sort();

        let mut key_list = Vec::new();
        for i in 0..node.read_num_cells() {
            key_list.push(node.read_key(i));
        }

        assert_eq!(val_list, key_list);
    }

    #[test]
    #[should_panic]
    fn insert_over_limit() {
        let mut page = Page::init();
        let mut node = Node::new(&mut page);
        for i in 0..100 {
            node.insert(i, &[0]);
        }
    }
}
