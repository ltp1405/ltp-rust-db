use std::{mem::size_of, rc::Rc, sync::Mutex};

use crate::page::{Page, Pager};

use super::{
    cell::Cell, CellsCount, NodePointer, NodeType, CELL_NUMS, CELL_POINTER_SIZE, IS_ROOT,
    LEAF_NODE_CELL_SIZE, LEAF_NODE_HEADER_SIZE, NODE_TYPE, PARENT_POINTER,
};

/// Each node of the btree is contained inside 1 page
pub struct Node<'a> {
    pager: Rc<Mutex<Pager>>,
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
    pub fn new(page: &'a mut Page, pager: Rc<Mutex<Pager>>) -> Self {
        Node { page, pager }
    }

    pub fn is_root(&self) -> bool {
        unsafe { self.page.read_val_at(IS_ROOT.0) }
    }

    pub fn set_is_root(&mut self, is_root: bool) {
        unsafe {
            self.page.write_val_at(IS_ROOT.0, is_root);
        }
    }

    pub fn node_type(&self) -> NodeType {
        unsafe { self.page.read_val_at(NODE_TYPE.0) }
    }

    pub fn set_node_type(&mut self, node_type: NodeType) {
        unsafe {
            self.page.write_val_at(NODE_TYPE.0, node_type);
        }
    }

    pub fn parent_pointer(&self) -> u32 {
        unsafe { self.page.read_val_at(PARENT_POINTER.0) }
    }

    pub fn set_parent_pointer(&mut self, parent_pointer: u32) {
        unsafe {
            self.page.write_val_at(PARENT_POINTER.0, parent_pointer);
        }
    }

    pub fn num_cells(&self) -> CellsCount {
        unsafe { self.page.read_val_at(CELL_NUMS.0) }
    }

    pub fn set_num_cells(&mut self, num_cells: u32) {
        unsafe {
            self.page.write_val_at(CELL_NUMS.0, num_cells);
        }
    }

    fn cell_pointer_offset(&self, cell_num: u32) -> usize {
        LEAF_NODE_HEADER_SIZE + (cell_num as usize * CELL_POINTER_SIZE)
    }

    fn cell(&self, cell_num: u32) -> Cell {
        let offset = self.cell_pointer_offset(cell_num);
        unsafe { Cell::at(&self.page, offset) }
    }

    pub fn search(&self, search_key: u32) -> Slot {
        let num_cells = self.num_cells();
        if num_cells == 0 {
            return Slot::Hole(0);
        }
        let mut hi = num_cells;
        let mut lo = 0;
        loop {
            let mid = (lo + hi) / 2;
            let mid_key = unsafe { self.cell(mid).key() };
            if search_key < mid_key {
                if mid == 0 {
                    return Slot::Hole(0);
                } else if search_key > unsafe { self.cell(mid - 1).key() } {
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

    pub fn split(&mut self) {
        // - If the need spliting node is the root, create a new node and set that to root.
        // - Create 2 children node
        // - Copy cell from first half to left, 2nd half to right
        // - Else, create 1 children node, copy the 2nd
        // - Create new interior cell, data = pointer to left node, insert to parent node
        // - Modify cell which point to this node to point to 2nd half
        let mut pager = self.pager.lock().unwrap();
        if self.is_root() {
            let (left_page_num, left_page) = pager.get_free_page().unwrap();
            let left_child = Node::new(&mut left_page, self.pager);
            let (right_page_num, right_page) = pager.get_free_page().unwrap();
            let right_child = Node::new(&mut right_page, self.pager);
            let mid_index = self.num_cells() / 2;
            let (first_half, second_half) = self.get_children().split_at(mid_index as usize);
            for node_addr in first_half {
                let cell = unsafe { Cell::at(&self.page, *node_addr as usize) };
                let (key, payload) = unsafe { (cell.key(), cell.payload()) };
                left_child.insert(key, payload.into())
            }
            for node_addr in second_half {
                let cell = unsafe { Cell::at(&self.page, *node_addr as usize) };
                let (key, payload) = unsafe { (cell.key(), cell.payload()) };
                right_child.insert(key, payload.into())
            }

            self.set_is_root(true);
            self.set_num_cells(0);
            let mid_node_key = unsafe { self.cell(mid_index).key() };
            self.insert(mid_node_key, left_page_num.to_be_bytes().to_vec())
        } else {
        }
    }

    pub fn get_children(&self) -> Vec<NodePointer> {
        todo!()
    }

    pub fn need_split(&self, payload_size: u32) -> bool {
        false
    }

    pub fn insert(&mut self, key: u32, payload: Vec<u8>) {
        let num_cells = self.num_cells();
        let payload_size = size_of::<u32>() + payload.len();
        if self.need_split(payload_size as u32) {
            let is_root = self.is_root();
            if is_root {
                self.split();
            } else {
                let mut pager = self.pager.lock().unwrap();
                let mut parent_page = pager.get_page_mut(self.parent_pointer() as usize).unwrap();
                let mut parent_node = Node::new(&mut parent_page, self.pager.clone());
                parent_node.insert(key, payload);
            }
        }
        let cell_num: u32 = if let Slot::Hole(hole) = self.search(key) {
            println!("{}", hole);
            hole
        } else {
            panic!("Key already inserted");
        };
        let (not_overflowed_payload, remaining_payload) = self.fragment_payload(payload);
        let cell = Cell::new_table_leaf(key, not_overflowed_payload, None);

        let payload_start = self.cell_content_start() - cell.size();

        self.set_num_cells(self.num_cells() + 1);
    }

    pub fn get(&self, key: u32) -> &[u8] {
        todo!()
    }

    fn fragment_payload(&self, payload: Vec<u8>) -> (Vec<u8>, Option<Vec<Vec<u8>>>) {
        todo!()
    }
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
        node.set_num_cells(4);
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
        node.set_num_cells(5);
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
        node.set_node_type(NodeType::Leaf);
        assert_eq!(node.node_type(), NodeType::Leaf);
    }

    #[test]
    fn parent_pointer() {
        let mut page = Page::init();
        let mut node = Node::new(&mut page);
        node.set_parent_pointer(10);
        assert_eq!(node.parent_pointer(), 10);
    }

    #[test]
    fn cell_nums() {
        let mut page = Page::init();
        let mut node = Node::new(&mut page);
        node.set_num_cells(10);
        assert_eq!(node.num_cells(), 10);
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
        for i in 0..node.num_cells() {
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
