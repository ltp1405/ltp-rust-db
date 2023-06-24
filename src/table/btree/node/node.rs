use std::{
    mem::size_of,
    ptr::slice_from_raw_parts_mut,
    rc::Rc,
    sync::{Arc, Mutex},
};

use ltp_rust_db_page::{
    page::{Page, PAGE_SIZE},
    pager::Pager,
};

impl std::fmt::Display for Node {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let page = self
            .pager
            .lock()
            .unwrap()
            .get_page(self.page_num as usize)
            .unwrap();
        match self.node_type() {
            NodeType::Interior => {
                let mut buf = String::new();
                buf.push_str(&format!("INTERIOR({} cells, {} child[ren]]):\n", self.num_cells(), self.num_cells() + 1));
                for i in 0..self.num_cells() {
                    let cell = self.cell_at(&page, self.cell_pointer(i));
                    buf.push_str(&format!("{:?}", cell));
                }
                f.write_str(&buf)
            }
            NodeType::Leaf => todo!(),
        }
    }
}

use super::{
    cell::Cell, CellPointer, CellsCount, NodePointer, NodeType, CELL_CONTENT_START, CELL_NUMS,
    CELL_POINTERS_ARRAY_OFFSET, CELL_POINTER_SIZE, NODE_TYPE, RIGHT_MOST_CHILD_POINTER,
};

#[derive(Debug)]
enum InsertDecision {
    Normal,
    Split,
    Overflow(usize),
}

/// Each node of the btree is contained inside 1 page
#[derive(Debug)]
pub struct Node {
    pager: Arc<Mutex<Pager>>,
    page_num: NodePointer,
}

#[derive(Debug, PartialEq)]
pub enum Slot {
    /// Represent a slot which is not occupied by a key yet
    Hole(u32),
    /// Represent a slot which is occupied by a key
    Cell(u32),
}

#[derive(Debug)]
enum InsertResult {
    KeyExisted(Node),
    Normal(Node),
    Splitted(NodePointer, Node, Node),
}

impl Node {
    pub fn new(page_num: usize, pager: Arc<Mutex<Pager>>) -> Self {
        let mut node = Node {
            page_num: page_num as u32,
            pager,
        };
        if node.cell_content_start() == 0 {
            node.set_cell_content_start(PAGE_SIZE as u32);
        }
        node
    }

    pub fn node_type(&self) -> NodeType {
        let page = self
            .pager
            .lock()
            .unwrap()
            .get_page(self.page_num as usize)
            .unwrap();
        unsafe { page.read_val_at(NODE_TYPE.0) }
    }

    pub fn set_node_type(&mut self, node_type: NodeType) {
        let page = self
            .pager
            .lock()
            .unwrap()
            .get_page(self.page_num as usize)
            .unwrap();
        unsafe {
            page.write_val_at::<NodeType>(NODE_TYPE.0, node_type);
        }
    }

    // pub fn parent_pointer(&self) -> u32 {
    //     unsafe { self.page.read_val_at(PARENT_POINTER.0) }
    // }

    // pub fn set_parent_pointer(&mut self, parent_pointer: u32) {
    //     unsafe {
    //         self.page.write_val_at(PARENT_POINTER.0, parent_pointer);
    //     }
    // }

    pub fn num_cells(&self) -> CellsCount {
        let page = self
            .pager
            .lock()
            .unwrap()
            .get_page(self.page_num as usize)
            .unwrap();
        unsafe { page.read_val_at(CELL_NUMS.0) }
    }

    pub fn set_num_cells(&mut self, num_cells: u32) {
        let page = self
            .pager
            .lock()
            .unwrap()
            .get_page(self.page_num as usize)
            .unwrap();
        unsafe {
            page.write_val_at(CELL_NUMS.0, num_cells);
        }
    }

    fn cell_pointer_offset(&self, cell_num: u32) -> usize {
        let val = CELL_POINTERS_ARRAY_OFFSET + CELL_POINTER_SIZE * cell_num as usize;
        val
    }

    fn cell_bound(&self, cell_num: u32) -> (usize, usize) {
        // if cell_num > self.num_cells() {
        //     panic!("Cell index out of bound");
        // }
        // let target = self.cell_pointer(cell_num) as usize;
        // let mut min = PAGE_SIZE;
        // for cell_num in 0..self.num_cells() {
        //     let off = self.cell_pointer(cell_num) as usize;
        //     if off <= target {
        //         continue;
        //     }
        //     if off < min {
        //         min = off
        //     }
        // }
        let page = self
            .pager
            .lock()
            .unwrap()
            .get_page(self.page_num as usize)
            .unwrap();
        let target = self.cell_pointer(cell_num) as usize;
        let cell = self.cell_at(&page, cell_num);
        (target, cell.cell_size() as usize)
    }

    fn right_child(&self) -> NodePointer {
        let page = self
            .pager
            .lock()
            .unwrap()
            .get_page(self.page_num as usize)
            .unwrap();
        unsafe { page.read_val_at(RIGHT_MOST_CHILD_POINTER.0) }
    }

    fn set_right_child(&self, child: NodePointer) {
        let page = self
            .pager
            .lock()
            .unwrap()
            .get_page(self.page_num as usize)
            .unwrap();
        unsafe { page.write_val_at(RIGHT_MOST_CHILD_POINTER.0, child) }
    }

    pub fn search(&self, search_key: u32) -> Slot {
        let page = self
            .pager
            .lock()
            .unwrap()
            .get_page(self.page_num as usize)
            .unwrap();
        let num_cells = self.num_cells();
        if num_cells == 0 {
            return Slot::Hole(0);
        }
        let mut hi = num_cells;
        let mut lo = 0;
        loop {
            let mid = (lo + hi) / 2;
            let mid_key = self.cell_at(&page, mid).key();
            if search_key < mid_key {
                if mid == 0 {
                    return Slot::Hole(0);
                } else if search_key > self.cell_at(&page, mid - 1).key() {
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

    pub fn leaf_split_to(node: Node) -> (Node, Node) {
        let right = node.pager.lock().unwrap().get_free_page().unwrap();
        let right = Node::new(right as usize, node.pager.clone());
        todo!()
    }

    pub fn get_children(&self) -> Vec<NodePointer> {
        todo!()
    }

    pub fn insert_cell_pointer(&mut self, hole: u32, pointer: u32) {
        self.set_num_cells(self.num_cells() + 1);
        let cell_num = self.num_cells();
        for cell_i in (hole + 1..cell_num).rev() {
            self.set_cell_pointer(cell_i, self.cell_pointer(cell_i - 1));
        }
        self.set_cell_pointer(hole, pointer);
    }

    /// Insert a payload into a leaf node
    /// Return a normal node if insert normally
    /// Return a pair of node if need split
    fn leaf_insert(
        mut self,
        key: u32,
        payload: &[u8],
        overflow_head: Option<NodePointer>,
    ) -> InsertResult {
        match self.insert_decision(payload.len()) {
            InsertDecision::Normal => {
                let hole = self.search(key);
                let hole = match hole {
                    Slot::Hole(hole) => hole,
                    Slot::Cell(_cell) => panic!(),
                };
                let cell_start = self.cell_content_start();
                let page = self
                    .pager
                    .lock()
                    .unwrap()
                    .get_page(self.page_num as usize)
                    .unwrap();
                let cell = Cell::insert_table_leaf(
                    &page,
                    cell_start as usize,
                    key,
                    payload.len() as u32,
                    overflow_head,
                    &payload,
                );
                let cell_start = cell_start - (cell.header_size() as u32) - payload.len() as u32;
                self.set_cell_content_start(cell_start);
                self.insert_cell_pointer(hole, cell_start as u32);
                InsertResult::Normal(self)
            }
            InsertDecision::Overflow(kept_size) => {
                let hole = self.search(key);
                let hole = match hole {
                    Slot::Hole(hole) => hole,
                    Slot::Cell(_cell) => panic!(),
                };
                let mut pager = self.pager.lock().unwrap();
                let page = pager.get_page(self.page_num as usize).unwrap();
                let payload_len = payload.len();
                let (non_overflow, overflow) = payload.split_at(kept_size);
                let cell_start = self.cell_content_start();
                let new_page = pager.get_free_page().unwrap();
                let cell = Cell::insert_table_leaf(
                    &page,
                    cell_start as usize,
                    key,
                    payload_len as u32,
                    Some(new_page as u32),
                    non_overflow,
                );
                // TODO: Handle remain payload
                drop(pager);
                let cell_start = cell_start - (cell.header_size() as u32) - payload.len() as u32;
                self.set_cell_content_start(cell_start);
                self.insert_cell_pointer(hole, cell_start as u32);
                InsertResult::Normal(self)
            }
            InsertDecision::Split => {
                let page = self
                    .pager
                    .lock()
                    .unwrap()
                    .get_page(self.page_num as usize)
                    .unwrap();
                let new_page = self.pager.lock().unwrap().get_free_page().unwrap();
                let mut new_node = Node::new(new_page, self.pager.clone());
                let mid = self.num_cells() / 2;
                for i in mid..self.num_cells() {
                    let cell = Cell::table_leaf_at(&page, self.cell_pointer(i) as usize);
                    new_node = if let InsertResult::Normal(node) = new_node.leaf_insert(
                        cell.key(),
                        cell.not_overflowed_payload(),
                        cell.overflow_page_head(),
                    ) {
                        node
                    } else {
                        unreachable!()
                    };
                }
                // TODO: Handle hole after split
                let cell_bound = self.cell_bound(mid);
                let mid_key = Cell::table_leaf_at(&page, cell_bound.0).key();
                self.set_num_cells(mid);

                if key >= mid_key {
                    new_node = match new_node.leaf_insert(key, payload, overflow_head) {
                        InsertResult::Normal(node) => node,
                        _ => unreachable!("Maybe overflow calculation go wrong"),
                    }
                } else {
                    self = match self.leaf_insert(key, payload, overflow_head) {
                        InsertResult::Normal(node) => node,
                        _ => unreachable!("Maybe overflow calculation go wrong"),
                    }
                };
                InsertResult::Splitted(mid_key, self, new_node)
            }
        }
    }

    fn cell_at<'a>(&'a self, page: &'a Page, cell_num: u32) -> Cell<'a> {
        let offset = self.cell_pointer(cell_num);
        match self.node_type() {
            NodeType::Leaf => Cell::table_leaf_at(page, offset as usize),
            NodeType::Interior => Cell::table_interior_at(page, offset as usize),
        }
    }

    fn interior_insert(mut self, key: u32, child: NodePointer) -> InsertResult {
        match self.insert_decision(size_of::<u32>()) {
            InsertDecision::Normal => {
                let hole = self.search(key);
                let hole = match hole {
                    Slot::Hole(hole) => hole,
                    Slot::Cell(_cell) => return InsertResult::KeyExisted(self),
                };
                let cell_start = self.cell_content_start();
                let page = self
                    .pager
                    .lock()
                    .unwrap()
                    .get_page(self.page_num as usize)
                    .unwrap();
                println!("{:?}", cell_start);
                let cell = Cell::insert_table_interior(&page, cell_start as usize, key, child);
                let cell_start = cell_start - (cell.header_size() as u32);
                self.set_cell_content_start(cell_start);
                self.insert_cell_pointer(hole, cell_start as u32);
                InsertResult::Normal(self)
            }
            InsertDecision::Overflow(_kept_size) => {
                unreachable!()
            }
            InsertDecision::Split => {
                let page = self
                    .pager
                    .lock()
                    .unwrap()
                    .get_page(self.page_num as usize)
                    .unwrap();
                let new_page = self.pager.lock().unwrap().get_free_page().unwrap();
                let mut new_node = Node::new(new_page, self.pager.clone());
                let mid = self.num_cells() / 2;
                for i in mid..self.num_cells() {
                    let cell = self.cell_at(&page, self.cell_pointer(i));
                    new_node = if let InsertResult::Normal(node) = new_node.leaf_insert(
                        cell.key(),
                        cell.not_overflowed_payload(),
                        cell.overflow_page_head(),
                    ) {
                        node
                    } else {
                        unreachable!()
                    };
                }
                // TODO: Handle hole after split
                let cell_bound = self.cell_bound(mid);
                let mid_key = self.cell_at(&page, cell_bound.0 as u32).key();
                self.set_num_cells(mid);

                if key >= mid_key {
                    new_node = match new_node.interior_insert(key, child) {
                        InsertResult::Normal(node) => node,
                        _ => unreachable!("Maybe overflow calculation go wrong"),
                    }
                } else {
                    self = match self.interior_insert(key, child) {
                        InsertResult::Normal(node) => node,
                        _ => unreachable!("Maybe overflow calculation go wrong"),
                    }
                };
                InsertResult::Splitted(mid_key, self, new_node)
            }
        }
    }

    fn node_insert(self, key: u32, payload: &[u8]) -> InsertResult {
        let node_type = self.node_type();
        match node_type {
            NodeType::Leaf => return self.leaf_insert(key, payload, None),
            NodeType::Interior => {
                // Find the child to insert the payload into
                let hole = self.search(key);
                let hole = match hole {
                    Slot::Hole(hole) => hole,
                    Slot::Cell(cell) => cell,
                };
                let result = {
                    let mut pager = self.pager.lock().unwrap();
                    let page = pager.get_page(self.page_num as usize).unwrap();
                    let cell = self.cell_at(&page, hole);
                    let to_insert_node = Node::new(cell.child() as usize, self.pager.clone());
                    to_insert_node.node_insert(key, payload)
                };
                match result {
                    InsertResult::Normal(node) => InsertResult::Normal(node),
                    InsertResult::Splitted(returned_key, left, right) => {
                        let mut pager = self.pager.lock().unwrap();
                        let page = pager.get_page(self.page_num as usize).unwrap();
                        if hole >= self.num_cells() {
                            self.set_right_child(right.page_num);
                        } else {
                            self.cell_at(&page, hole).set_child(right.page_num);
                        }
                        drop(pager);
                        self.interior_insert(returned_key, left.page_num)
                    }
                    InsertResult::KeyExisted(key) => InsertResult::KeyExisted(key),
                }
            }
        }
    }

    fn overflow_amount(&self, payload_size: u32) -> Option<u32> {
        let free_size = self.free_size();
        if payload_size < free_size as u32 - 12 {
            None
        } else {
            Some(payload_size - free_size as u32 + 200)
        }
    }

    fn min_threshold_for_non_overflow(&self) -> usize {
        let m = ((PAGE_SIZE - 12) * 32 / 255) - 23;
        println!("{}", m);
        m
    }

    fn cell_content_start(&self) -> u32 {
        let page = self
            .pager
            .lock()
            .unwrap()
            .get_page(self.page_num as usize)
            .unwrap();
        unsafe { page.read_val_at(CELL_CONTENT_START.0) }
    }

    fn set_cell_content_start(&mut self, val: u32) {
        let page = self
            .pager
            .lock()
            .unwrap()
            .get_page(self.page_num as usize)
            .unwrap();
        unsafe { page.write_val_at(CELL_CONTENT_START.0, val) }
    }

    fn set_cell_pointer(&mut self, cell_num: u32, val: u32) {
        let page = self
            .pager
            .lock()
            .unwrap()
            .get_page(self.page_num as usize)
            .unwrap();
        unsafe {
            page.write_val_at(self.cell_pointer_offset(cell_num), val);
        }
    }

    fn free_size(&self) -> usize {
        self.cell_content_start() as usize - self.cell_pointer_offset(self.num_cells())
    }

    fn cell_pointer(&self, cell_num: u32) -> CellPointer {
        let page = self
            .pager
            .lock()
            .unwrap()
            .get_page(self.page_num as usize)
            .unwrap();
        unsafe { page.read_val_at(self.cell_pointer_offset(cell_num)) }
    }

    fn insert_decision(&self, payload_size: usize) -> InsertDecision {
        if payload_size > 100 {
            InsertDecision::Overflow(30)
        } else if self.num_cells() > 3 {
            InsertDecision::Split
        } else {
            InsertDecision::Normal
        }
    }
}

#[cfg(test)]
mod node {
    use std::sync::{Arc, Mutex};

    use ltp_rust_db_page::pager::Pager;

    use crate::table::btree::node::{node::InsertResult, NodeType};

    use super::Node;

    #[test]
    fn interior_insert() {
        let pager = Arc::new(Mutex::new(Pager::init("insert_ptr")));
        let node = Node::new(0, pager.clone());
        let node = match node.leaf_insert(533, &[2; 50], None) {
            InsertResult::Normal(node) => node,
            _ => unreachable!(),
        };
        let node = match node.leaf_insert(22, &[1; 12], None) {
            InsertResult::Normal(node) => node,
            _ => unreachable!(),
        };
        let node = match node.leaf_insert(12, &[9; 3], None) {
            InsertResult::Normal(node) => node,
            _ => unreachable!(),
        };
        let node = match node.leaf_insert(124, &[1, 2, 5, 6], None) {
            InsertResult::Normal(node) => node,
            _ => unreachable!(),
        };
    }

    #[test]
    fn insert_ptr() {
        let pager = Arc::new(Mutex::new(Pager::init("insert_ptr")));
        let mut node = Node::new(0, pager.clone());
        node.insert_cell_pointer(0, 12);
        assert_eq!(node.cell_pointer(0), 12);
        assert_eq!(node.num_cells(), 1);

        node.insert_cell_pointer(1, 15);
        assert_eq!(node.cell_pointer(1), 15);
        assert_eq!(node.num_cells(), 2);

        node.insert_cell_pointer(0, 1521);
        assert_eq!(node.cell_pointer(0), 1521);
        assert_eq!(node.num_cells(), 3);

        node.insert_cell_pointer(2, 643);
        assert_eq!(node.cell_pointer(2), 643);
        assert_eq!(node.num_cells(), 4);

        let page = pager.lock().unwrap().get_page(0).unwrap();
    }

    #[test]
    fn insert_single_leaf_interior() {
        let pager = Arc::new(Mutex::new(Pager::init("insert_ptr")));
        let page1 = pager.lock().unwrap().get_free_page().unwrap();
        let page2 = pager.lock().unwrap().get_free_page().unwrap();
        let mut node = Node::new(0, pager.clone());
        node.set_node_type(NodeType::Interior);
        let node = if let InsertResult::Normal(node) = node.interior_insert(22, page1 as u32) {
            node
        } else {
            unreachable!()
        };
        let node = if let InsertResult::Normal(node) = node.interior_insert(12, page2 as u32) {
            node
        } else {
            unreachable!()
        };
        let node = if let InsertResult::Normal(node) = node.interior_insert(12, page2 as u32) {
            node
        } else {
            unreachable!()
        };
        let page = pager.lock().unwrap().get_page(0).unwrap();
        let cell = node.cell_at(&page, 0);
        assert_eq!(cell.key(), 22);
        assert_eq!(cell.key(), 22);
        assert_eq!(cell.child(), 12);
        assert_eq!(cell.child(), 12);
        for i in 0..node.num_cells() - 1 {
            let lo = node.cell_at(&page, i).key();
            let hi = node.cell_at(&page, i + 1).key();
            assert!(lo < hi, "Key should be sorted: {} > {}", lo, hi);
        }
    }

    #[test]
    fn single_leaf_insert() {
        let pager = Arc::new(Mutex::new(Pager::init("insert_ptr")));
        let node = Node::new(0, pager.clone());
        let node =
            if let InsertResult::Normal(node) = node.leaf_insert(22, &[1, 2, 3, 4, 5, 6], None) {
                node
            } else {
                unreachable!()
            };
        let page = pager.lock().unwrap().get_page(0).unwrap();
        let cell = node.cell_at(&page, 0);
        assert_eq!(cell.not_overflowed_payload(), &[1, 2, 3, 4, 5, 6],);
    }

    #[test]
    fn leaf_insert_cell() {
        let pager = Arc::new(Mutex::new(Pager::init("insert_ptr")));
        let node = Node::new(0, pager.clone());
        let node =
            if let InsertResult::Normal(node) = node.leaf_insert(22, &[1, 2, 3, 4, 5, 6], None) {
                node
            } else {
                unreachable!()
            };
        let node = if let InsertResult::Normal(node) = node.leaf_insert(12, &[1, 2, 3], None) {
            node
        } else {
            unreachable!()
        };
        let node = if let InsertResult::Normal(node) = node.leaf_insert(124, &[1, 2, 5, 6], None) {
            node
        } else {
            unreachable!()
        };
        let page = pager.lock().unwrap().get_page(0).unwrap();
        let cell = node.cell_at(&page, 0);
        assert_eq!(cell.not_overflowed_payload(), &[1, 2, 3],);
        let cell = node.cell_at(&page, 1);
        assert_eq!(cell.not_overflowed_payload(), &[1, 2, 3, 4, 5, 6]);
        for i in 0..node.num_cells() - 1 {
            let lo = node.cell_at(&page, i).key();
            let hi = node.cell_at(&page, i + 1).key();
            assert!(lo < hi, "Key should be sorted: {} > {}", lo, hi);
        }
    }

    #[test]
    fn leaf_insert_split() {
        let pager = Arc::new(Mutex::new(Pager::init("insert_ptr")));
        let node = Node::new(0, pager.clone());
        let node = match node.leaf_insert(533, &[2; 50], None) {
            InsertResult::Normal(node) => node,
            _ => unreachable!(),
        };
        let node = match node.leaf_insert(22, &[1; 12], None) {
            InsertResult::Normal(node) => node,
            _ => unreachable!(),
        };
        let node = match node.leaf_insert(12, &[9; 3], None) {
            InsertResult::Normal(node) => node,
            _ => unreachable!(),
        };
        let node = match node.leaf_insert(124, &[1, 2, 5, 6], None) {
            InsertResult::Normal(node) => node,
            _ => unreachable!(),
        };
        let node = match node.leaf_insert(5, &[4; 15], None) {
            InsertResult::Splitted(k, l, r) => {
                let new_page = pager.lock().unwrap().get_free_page().unwrap();
                let mut node = Node::new(new_page, pager.clone());
                node.set_node_type(NodeType::Interior);
                node.set_right_child(r.page_num);

                let page = pager.lock().unwrap().get_page(node.page_num as usize);
                println!("{:?}", page);
                let node = match node.interior_insert(k, l.page_num) {
                    InsertResult::Normal(node) => node,
                    _ => unreachable!(),
                };
                node
            }
            _ => unreachable!(),
        };
        println!("{}", node);
        panic!()
    }
}
