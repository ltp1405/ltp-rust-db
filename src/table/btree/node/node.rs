use std::{
    fmt::Debug,
    mem::size_of,
    sync::{Arc, Mutex},
};

use ltp_rust_db_page::{
    page::{Page, PAGE_SIZE},
    pager::Pager,
};

impl Debug for Node {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let page = self
            .pager
            .lock()
            .unwrap()
            .get_page(self.page_num as usize)
            .unwrap();
        match self.node_type() {
            NodeType::Leaf => {
                let mut cells = Vec::new();
                for i in 0..self.num_cells() {
                    let cell = Cell::table_leaf_at(&page, self.cell_pointer(i) as usize);
                    cells.push(cell);
                }
                f.debug_struct("LeafNode").field("cells", &cells).finish()
            }
            NodeType::Interior => {
                let mut children = Vec::new();
                for i in 0..self.num_cells() {
                    let cell = Cell::table_interior_at(&page, self.cell_pointer(i) as usize);
                    let node = Node::new(cell.child() as usize, self.pager.clone());
                    let key = cell.key();
                    children.push((key, node));
                }
                let right_child = Node::new(self.right_child() as usize, self.pager.clone());
                f.debug_struct("InteriorNode")
                    .field("address", &self.page_num)
                    .field("children_num", &children.len())
                    .field("children", &children)
                    .field("right_most_child", &right_child)
                    .finish()
            }
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
pub enum InsertResult {
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
        let node_type = unsafe { page.read_val_at::<NodeType>(NODE_TYPE.0) };
        if node_type as u8 == 0x2 {
            NodeType::Interior
        } else if node_type as u8 == 0x5 {
            NodeType::Leaf
        } else {
            println!("{:?}", node_type as u8);
            panic!("Invalid node type")
        }
    }

    fn set_node_type(&mut self, node_type: NodeType) {
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
        assert_eq!(self.node_type(), NodeType::Interior);
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
                todo!();
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
                new_node.set_node_type(NodeType::Leaf);
                let mid = self.num_cells() / 2;
                for i in mid..self.num_cells() {
                    let cell = Cell::table_leaf_at(&page, self.cell_pointer(i) as usize);
                    new_node = if let InsertResult::Normal(node) = new_node.leaf_insert(
                        cell.key(),
                        cell.kept_payload(),
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
        let node_type = self.node_type();
        match node_type {
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
                new_node.set_node_type(NodeType::Interior);
                let mid = self.num_cells() / 2;
                for i in mid..self.num_cells() {
                    println!("{}", self.cell_pointer(i));
                    let cell = self.cell_at(&page, i);
                    println!("{:?}", cell);
                    new_node = match new_node.interior_insert(cell.key(), cell.child()) {
                        InsertResult::Normal(node) => node,
                        _ => unreachable!(),
                    };
                }
                // TODO: Handle hole after split
                let mid_key = self.cell_at(&page, mid).key();
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
                self.set_node_type(NodeType::Interior);
                InsertResult::Splitted(mid_key, self, new_node)
            }
        }
    }

    pub fn node_insert(self, key: u32, payload: &[u8]) -> InsertResult {
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
                let right_child = self.right_child();
                let offset = self.cell_pointer(hole);
                let child = {
                    if hole >= self.num_cells() {
                        right_child
                    } else {
                        {
                            let pager = self.pager.clone();
                            let mut pager = pager.lock().unwrap();
                            let page = pager.get_page(self.page_num as usize).unwrap();
                            let cell = Cell::table_interior_at(&page, offset as usize);
                            cell.child()
                        }
                    }
                };
                let result = {
                    let to_insert_node = Node::new(child as usize, self.pager.clone());
                    to_insert_node.node_insert(key, payload)
                };
                match result {
                    InsertResult::Normal(_node) => InsertResult::Normal(self),
                    InsertResult::Splitted(returned_key, left, right) => {
                        let num_cells = self.num_cells();
                        if hole >= num_cells {
                            self.set_right_child(right.page_num);
                        } else {
                            let mut pager = self.pager.lock().unwrap();
                            let page = pager.get_page(self.page_num as usize).unwrap();
                            self.cell_at(&page, hole).set_child(right.page_num);
                        }
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

    use crate::table::btree::node::{node::InsertResult, NodePointer, NodeType};

    use super::Node;

    #[test]
    fn split_logic() {
        let pager = Arc::new(Mutex::new(Pager::init("insert_ptr")));
        let mut node = Node::new(0, pager.clone());
        node.set_node_type(NodeType::Leaf);
        let data = vec![
            (12, [1, 2, 3]),
            (222, [1, 2, 3]),
            (11, [1, 2, 3]),
            (88, [1, 2, 3]),
            (8, [1, 2, 3]),
            (1, [1, 2, 3]),
            (111, [1, 2, 3]),
            (333, [1, 2, 3]),
            (7777, [1, 2, 3]),
            (12521, [1, 2, 3]),
        ];
        for datum in &data[0..4] {
            node = match node.node_insert(datum.0, &datum.1) {
                InsertResult::Normal(node) => node,
                _ => unreachable!(),
            };
        }
        node = match node.node_insert(data[4].0, &data[4].1) {
            InsertResult::Splitted(key, left, right) => {
                let new_page = pager.lock().unwrap().get_free_page().unwrap();
                let mut node = Node::new(new_page as usize, pager.clone());
                node.set_node_type(NodeType::Interior);
                node.set_right_child(right.page_num);
                let node = match node.interior_insert(key, left.page_num) {
                    InsertResult::Normal(node) => node,
                    _ => unreachable!(),
                };
                node
            }
            _ => unreachable!(),
        };
        for datum in &data[5..] {
            node = match node.node_insert(datum.0, &datum.1) {
                InsertResult::Normal(node) => {
                    println!("{:#?}", node);
                    node
                }
                _ => unreachable!(),
            };
        }
        panic!();
    }

    #[test]
    fn interior_insert() {
        let pager = Arc::new(Mutex::new(Pager::init("insert_ptr")));
        let new_page = pager.lock().unwrap().get_free_page().unwrap();
        let node = Node::new(new_page as usize, pager.clone());
        node.set_right_child(4);
        assert_eq!(node.right_child(), 4);
        let node = match node.interior_insert(12, 42) {
            InsertResult::Normal(node) => node,
            _ => unreachable!(),
        };
        let node = match node.interior_insert(42, 143) {
            InsertResult::Normal(node) => node,
            _ => unreachable!(),
        };
        assert_eq!(node.right_child(), 4);
    }

    #[test]
    fn insert_cell_pointer() {
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

    fn create_leaf_node_samples(pager: Arc<Mutex<Pager>>, num: usize) -> Vec<NodePointer> {
        let mut children = Vec::new();
        for i in 0..num {
            let page = pager.lock().unwrap().get_free_page().unwrap();
            let mut node = Node::new(page, pager.clone());
            node.set_node_type(NodeType::Leaf);
            let node = node.leaf_insert(i as u32, &[1, 2, 3], None);
            match node {
                InsertResult::Normal(node) => {
                    println!("{:#?}", node);
                    children.push(node.page_num);
                }
                _ => unreachable!(),
            }
        }
        children
    }

    #[test]
    fn single_interior_insert() {
        let pager = Arc::new(Mutex::new(Pager::init("single_interior_insert")));
        let children = create_leaf_node_samples(pager.clone(), 5);
        let new_page = pager.lock().unwrap().get_free_page().unwrap();
        let mut node = Node::new(new_page as usize, pager.clone());
        node.set_node_type(NodeType::Interior);
        let node = if let InsertResult::Normal(node) = node.interior_insert(22, children[0]) {
            node
        } else {
            unreachable!()
        };
        let node = if let InsertResult::Normal(node) = node.interior_insert(12, children[1]) {
            node
        } else {
            unreachable!()
        };
        let node = if let InsertResult::Normal(node) = node.interior_insert(300, children[2]) {
            node
        } else {
            unreachable!()
        };
        let node = if let InsertResult::Normal(node) = node.interior_insert(1242, children[2]) {
            node
        } else {
            unreachable!()
        };
        println!("{:#?}", node);
        let node = match node.interior_insert(200, children[4] as u32) {
            InsertResult::Splitted(key, left, right) => {
                println!("{:#?}", key);
                println!("{:#?}", left);
                println!("{:#?}", right);
                left
            }
            _ => unreachable!("Bad result"),
        };
        let page = pager.lock().unwrap().get_page(0).unwrap();
        let cell = node.cell_at(&page, 0);
        assert_eq!(cell.key(), 22);
        assert_eq!(cell.child(), 12);
        for i in 0..node.num_cells() - 1 {
            let lo = node.cell_at(&page, i).key();
            let hi = node.cell_at(&page, i + 1).key();
            assert!(lo < hi, "Key should be sorted: {} > {}", lo, hi);
        }
    }

    #[test]
    fn basic_leaf_insert() {
        let pager = Arc::new(Mutex::new(Pager::init("insert_ptr")));
        let node = Node::new(0, pager.clone());
        let node =
            if let InsertResult::Normal(node) = node.leaf_insert(22, &[1, 2, 3, 4, 5, 6], None) {
                node
            } else {
                unreachable!()
            };
        let page = pager.lock().unwrap().get_page(0).unwrap();
        let cell = node.cell_at(&page, node.cell_pointer(0));
        assert_eq!(cell.kept_payload(), &[1, 2, 3, 4, 5, 6],);
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
        assert_eq!(cell.kept_payload(), &[1, 2, 3],);
        let cell = node.cell_at(&page, 1);
        assert_eq!(cell.kept_payload(), &[1, 2, 3, 4, 5, 6]);
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
                let node = match node.interior_insert(k, l.page_num) {
                    InsertResult::Normal(node) => node,
                    _ => unreachable!(),
                };
                println!("{:?}", node);
                panic!();
                node
            }
            _ => unreachable!(),
        };
    }
}
