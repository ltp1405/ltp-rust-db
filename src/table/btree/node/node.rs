use std::{
    mem::size_of,
    ptr::slice_from_raw_parts_mut,
    rc::Rc,
    sync::{Arc, Mutex},
};

use crate::page::{Page, Pager, PAGE_SIZE};

use super::{
    cell::Cell, CellPointer, CellsCount, NodePointer, NodeType, CELL_CONTENT_START, CELL_NUMS,
    CELL_POINTERS_ARRAY_OFFSET, CELL_POINTER_SIZE, COMMON_NODE_HEADER_SIZE, IS_ROOT, NODE_TYPE,
    PARENT_POINTER,
};

/// Each node of the btree is contained inside 1 page
pub struct Node<'a> {
    pager: Arc<Mutex<Pager>>,
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
    pub fn new(page: &'a mut Page, pager: Arc<Mutex<Pager>>) -> Self {
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
        let val = CELL_POINTERS_ARRAY_OFFSET + CELL_POINTER_SIZE * cell_num as usize;
        val
    }

    fn cell(&self, cell_num: u32) -> Cell {
        if cell_num > self.num_cells() {
            panic!("Cell index out of bound");
        }
        let offset = self.cell_pointer(cell_num);
        unsafe { Cell::table_leaf_at(&self.page, offset as usize, 0) }
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
            let mid_key = self.cell(mid).key();
            if search_key < mid_key {
                if mid == 0 {
                    return Slot::Hole(0);
                } else if search_key > self.cell(mid - 1).key() {
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
        let pager1 = self.pager.clone();
        let pager2 = self.pager.clone();
        if self.is_root() {
            let mut pager = pager1.lock().unwrap();
            let (left_page_num, mut left_page) = pager.get_free_page_mut().unwrap();
            let mut left_child = Node::new(&mut left_page, self.pager.clone());

            let mut pager = pager2.lock().unwrap();
            let (right_page_num, mut right_page) = pager.get_free_page_mut().unwrap();
            let mut right_child = Node::new(&mut right_page, self.pager.clone());

            let mid_index = self.num_cells() / 2;
            let chilren = self.get_children();
            let (first_half, second_half) = chilren.split_at(mid_index as usize);
            for node_addr in first_half {
                let cell = self.cell(*node_addr);
                let (key, payload) = (cell.key(), cell.payload());
                left_child.insert(key, payload.into())
            }
            for node_addr in second_half {
                let cell = self.cell(*node_addr);
                let (key, payload) = (cell.key(), cell.payload());
                right_child.insert(key, payload.into())
            }

            self.set_is_root(true);
            self.set_num_cells(0);
            let mid_node_key = self.cell(mid_index).key();
            self.insert(mid_node_key, left_page_num.to_be_bytes().to_vec())
        } else {
        }
    }

    pub fn get_children(&self) -> Vec<NodePointer> {
        todo!()
    }

    pub fn insert(&mut self, key: u32, payload: Vec<u8>) {
        let payload_size = payload.len();
        let overflow_size = self.overflow_amount(payload_size as u32);
        let (not_overflowed_payload, overflow_head) = match overflow_size {
            Some(overflow_size) => {
                if payload_size as u32 - overflow_size
                    < self.min_threshold_for_non_overflow() as u32
                {
                    self.split();
                    todo!("SPLIT HERE")
                } else {
                    let (not_overflowed_payload, overflow_payload) =
                        payload.split_at(payload_size - overflow_size as usize);
                    let page_num = self.pager.lock().unwrap().get_free_page().unwrap() as u32;
                    self.handle_overflow(page_num, overflow_payload);
                    (not_overflowed_payload, Some(page_num))
                }
            }
            None => (payload.as_slice(), None),
        };
        let cell_num: u32 = if let Slot::Hole(hole) = self.search(key) {
            hole
        } else {
            panic!("Key already inserted");
        };
        println!("Payload size {:?}", not_overflowed_payload.len());
        let cell = Cell::new_table_leaf(
            key,
            payload_size as u32,
            not_overflowed_payload.to_vec(),
            overflow_head,
        );
        println!("{:?}", cell);

        let cell_start = self.cell_content_start() as usize - cell.size();

        let slice = &mut self.page[cell_start as usize..(cell_start + cell.size()) as usize];
        assert_eq!(slice.len(), cell.size());
        unsafe {
            cell.serialize_to(slice);
        }

        self.set_cell_pointer(cell_num, cell_start as u32);

        self.set_cell_content_start(cell_start as u32);
        self.set_num_cells(self.num_cells() + 1);
    }

    fn handle_overflow(&self, overflow_head: u32, remaining_payloads: &[u8]) {
        let start_offset = size_of::<u32>();
        let page_available_size = PAGE_SIZE - size_of::<u32>();
        let pages_needed = remaining_payloads.len() / (PAGE_SIZE - size_of::<u32>());
        let mut pages = Vec::new();
        pages.push(overflow_head as usize);
        {
            let mut pager = self.pager.lock().unwrap();
            pages.resize_with(pages_needed, || pager.get_free_page().unwrap());
        }
        for (i, page_addr) in pages.iter().enumerate() {
            let mut pager = self.pager.lock().unwrap();
            let page = pager.get_page_mut(*page_addr).unwrap();

            let start = i * page_available_size;
            let end = start + page_available_size;

            let next_page = pages.get(i + 1);
            if next_page.is_some() {
                unsafe {
                    page.write_val_at(0, next_page);
                }
            }

            let slice = &mut page[start_offset..PAGE_SIZE];
            slice.copy_from_slice(&remaining_payloads[start..end]);
        }
        let remain = remaining_payloads.len() % page_available_size;
        let start = pages_needed * page_available_size;
        let end = start + remain;
        let page_addr = {
            let mut pager = self.pager.lock().unwrap();
            pager.get_free_page().unwrap()
        };
        let mut pager = self.pager.lock().unwrap();
        let page = pager.get_page_mut(page_addr).unwrap();
        let slice = &mut page[start_offset..start_offset + remain];
        slice.copy_from_slice(&remaining_payloads[start..end]);
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

    fn cells_content(&self) -> &[u8] {
        let start = self.cell_content_start();
        &self.page[start as usize..PAGE_SIZE as usize]
    }

    fn cell_content_start(&self) -> u32 {
        unsafe { self.page.read_val_at(CELL_CONTENT_START.0) }
    }

    fn set_cell_content_start(&mut self, val: u32) {
        unsafe { self.page.write_val_at(CELL_CONTENT_START.0, val) }
    }

    fn set_cell_pointer(&mut self, cell_num: u32, val: u32) {
        unsafe {
            self.page
                .write_val_at(self.cell_pointer_offset(cell_num), val);
        }
    }

    fn free_size(&self) -> usize {
        self.cell_content_start() as usize - self.cell_pointer_offset(self.num_cells())
    }

    fn cell_pointer(&self, cell_num: u32) -> CellPointer {
        unsafe { self.page.read_val_at(self.cell_pointer_offset(cell_num)) }
    }
}

#[cfg(test)]
mod node {
    lazy_static! {
        static ref PAGER: Arc<Mutex<Pager>> = Arc::new(Mutex::new(Pager::init("testdb")));
    }

    use std::sync::{Arc, Mutex};

    use lazy_static::lazy_static;

    use crate::{
        page::{Page, Pager, PAGE_SIZE},
        table::{btree::node::NodeType, ROW_SIZE},
    };

    use super::Node;

    #[test]
    fn node_type() {
        let mut page = Page::init();
        let mut node = Node::new(&mut page, PAGER.clone());
        node.set_node_type(NodeType::Leaf);
        assert_eq!(node.node_type(), NodeType::Leaf);
    }

    #[test]
    fn parent_pointer() {
        let mut page = Page::init();
        let mut node = Node::new(&mut page, PAGER.clone());
        node.set_parent_pointer(10);
        assert_eq!(node.parent_pointer(), 10);
    }

    #[test]
    fn cell_nums() {
        let mut page = Page::init();
        let mut node = Node::new(&mut page, PAGER.clone());
        node.set_num_cells(10);
        assert_eq!(node.num_cells(), 10);
    }

    #[test]
    fn cell_pointer() {
        let mut page = Page::init();
        let mut node = Node::new(&mut page, PAGER.clone());
        node.set_cell_pointer(0, 10);
        assert_eq!(node.cell_pointer(0), 10);
    }

    #[test]
    fn single_insert() {
        let mut page = Page::init();
        let mut node = Node::new(&mut page, PAGER.clone());
        node.set_cell_content_start(PAGE_SIZE as u32);
        node.insert(12, vec![1, 2, 3]);
        assert_eq!(node.cell(0).payload(), vec![1, 2, 3]);
    }

    #[test]
    fn big_payload_single_insert() {
        let mut page = Page::init();
        let mut node = Node::new(&mut page, PAGER.clone());
        node.set_cell_content_start(PAGE_SIZE as u32);
        let p: Vec<u8> = [1, 2, 3].repeat(30);
        node.insert(12, p.clone());
        assert_eq!(node.cell(0).payload(), p);
    }

    #[test]
    fn more_insert() {
        let mut page = Page::init();
        let mut node = Node::new(&mut page, PAGER.clone());
        node.set_cell_content_start(PAGE_SIZE as u32);
        node.insert(12, vec![1, 2, 3]);
        node.insert(14, vec![4, 5, 6, 7]);
        println!("{:?}", node.cells_content());
        assert_eq!(node.cell(0).key(), 12);
        assert_eq!(node.cell(1).key(), 14);
        assert_eq!(node.cell(0).payload(), vec![1, 2, 3]);
        assert_eq!(node.cell(1).payload(), vec![4, 5, 6, 7]);
    }

    #[test]
    fn two_big_payload() {
        let mut page = Page::init();
        let mut node = Node::new(&mut page, PAGER.clone());
        node.set_cell_content_start(PAGE_SIZE as u32);
        let big_payload = [1, 2, 3].repeat(1200);
        node.insert(12, big_payload.clone());
        node.insert(14, big_payload.clone());
        assert_eq!(node.cell(0).key(), 12);
        assert_eq!(&node.cell(0).payload()[0..600], &big_payload[0..600]);
    }

    #[test]
    fn big_and_overflow() {
        let pager = PAGER.clone();
        let page = {
            let mut pager = pager.lock().unwrap();
            pager.get_free_page().unwrap()
        };
        let mut pager = pager.lock().unwrap();
        let mut page = pager.get_page_mut(page).unwrap();
        let mut node = Node::new(&mut page, PAGER.clone());
        node.set_cell_content_start(PAGE_SIZE as u32);
        let big_payload = [1, 2, 3].repeat(1000);
        node.insert(12, big_payload.clone());
        node.insert(14, big_payload.clone());
        assert_eq!(node.cell(0).key(), 12);
        assert_eq!(&node.cell(0).payload()[0..600], &big_payload[0..600]);
        assert_ne!(node.cell(1).overflow_page_head(), None);

        // let overflow_page = node.cell(1).overflow_page_head().unwrap();
        // let pager = PAGER.clone();
        // let mut pager = pager.lock().unwrap();
        // println!("{:?}", pager.get_page(overflow_page as usize));
    }

    #[test]
    fn only_fit_one_cell() {
        let mut page = Page::init();
        let mut node = Node::new(&mut page, PAGER.clone());
        node.set_cell_content_start(PAGE_SIZE as u32);
        let big_payload = [1, 2, 3].repeat(1250);
        let small_payload = [1].repeat(100);
        node.insert(12, big_payload.clone());
        node.insert(14, small_payload.clone());
        assert_eq!(node.cell(0).key(), 12);
        assert_eq!(&node.cell(0).payload()[0..600], &big_payload[0..600]);
        assert_eq!(node.cell(1).key(), 14);
        assert_eq!(&node.cell(1).payload(), &small_payload);
    }

    #[test]
    fn insert_with_overflow() {
        let mut page = Page::init();
        let mut node = Node::new(&mut page, PAGER.clone());
        node.set_cell_content_start(PAGE_SIZE as u32);
        let big_payload = [1, 2, 3].repeat(2000);
        node.insert(12, big_payload.clone());
        assert_eq!(node.cell(0).key(), 12);
        assert_eq!(&node.cell(0).payload()[0..600], &big_payload[0..600]);
        assert_ne!(node.free_size(), PAGE_SIZE - 12);
    }
}
