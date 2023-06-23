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
#[derive(Debug)]
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

#[derive(Debug)]
enum InsertResult {
    KeyExisted(NodePointer),
    Normal(NodePointer),
    Splitted(NodePointer, NodePointer, NodePointer),
}

impl<'a> Node<'a> {
    // pub fn new(page_num: usize, pager: Arc<Mutex<Pager>>) -> Self {
    //     let pager_clone = pager.clone().lock().unwrap();
    //     let page = pager_clone.get_page_mut(page_num).unwrap();
    //     Node { page, pager }
    // }
    // pub fn is_root(&self) -> bool {
    //     unsafe { self.page.read_val_at(IS_ROOT.0) }
    // }

    // pub fn set_is_root(&mut self, is_root: bool) {
    //     unsafe {
    //         self.page.write_val_at(IS_ROOT.0, is_root);
    //     }
    // }

    // pub fn node_type(&self) -> NodeType {
    //     unsafe { self.page.read_val_at(NODE_TYPE.0) }
    // }

    // pub fn set_node_type(&mut self, node_type: NodeType) {
    //     unsafe {
    //         self.page.write_val_at(NODE_TYPE.0, node_type);
    //     }
    // }

    // pub fn parent_pointer(&self) -> u32 {
    //     unsafe { self.page.read_val_at(PARENT_POINTER.0) }
    // }

    // pub fn set_parent_pointer(&mut self, parent_pointer: u32) {
    //     unsafe {
    //         self.page.write_val_at(PARENT_POINTER.0, parent_pointer);
    //     }
    // }

    // pub fn num_cells(&self) -> CellsCount {
    //     unsafe { self.page.read_val_at(CELL_NUMS.0) }
    // }

    // pub fn set_num_cells(&mut self, num_cells: u32) {
    //     unsafe {
    //         self.page.write_val_at(CELL_NUMS.0, num_cells);
    //     }
    // }

    // fn cell_pointer_offset(&self, cell_num: u32) -> usize {
    //     let val = CELL_POINTERS_ARRAY_OFFSET + CELL_POINTER_SIZE * cell_num as usize;
    //     val
    // }

    // fn cell(&self, cell_num: u32) -> Cell {
    //     if cell_num > self.num_cells() {
    //         panic!("Cell index out of bound");
    //     }
    //     let offset = self.cell_pointer(cell_num);
    //     unsafe { Cell::table_leaf_at(&self.page, offset as usize, 0) }
    // }

    // pub fn search(&self, search_key: u32) -> Slot {
    //     let num_cells = self.num_cells();
    //     if num_cells == 0 {
    //         return Slot::Hole(0);
    //     }
    //     let mut hi = num_cells;
    //     let mut lo = 0;
    //     loop {
    //         let mid = (lo + hi) / 2;
    //         let mid_key = self.cell(mid).key();
    //         if search_key < mid_key {
    //             if mid == 0 {
    //                 return Slot::Hole(0);
    //             } else if search_key > self.cell(mid - 1).key() {
    //                 return Slot::Hole(mid);
    //             }
    //             hi = mid;
    //         } else if search_key > mid_key {
    //             if mid == num_cells - 1 {
    //                 return Slot::Hole(num_cells);
    //             }
    //             lo = mid;
    //         } else {
    //             return Slot::Cell(mid);
    //         }
    //     }
    // }
    // pub fn node_split(node: Node<'a>) -> (u32, NodePointer, NodePointer) {
    //     todo!()
    //     // let node_type = node.node_type();
    //     // let left = node;
    //     // match node_type {
    //     //     NodeType::Leaf => {
    //     //         let (l, r) = split_in_half(vals);
    //     //         let mid_key = r.first().unwrap().0.clone();
    //     //         let l = Box::new(SimpleNode::Leaf { vals: l });
    //     //         let r = Box::new(SimpleNode::Leaf { vals: r });
    //     //         (mid_key, l, r)
    //     //     }
    //     //     NodeType::Interior => {
    //     //         let (l, mid, r) = split_in_half_with_mid(vals);
    //     //         let mid_key = mid.0;
    //     //         let l = Box::new(SimpleNode::Interior {
    //     //             vals: l,
    //     //             left_child: Some(mid.1),
    //     //         });
    //     //         let r = Box::new(SimpleNode::Interior {
    //     //             vals: r,
    //     //             left_child: Some(left_child.unwrap()),
    //     //         });
    //     //         (mid_key, l, r)
    //     //     }
    //     // }
    // }

    pub fn get_children(&self) -> Vec<NodePointer> {
        todo!()
    }

    /// Insert a payload into a leaf node
    /// Return a normal node if insert normally
    /// Return a pair of node if need split
    fn leaf_node_insert(
        mut node_ptr: NodePointer,
        pager: Arc<Mutex<Pager>>,
        key: u32,
        payload: Vec<u8>,
    ) -> InsertResult {
        let pager_clone = pager.clone();
        let mut pager = pager.lock().unwrap();
        let page = pager.get_page_mut(node_ptr as usize).unwrap();
        let node = Node {
            pager: pager_clone,
            page,
        };
        if node.should_overflow(payload.len()) {
            let (k, l, r) = Node::node_split(node);
            return InsertResult::Splitted(k, l, r);
        }
        let slot = node.search(key);
        todo!()
    }

    fn interior_node_insert(
        mut node_ptr: NodePointer,
        pager: Arc<Mutex<Pager>>,
        key: u32,
        left_node_ptr: NodePointer,
    ) -> InsertResult {
        let pager_clone = pager.clone();
        let mut pager = pager.lock().unwrap();
        let page = pager.get_page_mut(node_ptr as usize).unwrap();
        let node = Node {
            pager: pager_clone,
            page,
        };
        if node.interior_should_overflow() {
            let (k, l, r) = Node::node_split(node);
            return InsertResult::Splitted(k, l, r);
        }
        todo!()
    }

    fn node_insert(
        mut node_ptr: NodePointer,
        pager: Arc<Mutex<Pager>>,
        key: u32,
        payload: Vec<u8>,
    ) -> InsertResult {
        let pager_clone = pager.clone();
        let mut pager = pager.lock().unwrap();
        let page = pager.get_page_mut(node_ptr as usize).unwrap();
        let node = Node {
            pager: pager_clone.clone(),
            page,
        };
        let slot = node.search(key);
        let node_type = node.node_type();
        match node_type {
            NodeType::Leaf => Node::leaf_node_insert(node_ptr, pager_clone, key, payload),
            NodeType::Interior => {
                let slot = node.search(key);
                let slot = match slot {
                    Slot::Hole(i) => i,
                    Slot::Cell(i) => i,
                };
                match node.get_children().get(slot as usize) {
                    Some(child) => {
                        let (split, val) =
                            match Node::node_insert(*child, pager_clone.clone(), key, payload) {
                                InsertResult::Normal(node) => (None, node),
                                InsertResult::Splitted(k, l, r) => (Some((k, l)), r),
                                InsertResult::KeyExisted(node) => {
                                    return InsertResult::KeyExisted(node)
                                }
                            };
                        // The child which we choose to insert is splitted,
                        // we should insert a splitted node as a child
                        if let Some(v) = split {
                            Node::interior_node_insert(node_ptr, pager_clone, v.0, v.1);
                        }
                        InsertResult::Normal(node_ptr)
                    }
                    None => {
                        // Insert to right-most child
                        todo!()
                    }
                }
            }
        }
    }

    pub fn leaf_insert_cell(&mut self, hole: u32, key: u32, payload: Vec<u8>) {}

    pub fn interior_insert_cell(&mut self, hole: u32, key: u32, node_ptr: NodePointer) {
        todo!();
        let cell = Cell::new_table_interior(key, node_ptr);
        let cell_start = self.cell_content_start() as usize - cell.size();

        let slice = &mut self.page[cell_start as usize..(cell_start + cell.size()) as usize];
        assert_eq!(slice.len(), cell.size());
        unsafe {
            cell.serialize_to(slice);
        }

        self.set_cell_pointer(hole, cell_start as u32);

        self.set_cell_content_start(cell_start as u32);
        self.set_num_cells(self.num_cells() + 1);
    }

    // pub fn insert(&mut self, hole: u32, key: u32, payload: Vec<u8>) {
    //     let payload_size = payload.len();
    //     let overflow_size = self.overflow_amount(payload_size as u32);
    //     let (not_overflowed_payload, overflow_head) = match overflow_size {
    //         Some(overflow_size) => {
    //             if payload_size as u32 - overflow_size
    //                 < self.min_threshold_for_non_overflow() as u32
    //             {
    //                 self.split();
    //                 todo!("SPLIT HERE")
    //             } else {
    //                 let (not_overflowed_payload, overflow_payload) =
    //                     payload.split_at(payload_size - overflow_size as usize);
    //                 let page_num = self.pager.lock().unwrap().get_free_page().unwrap() as u32;
    //                 self.handle_overflow(page_num, overflow_payload);
    //                 (not_overflowed_payload, Some(page_num))
    //             }
    //         }
    //         None => (payload.as_slice(), None),
    //     };
    //     println!("Payload size {:?}", not_overflowed_payload.len());
    //     let cell = Cell::new_table_leaf(
    //         key,
    //         payload_size as u32,
    //         not_overflowed_payload.to_vec(),
    //         overflow_head,
    //     );
    //     println!("{:?}", cell);

    //     let cell_start = self.cell_content_start() as usize - cell.size();

    //     let slice = &mut self.page[cell_start as usize..(cell_start + cell.size()) as usize];
    //     assert_eq!(slice.len(), cell.size());
    //     unsafe {
    //         cell.serialize_to(slice);
    //     }

    //     self.set_cell_pointer(cell_num, cell_start as u32);

    //     self.set_cell_content_start(cell_start as u32);
    //     self.set_num_cells(self.num_cells() + 1);
    // }

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

    fn should_overflow(&self, len: usize) -> bool {
        todo!()
    }

    fn interior_should_overflow(&self) -> bool {
        todo!()
    }
}

#[cfg(test)]
mod node {
    use std::sync::{Arc, Mutex};

    use crate::{page::Pager, table::btree::node::NodeType};

    use super::Node;

    #[test]
    fn node_init() {
        let pager = Arc::new(Mutex::new(Pager::init("testnode")));
        let pager2 = pager.clone();
        let pager_clone = pager.clone();
        // {
        //     let mut pager = pager.lock().unwrap();
        //     let page = pager.get_page_mut(1 as usize).unwrap();
        //     let mut node = Node {
        //         pager: pager_clone,
        //         page,
        //     };
        //     node.set_is_root(true);
        //     node.set_node_type(NodeType::Leaf);
        //     node.set_num_cells(0);
        //     println!("{:?}", node);
        // }
        Node::node_insert(1, pager2, 1, vec![1, 2]);
        panic!()
    }
}
