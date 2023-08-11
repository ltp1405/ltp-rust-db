use crate::{
    buffer_manager::{BufferManager, Page},
    disk_manager::DiskManager,
};
use std::{fmt::Debug, mem::size_of};

use super::node_header::{
    CellPointer, CellsCount, NodeHeaderReader, NodeHeaderWriter, NodePointer, NodeType,
};

/// Each node of the btree is contained inside 1 page
pub struct Node<'a, const BLOCKSIZE: usize, const CAPACITY: usize, const MEMORY_CAPACITY: usize> {
    page_number: u32,
    disk_manager: &'a DiskManager<BLOCKSIZE, CAPACITY>,
    buffer_manager: &'a BufferManager<'a, BLOCKSIZE, CAPACITY, MEMORY_CAPACITY>,
}

// impl<'a, const BLOCKSIZE: usize, const CAPACITY: usize, const MEMORY_CAPACITY: usize> Debug
//     for Node<'a, BLOCKSIZE, CAPACITY, MEMORY_CAPACITY>
// {
//     fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
//         let page = self.page();
//         match self.node_type() {
//             NodeType::Leaf => {
//                 let mut cells = Vec::new();
//                 for i in 0..self.num_cells() {
//                     let cell = unsafe { self.cell_at(i) };
//                     cells.push(cell);
//                 }
//                 f.debug_struct("LeafNode").field("cells", &cells).finish()
//             }
//             NodeType::Interior => {
//                 let mut children = Vec::new();
//                 for i in 0..self.num_cells() {
//                     let cell = unsafe { self.cell_at(i) };
//                     let node = Node::from(self.buffer_manager, self.disk_manager, cell.child());
//                     let key = cell.key();
//                     children.push((key, node));
//                 }
//                 let right_child =
//                     Node::from(self.buffer_manager, self.disk_manager, self.right_child());
//                 f.debug_struct("InteriorNode")
//                     .field("address", &self.page_number)
//                     .field("children_num", &children.len())
//                     .field("children", &children)
//                     .field("right_most_child", &right_child)
//                     .finish()
//             }
//         }
//     }
// }

#[derive(Debug)]
enum InsertDecision {
    Normal,
    Split,
    Overflow(usize),
}

#[derive(Debug, PartialEq)]
pub enum Slot {
    /// Represent a slot which is not occupied by a key yet
    Hole(u32),
    /// Represent a slot which is occupied by a key
    Cell(u32),
}

// #[derive(Debug)]
// pub enum InsertResult<
//     'a,
//     const BLOCKSIZE: usize,
//     const CAPACITY: usize,
//     const MEMORY_CAPACITY: usize,
// > {
//     KeyExisted(Node<'a, BLOCKSIZE, CAPACITY, MEMORY_CAPACITY>),
//     Normal(Node<'a, BLOCKSIZE, CAPACITY, MEMORY_CAPACITY>),
//     Splitted(
//         NodePointer,
//         Node<'a, BLOCKSIZE, CAPACITY, MEMORY_CAPACITY>,
//         Node<'a, BLOCKSIZE, CAPACITY, MEMORY_CAPACITY>,
//     ),
// }

impl<'a, const BLOCKSIZE: usize, const CAPACITY: usize, const MEMORY_CAPACITY: usize>
    Node<'a, BLOCKSIZE, CAPACITY, MEMORY_CAPACITY>
{
    pub fn new(
        buffer_manager: &'a BufferManager<'a, BLOCKSIZE, CAPACITY, MEMORY_CAPACITY>,
        disk_manager: &'a DiskManager<BLOCKSIZE, CAPACITY>,
    ) -> Self {
        let new_page = disk_manager.allocate().unwrap();
        let mut node = Node {
            page_number: new_page,
            buffer_manager,
            disk_manager,
        };
        if node.cell_content_start() == 0 {
            node.set_cell_content_start(BLOCKSIZE as u32);
        }
        node
    }

    pub fn from(
        buffer_manager: &'a BufferManager<'a, BLOCKSIZE, CAPACITY, MEMORY_CAPACITY>,
        disk_manager: &'a DiskManager<BLOCKSIZE, CAPACITY>,
        page_num: u32,
    ) -> Self {
        Node {
            page_number: page_num,
            buffer_manager,
            disk_manager,
        }
    }

    fn page(&self) -> Page<'a, BLOCKSIZE, CAPACITY, MEMORY_CAPACITY> {
        self.buffer_manager.get_page(self.page_number)
    }

    pub fn node_type(&self) -> NodeType {
        let page = self.page();
        let node_type = unsafe { NodeHeaderReader::new(self.page().as_ptr()).node_type() };
        if node_type as u8 == 0x2 {
            NodeType::Interior
        } else if node_type as u8 == 0x5 {
            NodeType::Leaf
        } else {
            println!("{:?}", node_type as u8);
            panic!("Invalid node type")
        }
    }

    pub fn set_node_type(&mut self, node_type: NodeType) {
        let page = self.page().as_mut_ptr();
        unsafe { NodeHeaderWriter::new(page).set_node_type(node_type) }
    }

    pub fn num_cells(&self) -> CellsCount {
        let page = self.page().as_mut_ptr();
        unsafe { NodeHeaderReader::new(page).num_cells() }
    }

    pub fn set_num_cells(&mut self, num_cells: u32) {
        let page = self.page().as_mut_ptr();
        unsafe { NodeHeaderWriter::new(page).set_num_cells(num_cells) }
    }

    fn cell_pointer_offset(&self, cell_num: u32) -> usize {
        let page = self.page().as_ptr();
        let cell_pointer_offset =
            unsafe { NodeHeaderReader::new(page).cell_pointer_offset(cell_num) };
        cell_pointer_offset as usize
    }

    fn cell_bound(&self, cell_num: u32) -> (usize, usize) {
        let page = self.page();
        let target = self.cell_pointer(cell_num) as usize;
        let cell = self.cell_at(cell_num);
        (target, cell.cell_size() as usize)
    }

    fn right_child(&self) -> NodePointer {
        assert_eq!(self.node_type(), NodeType::Interior);
        let page = self.page();
        unsafe { NodeHeaderReader::new(page.as_ptr()).right_most_child() }
    }

    pub fn set_right_child(&self, child: NodePointer) {
        assert_eq!(self.node_type(), NodeType::Interior);
        let mut page = self.page();
        unsafe { NodeHeaderWriter::new(page.as_mut_ptr()).set_right_most_child(child) };
    }

    pub fn search(&self, search_key: u32) -> Slot {
        let page = self.page();
        let num_cells = self.num_cells();
        if num_cells == 0 {
            return Slot::Hole(0);
        }
        let mut hi = num_cells;
        let mut lo = 0;
        loop {
            let mid = (lo + hi) / 2;
            let mid_key = self.cell_at(mid).key();
            if search_key < mid_key {
                if mid == 0 {
                    return Slot::Hole(0);
                } else if search_key > self.cell_at(mid - 1).key() {
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

    pub fn leaf_split_to(
        node: Node<'a, BLOCKSIZE, CAPACITY, MEMORY_CAPACITY>,
    ) -> (
        Node<'a, BLOCKSIZE, CAPACITY, MEMORY_CAPACITY>,
        Node<'a, BLOCKSIZE, CAPACITY, MEMORY_CAPACITY>,
    ) {
        let right = Node::new(node.buffer_manager, node.disk_manager);
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

    // /// Insert a payload into a leaf node
    // /// Return a normal node if insert normally
    // /// Return a pair of node if need split
    // fn leaf_insert(
    //     mut self,
    //     key: u32,
    //     payload: &[u8],
    //     overflow_head: Option<NodePointer>,
    // ) -> InsertResult<'a, BLOCKSIZE, CAPACITY, MEMORY_CAPACITY> {
    //     match self.insert_decision(payload.len()) {
    //         InsertDecision::Normal => {
    //             let hole = self.search(key);
    //             let hole = match hole {
    //                 Slot::Hole(hole) => hole,
    //                 Slot::Cell(_cell) => panic!(),
    //             };
    //             let cell_start = self.cell_content_start();
    //             let page = self.page();
    //             let cell = Cell::insert_table_leaf(
    //                 &page,
    //                 cell_start as usize,
    //                 key,
    //                 payload.len() as u32,
    //                 overflow_head,
    //                 &payload,
    //             );
    //             let cell_start = cell_start - (cell.header_size() as u32) - payload.len() as u32;
    //             self.set_cell_content_start(cell_start);
    //             self.insert_cell_pointer(hole, cell_start as u32);
    //             InsertResult::Normal(self)
    //         }
    //         InsertDecision::Overflow(kept_size) => {
    //             todo!();
    //             let hole = self.search(key);
    //             let hole = match hole {
    //                 Slot::Hole(hole) => hole,
    //                 Slot::Cell(_cell) => panic!(),
    //             };
    //             let page = self.page();
    //             let payload_len = payload.len();
    //             let (non_overflow, overflow) = payload.split_at(kept_size);
    //             let cell_start = self.cell_content_start();
    //             let new_page = self.disk_manager.allocate().unwrap();
    //             let cell = Cell::insert_table_leaf(
    //                 &page,
    //                 cell_start as usize,
    //                 key,
    //                 payload_len as u32,
    //                 Some(new_page as u32),
    //                 non_overflow,
    //             );
    //             // TODO: Handle remain payload
    //             let cell_start = cell_start - (cell.header_size() as u32) - payload.len() as u32;
    //             self.set_cell_content_start(cell_start);
    //             self.insert_cell_pointer(hole, cell_start as u32);
    //             InsertResult::Normal(self)
    //         }
    //         InsertDecision::Split => {
    //             let page = self.page();
    //             let mut new_node = Node::new(self.buffer_manager, self.disk_manager);
    //             new_node.set_node_type(NodeType::Leaf);
    //             let mid = self.num_cells() / 2;
    //             for i in mid..self.num_cells() {
    //                 let cell = self.cell_at(i);
    //                 new_node = if let InsertResult::Normal(node) = new_node.leaf_insert(
    //                     cell.key(),
    //                     cell.kept_payload(),
    //                     cell.overflow_page_head(),
    //                 ) {
    //                     node
    //                 } else {
    //                     unreachable!()
    //                 };
    //             }
    //             // TODO: Handle hole after split
    //             let cell_bound = self.cell_bound(mid);
    //             let mid_key = self.cell_at(mid).key();
    //             self.set_num_cells(mid);

    //             if key >= mid_key {
    //                 new_node = match new_node.leaf_insert(key, payload, overflow_head) {
    //                     InsertResult::Normal(node) => node,
    //                     _ => unreachable!("Maybe overflow calculation go wrong"),
    //                 }
    //             } else {
    //                 self = match self.leaf_insert(key, payload, overflow_head) {
    //                     InsertResult::Normal(node) => node,
    //                     _ => unreachable!("Maybe overflow calculation go wrong"),
    //                 }
    //             };
    //             InsertResult::Splitted(mid_key, self, new_node)
    //         }
    //     }
    // }

    // fn cell_at(&self, cell_num: u32) -> Cell {
    //     let page = self.page();
    //     let offset = self.cell_pointer(cell_num);
    //     let node_type = self.node_type();
    //     match node_type {
    //         NodeType::Leaf => unsafe {
    //             Cell::leaf_at((page.as_ptr() as *const u8).add(offset as usize))
    //         },
    //         NodeType::Interior => unsafe {
    //             Cell::interior_at((page.as_ptr() as *const u8).add(offset as usize))
    //         },
    //     }
    // }

    // pub fn interior_insert(
    //     mut self,
    //     key: u32,
    //     child: NodePointer,
    // ) -> InsertResult<'a, BLOCKSIZE, CAPACITY, MEMORY_CAPACITY> {
    //     match self.insert_decision(size_of::<u32>()) {
    //         InsertDecision::Normal => {
    //             let hole = self.search(key);
    //             let hole = match hole {
    //                 Slot::Hole(hole) => hole,
    //                 Slot::Cell(_cell) => return InsertResult::KeyExisted(self),
    //             };
    //             let cell_start = self.cell_content_start();
    //             let page = self.page();
    //             let cell = Cell::insert_table_interior(&page, cell_start as usize, key, child);
    //             let cell_start = cell_start - (cell.header_size() as u32);
    //             self.set_cell_content_start(cell_start);
    //             self.insert_cell_pointer(hole, cell_start as u32);
    //             InsertResult::Normal(self)
    //         }
    //         InsertDecision::Overflow(_kept_size) => {
    //             unreachable!()
    //         }
    //         InsertDecision::Split => {
    //             let page = self.page();
    //             let mut new_node = Node::new(self.buffer_manager, self.disk_manager);
    //             new_node.set_node_type(NodeType::Interior);
    //             let mid = self.num_cells() / 2;
    //             for i in mid..self.num_cells() {
    //                 println!("{}", self.cell_pointer(i));
    //                 let cell = self.cell_at(i);
    //                 println!("{:?}", cell);
    //                 new_node = match new_node.interior_insert(cell.key(), cell.child()) {
    //                     InsertResult::Normal(node) => node,
    //                     _ => unreachable!(),
    //                 };
    //             }
    //             // TODO: Handle hole after split
    //             let mid_key = self.cell_at(mid).key();
    //             self.set_num_cells(mid);

    //             if key >= mid_key {
    //                 new_node = match new_node.interior_insert(key, child) {
    //                     InsertResult::Normal(node) => node,
    //                     _ => unreachable!("Maybe overflow calculation go wrong"),
    //                 }
    //             } else {
    //                 self = match self.interior_insert(key, child) {
    //                     InsertResult::Normal(node) => node,
    //                     _ => unreachable!("Maybe overflow calculation go wrong"),
    //                 }
    //             };
    //             self.set_node_type(NodeType::Interior);
    //             InsertResult::Splitted(mid_key, self, new_node)
    //         }
    //     }
    // }

    pub fn find_holes(&self) -> Vec<(usize, usize)> {
        let page = self.page();
        let mut cells = Vec::new();
        for i in 0..self.num_cells() {
            let pos = self.cell_pointer(i);
            let cell = self.cell_at(i);
            let size = cell.cell_size();
            cells.push((pos as usize, size as usize));
        }
        cells.sort_by_key(|(start, _size)| *start);
        let hole_start = cells[0].0 + cells[0].1;
        // for i in 1..cells.len() {
        //     holes.push((hole_start, cell[hole]));
        // }
        cells
    }

    // pub fn node_insert(
    //     self,
    //     key: u32,
    //     payload: &[u8],
    // ) -> InsertResult<'a, BLOCKSIZE, CAPACITY, MEMORY_CAPACITY> {
    //     let node_type = self.node_type();
    //     match node_type {
    //         NodeType::Leaf => return self.leaf_insert(key, payload, None),
    //         NodeType::Interior => {
    //             // Find the child to insert the payload into
    //             let hole = self.search(key);
    //             let hole = match hole {
    //                 Slot::Hole(hole) => hole,
    //                 Slot::Cell(cell) => cell,
    //             };
    //             let right_child = self.right_child();
    //             let offset = self.cell_pointer(hole);
    //             let child = {
    //                 if hole >= self.num_cells() {
    //                     right_child
    //                 } else {
    //                     {
    //                         // let pager = self.pager.clone();
    //                         // let mut pager = pager.lock().unwrap();
    //                         // let page = pager.get_page(self.page_num as usize).unwrap();
    //                         // let cell = unsafe {Cell::interior_at(page.as, offset as usize)};
    //                         // cell.child()
    //                         todo!();
    //                     }
    //                 }
    //             };
    //             let result = {
    //                 let to_insert_node = Node::from(self.buffer_manager, self.disk_manager, child);
    //                 to_insert_node.node_insert(key, payload)
    //             };
    //             match result {
    //                 InsertResult::Normal(_node) => InsertResult::Normal(self),
    //                 InsertResult::Splitted(returned_key, left, right) => {
    //                     let num_cells = self.num_cells();
    //                     if hole >= num_cells {
    //                         self.set_right_child(right.page_number);
    //                     } else {
    //                         let cell_offset = self.cell_pointer(hole);
    //                         // let page = pager.get_page(self.page_num as usize).unwrap();
    //                         // let cell = Cell::interior_at(&page, cell_offset as usize);
    //                         // cell.set_child(right.page_num);
    //                         todo!();
    //                     }
    //                     self.interior_insert(returned_key, left.page_number)
    //                 }
    //                 InsertResult::KeyExisted(key) => InsertResult::KeyExisted(key),
    //             }
    //         }
    //     }
    // }

    fn overflow_amount(&self, payload_size: u32) -> Option<u32> {
        let free_size = self.free_size();
        if payload_size < free_size as u32 - 12 {
            None
        } else {
            Some(payload_size - free_size as u32 + 200)
        }
    }

    fn min_threshold_for_non_overflow(&self) -> usize {
        let m = ((BLOCKSIZE - 12) * 32 / 255) - 23;
        println!("{}", m);
        m
    }

    fn cell_content_start(&self) -> u32 {
        let page = self.page();
        unsafe { NodeHeaderReader::new(page.as_ptr()).cell_content_start() }
    }

    fn set_cell_content_start(&mut self, val: u32) {
        let mut page = self.page();
        unsafe { NodeHeaderWriter::new(page.as_mut_ptr()).set_cell_content_start(val) }
    }

    fn set_cell_pointer(&mut self, cell_num: u32, val: u32) {
        let mut page = self.page();
        unsafe { NodeHeaderWriter::new(page.as_mut_ptr()).set_cell_pointer(cell_num, val) }
    }

    fn free_size(&self) -> usize {
        self.cell_content_start() as usize - self.cell_pointer_offset(self.num_cells())
    }

    fn cell_pointer(&self, cell_num: u32) -> CellPointer {
        let page = self.page();
        unsafe { NodeHeaderReader::new(page.as_ptr()).cell_pointer(cell_num) }
    }

    fn insert_decision(&self, payload_size: usize) -> InsertDecision {
        let free_size = self.free_size();
        let node_type = self.node_type();
        if self.num_cells() > 3 {
            return InsertDecision::Split;
        }
        match node_type {
            NodeType::Interior => {
                if free_size < payload_size {
                    return InsertDecision::Split;
                } else {
                    return InsertDecision::Normal;
                }
            }
            NodeType::Leaf => {
                if free_size < 30 {
                    return InsertDecision::Split;
                } else if free_size < payload_size {
                    return InsertDecision::Overflow(free_size);
                } else {
                    return InsertDecision::Normal;
                }
            }
        }
    }
}

#[cfg(test)]
mod node {
    use crate::{
        btree_index::btree::node::node_header::NodeType,
        buffer_manager::BufferManager,
        disk_manager::DiskManager,
    };

    use super::Node;

    #[test]
    fn basic_header() {
        const BLOCK_SIZE: usize = 4096;
        const DISK_CAPACITY: usize = 4096 * 32;
        const MEMORY_CAPACITY: usize = 4096 * 16;

        let memory = [0; MEMORY_CAPACITY];
        let disk = disk::Disk::<BLOCK_SIZE, DISK_CAPACITY>::create("basic_header").unwrap();
        let buffer_manager: BufferManager<'_, BLOCK_SIZE, DISK_CAPACITY, MEMORY_CAPACITY> =
            BufferManager::init(&memory, &disk);
        let disk_manager = DiskManager::init(&disk);

        let mut node = Node::new(&buffer_manager, &disk_manager);
        node.set_node_type(NodeType::Leaf);
        assert_eq!(node.node_type(), NodeType::Leaf);
        assert_eq!(node.num_cells(), 0);
        assert_eq!(node.right_child(), 0);
        assert_eq!(node.free_size(), 4084);
        assert_eq!(node.cell_content_start(), 4084);

        node.set_right_child(1);
        assert_eq!(node.right_child(), 1);
    }

    // #[test]
    // fn find_holes() {
    //     const BLOCK_SIZE: usize = 4096;
    //     const DISK_CAPACITY: usize = 4096 * 32;
    //     const MEMORY_CAPACITY: usize = 4096 * 16;

    //     let memory = [0; MEMORY_CAPACITY];
    //     let disk = disk::Disk::<BLOCK_SIZE, DISK_CAPACITY>::create("find_holes").unwrap();
    //     let buffer_manager: BufferManager<'_, BLOCK_SIZE, DISK_CAPACITY, MEMORY_CAPACITY> =
    //         BufferManager::init(&memory, &disk);
    //     let disk_manager = DiskManager::init(&disk);

    //     let mut node = Node::new(&buffer_manager, &disk_manager);
    //     node.set_node_type(NodeType::Leaf);
    //     let data = vec![
    //         (12, [1, 2, 3]),
    //         (222, [1, 2, 3]),
    //         (11, [1, 2, 3]),
    //         (88, [1, 2, 3]),
    //         (8, [1, 2, 3]),
    //     ];
    //     for datum in &data[0..4] {
    //         node = match node.node_insert(datum.0, &datum.1) {
    //             InsertResult::Normal(node) => node,
    //             _ => unreachable!(),
    //         };
    //     }
    //     match node.node_insert(data[4].0, &data[4].1) {
    //         InsertResult::Splitted(key, left, right) => {
    //             println!("{:#?}", left.find_holes());
    //         }
    //         _ => unreachable!(),
    //     };
    //     panic!()
    // }

    // #[test]
    // fn split_logic() {
    //     const BLOCK_SIZE: usize = 4096;
    //     const DISK_CAPACITY: usize = 4096 * 32;
    //     const MEMORY_CAPACITY: usize = 4096 * 16;

    //     let memory = [0; MEMORY_CAPACITY];
    //     let disk = disk::Disk::<BLOCK_SIZE, DISK_CAPACITY>::create("find_holes").unwrap();
    //     let buffer_manager: BufferManager<'_, BLOCK_SIZE, DISK_CAPACITY, MEMORY_CAPACITY> =
    //         BufferManager::init(&memory, &disk);
    //     let disk_manager = DiskManager::init(&disk);

    //     let mut node = Node::new(&buffer_manager, &disk_manager);
    //     node.set_node_type(NodeType::Leaf);
    //     let data = vec![
    //         (12, [1, 2, 3]),
    //         (222, [1, 2, 3]),
    //         (11, [1, 2, 3]),
    //         (88, [1, 2, 3]),
    //         (8, [1, 2, 3]),
    //         (1, [1, 2, 3]),
    //         (111, [1, 2, 3]),
    //         (333, [1, 2, 3]),
    //         (7777, [1, 2, 3]),
    //         (23, [1, 2, 3]),
    //         (56, [1, 2, 3]),
    //         (12521, [1, 2, 3]),
    //     ];
    //     for datum in &data[0..4] {
    //         node = match node.node_insert(datum.0, &datum.1) {
    //             InsertResult::Normal(node) => node,
    //             _ => unreachable!(),
    //         };
    //     }
    //     node = match node.node_insert(data[4].0, &data[4].1) {
    //         InsertResult::Splitted(key, left, right) => {
    //             let mut node = Node::new(&buffer_manager, &disk_manager);
    //             node.set_node_type(NodeType::Interior);
    //             node.set_right_child(right.page_number);
    //             let node = match node.interior_insert(key, left.page_number) {
    //                 InsertResult::Normal(node) => node,
    //                 _ => unreachable!(),
    //             };
    //             node
    //         }
    //         _ => unreachable!(),
    //     };
    //     for datum in &data[5..] {
    //         node = match node.node_insert(datum.0, &datum.1) {
    //             InsertResult::Normal(node) => {
    //                 println!("{:#?}", node);
    //                 node
    //             }
    //             _ => unreachable!(),
    //         };
    //     }
    //     panic!();
    // }

    // #[test]
    // fn interior_insert() {
    //     const BLOCK_SIZE: usize = 4096;
    //     const DISK_CAPACITY: usize = 4096 * 32;
    //     const MEMORY_CAPACITY: usize = 4096 * 16;

    //     let memory = [0; MEMORY_CAPACITY];
    //     let disk = disk::Disk::<BLOCK_SIZE, DISK_CAPACITY>::create("interior_insert").unwrap();
    //     let buffer_manager: BufferManager<'_, BLOCK_SIZE, DISK_CAPACITY, MEMORY_CAPACITY> =
    //         BufferManager::init(&memory, &disk);
    //     let disk_manager = DiskManager::init(&disk);

    //     let node = Node::new(&buffer_manager, &disk_manager);
    //     node.set_right_child(4);
    //     assert_eq!(node.right_child(), 4);
    //     let node = match node.interior_insert(12, 42) {
    //         InsertResult::Normal(node) => node,
    //         _ => unreachable!(),
    //     };
    //     let node = match node.interior_insert(42, 143) {
    //         InsertResult::Normal(node) => node,
    //         _ => unreachable!(),
    //     };
    //     assert_eq!(node.right_child(), 4);
    // }

    // #[test]
    // fn insert_cell_pointer() {
    //     const BLOCK_SIZE: usize = 4096;
    //     const DISK_CAPACITY: usize = 4096 * 32;
    //     const MEMORY_CAPACITY: usize = 4096 * 16;

    //     let memory = [0; MEMORY_CAPACITY];
    //     let disk = disk::Disk::<BLOCK_SIZE, DISK_CAPACITY>::create("insert_cell_pointer").unwrap();
    //     let buffer_manager: BufferManager<'_, BLOCK_SIZE, DISK_CAPACITY, MEMORY_CAPACITY> =
    //         BufferManager::init(&memory, &disk);
    //     let disk_manager = DiskManager::init(&disk);

    //     let mut node = Node::new(&buffer_manager, &disk_manager);
    //     node.insert_cell_pointer(0, 12);
    //     assert_eq!(node.cell_pointer(0), 12);
    //     assert_eq!(node.num_cells(), 1);

    //     node.insert_cell_pointer(1, 15);
    //     assert_eq!(node.cell_pointer(1), 15);
    //     assert_eq!(node.num_cells(), 2);

    //     node.insert_cell_pointer(0, 1521);
    //     assert_eq!(node.cell_pointer(0), 1521);
    //     assert_eq!(node.num_cells(), 3);

    //     node.insert_cell_pointer(2, 643);
    //     assert_eq!(node.cell_pointer(2), 643);
    //     assert_eq!(node.num_cells(), 4);
    // }

    // fn create_leaf_node_samples<
    //     'a,
    //     const BLOCKSIZE: usize,
    //     const CAPACITY: usize,
    //     const MEMORY_CAPACITY: usize,
    // >(
    //     disk_manager: &DiskManager<BLOCKSIZE, CAPACITY>,
    //     buffer_manager: &BufferManager<'a, BLOCKSIZE, CAPACITY, MEMORY_CAPACITY>,
    //     sample_size: usize,
    // ) -> Vec<u32> {
    //     let mut children = Vec::new();
    //     for i in 0..sample_size {
    //         let mut node = Node::new(buffer_manager, disk_manager);
    //         node.set_node_type(NodeType::Leaf);
    //         let node = node.leaf_insert(i as u32, &[1, 2, 3], None);
    //         match node {
    //             InsertResult::Normal(node) => {
    //                 println!("{:#?}", node);
    //                 children.push(node.page_number);
    //             }
    //             _ => unreachable!(),
    //         }
    //     }
    //     children
    // }

    // #[test]
    // fn single_interior_insert() {
    //     const BLOCK_SIZE: usize = 4096;
    //     const DISK_CAPACITY: usize = 4096 * 32;
    //     const MEMORY_CAPACITY: usize = 4096 * 16;

    //     let memory = [0; MEMORY_CAPACITY];
    //     let disk =
    //         disk::Disk::<BLOCK_SIZE, DISK_CAPACITY>::create("single_interior_insert").unwrap();
    //     let buffer_manager: BufferManager<'_, BLOCK_SIZE, DISK_CAPACITY, MEMORY_CAPACITY> =
    //         BufferManager::init(&memory, &disk);
    //     let disk_manager = DiskManager::init(&disk);

    //     let children = create_leaf_node_samples(&disk_manager, &buffer_manager, 5);
    //     let mut node = Node::new(&buffer_manager, &disk_manager);
    //     node.set_node_type(NodeType::Interior);
    //     let node = if let InsertResult::Normal(node) = node.interior_insert(22, children[0]) {
    //         node
    //     } else {
    //         unreachable!()
    //     };
    //     let node = if let InsertResult::Normal(node) = node.interior_insert(12, children[1]) {
    //         node
    //     } else {
    //         unreachable!()
    //     };
    //     let node = if let InsertResult::Normal(node) = node.interior_insert(300, children[2]) {
    //         node
    //     } else {
    //         unreachable!()
    //     };
    //     let node = if let InsertResult::Normal(node) = node.interior_insert(1242, children[2]) {
    //         node
    //     } else {
    //         unreachable!()
    //     };
    //     println!("{:#?}", node);
    //     let node = match node.interior_insert(200, children[4] as u32) {
    //         InsertResult::Splitted(key, left, right) => {
    //             println!("{:#?}", key);
    //             println!("{:#?}", left);
    //             println!("{:#?}", right);
    //             left
    //         }
    //         _ => unreachable!("Bad result"),
    //     };
    //     let page = pager.lock().unwrap().get_page(0).unwrap();
    //     let cell = node.cell_at(&page, 0);
    //     assert_eq!(cell.key(), 22);
    //     assert_eq!(cell.child(), 12);
    //     for i in 0..node.num_cells() - 1 {
    //         let lo = node.cell_at(i).key();
    //         let hi = node.cell_at(i + 1).key();
    //         assert!(lo < hi, "Key should be sorted: {} > {}", lo, hi);
    //     }
    // }

    // #[test]
    // fn basic_leaf_insert() {
    //     const BLOCK_SIZE: usize = 4096;
    //     const DISK_CAPACITY: usize = 4096 * 32;
    //     const MEMORY_CAPACITY: usize = 4096 * 16;

    //     let memory = [0; MEMORY_CAPACITY];
    //     let disk = disk::Disk::<BLOCK_SIZE, DISK_CAPACITY>::create("basic_leaf_insert").unwrap();
    //     let buffer_manager: BufferManager<'_, BLOCK_SIZE, DISK_CAPACITY, MEMORY_CAPACITY> =
    //         BufferManager::init(&memory, &disk);
    //     let disk_manager = DiskManager::init(&disk);

    //     let mut node = Node::new(&buffer_manager, &disk_manager);
    //     let node =
    //         if let InsertResult::Normal(node) = node.leaf_insert(22, &[1, 2, 3, 4, 5, 6], None) {
    //             node
    //         } else {
    //             unreachable!()
    //         };
    //     let cell = node.cell_at(node.cell_pointer(0));
    //     assert_eq!(cell.kept_payload(), &[1, 2, 3, 4, 5, 6],);
    // }

    // #[test]
    // fn leaf_insert_cell() {
    //     const BLOCK_SIZE: usize = 4096;
    //     const DISK_CAPACITY: usize = 4096 * 32;
    //     const MEMORY_CAPACITY: usize = 4096 * 16;

    //     let memory = [0; MEMORY_CAPACITY];
    //     let disk = disk::Disk::<BLOCK_SIZE, DISK_CAPACITY>::create("leaf_insert_cell").unwrap();
    //     let buffer_manager: BufferManager<'_, BLOCK_SIZE, DISK_CAPACITY, MEMORY_CAPACITY> =
    //         BufferManager::init(&memory, &disk);
    //     let disk_manager = DiskManager::init(&disk);

    //     let mut node = Node::new(&buffer_manager, &disk_manager);
    //     let node =
    //         if let InsertResult::Normal(node) = node.leaf_insert(22, &[1, 2, 3, 4, 5, 6], None) {
    //             node
    //         } else {
    //             unreachable!()
    //         };
    //     let node = if let InsertResult::Normal(node) = node.leaf_insert(12, &[1, 2, 3], None) {
    //         node
    //     } else {
    //         unreachable!()
    //     };
    //     let node = if let InsertResult::Normal(node) = node.leaf_insert(124, &[1, 2, 5, 6], None) {
    //         node
    //     } else {
    //         unreachable!()
    //     };
    //     let cell = node.cell_at(0);
    //     assert_eq!(cell.kept_payload(), &[1, 2, 3],);
    //     let cell = node.cell_at(1);
    //     assert_eq!(cell.kept_payload(), &[1, 2, 3, 4, 5, 6]);
    //     for i in 0..node.num_cells() - 1 {
    //         let lo = node.cell_at(i).key();
    //         let hi = node.cell_at(i + 1).key();
    //         assert!(lo < hi, "Key should be sorted: {} > {}", lo, hi);
    //     }
    // }

    // #[test]
    // fn leaf_insert_split() {
    //     const BLOCK_SIZE: usize = 4096;
    //     const DISK_CAPACITY: usize = 4096 * 32;
    //     const MEMORY_CAPACITY: usize = 4096 * 16;

    //     let memory = [0; MEMORY_CAPACITY];
    //     let disk = disk::Disk::<BLOCK_SIZE, DISK_CAPACITY>::create("leaf_insert_split").unwrap();
    //     let buffer_manager: BufferManager<'_, BLOCK_SIZE, DISK_CAPACITY, MEMORY_CAPACITY> =
    //         BufferManager::init(&memory, &disk);
    //     let disk_manager = DiskManager::init(&disk);

    //     let node = Node::new(&buffer_manager, &disk_manager);
    //     let node = match node.leaf_insert(533, &[2; 50], None) {
    //         InsertResult::Normal(node) => node,
    //         _ => unreachable!(),
    //     };
    //     let node = match node.leaf_insert(22, &[1; 12], None) {
    //         InsertResult::Normal(node) => node,
    //         _ => unreachable!(),
    //     };
    //     let node = match node.leaf_insert(12, &[9; 3], None) {
    //         InsertResult::Normal(node) => node,
    //         _ => unreachable!(),
    //     };
    //     let node = match node.leaf_insert(124, &[1, 2, 5, 6], None) {
    //         InsertResult::Normal(node) => node,
    //         _ => unreachable!(),
    //     };
    //     let node = match node.leaf_insert(5, &[4; 15], None) {
    //         InsertResult::Splitted(k, l, r) => {
    //             let mut node = Node::new(&buffer_manager, &disk_manager);
    //             node.set_node_type(NodeType::Interior);
    //             node.set_right_child(r.page_number);

    //             let page = node.page();
    //             let node = match node.interior_insert(k, l.page_number) {
    //                 InsertResult::Normal(node) => node,
    //                 _ => unreachable!(),
    //             };
    //             println!("{:?}", node);
    //             panic!();
    //             node
    //         }
    //         _ => unreachable!(),
    //     };
    // }
}
