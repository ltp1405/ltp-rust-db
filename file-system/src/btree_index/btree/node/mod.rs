pub mod cell;
mod node_header;

#[cfg(test)]
mod tests;

use crate::{
    buffer_manager::{BufferManager, Page},
    disk_manager::DiskManager,
};
use std::{
    fmt::Debug,
    ptr::{slice_from_raw_parts, slice_from_raw_parts_mut},
};

use self::cell::{Cell, CellMut};
use self::node_header::*;

use super::RowAddress;

/// Each node of the btree is contained inside 1 page
pub struct Node<'a, const BLOCKSIZE: usize, const CAPACITY: usize, const MEMORY_CAPACITY: usize> {
    page_number: u32,
    disk_manager: &'a DiskManager<BLOCKSIZE, CAPACITY>,
    buffer_manager: &'a BufferManager<'a, BLOCKSIZE, CAPACITY, MEMORY_CAPACITY>,
}

impl<'a, const BLOCKSIZE: usize, const CAPACITY: usize, const MEMORY_CAPACITY: usize> Debug
    for Node<'a, BLOCKSIZE, CAPACITY, MEMORY_CAPACITY>
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self.node_type() {
            NodeType::Leaf => {
                let mut cells = Vec::new();
                for i in 0..self.num_cells() {
                    let cell = self.cell_at(i);
                    cells.push(cell);
                }
                f.debug_struct("LeafNode")
                    .field("address", &self.page_number)
                    .field("Space left", &self.free_size())
                    .field("cells_num", &cells.len())
                    .field("cells", &cells)
                    .finish()
            }
            NodeType::Interior => {
                let mut children = Vec::new();
                for i in 0..self.num_cells() {
                    let cell = self.cell_at(i);
                    let node =
                        Node::from(self.buffer_manager, self.disk_manager, cell.child_pointer());
                    let key = self.key_of_cell(i);
                    let max_key_display_size = 10;
                    let key_display_size = std::cmp::min(max_key_display_size, key.len());
                    let key_display = &key[..key_display_size];
                    let key = String::from_utf8(key_display.to_vec()).unwrap();
                    children.push((key, node));
                }
                let right_child =
                    Node::from(self.buffer_manager, self.disk_manager, self.right_child());
                f.debug_struct("InteriorNode")
                    .field("address", &self.page_number)
                    .field("Space left", &self.free_size())
                    .field("children_num", &self.num_cells())
                    .field("children", &children)
                    .field("right_most_child", &right_child)
                    .finish()
            }
        }
    }
}

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

#[derive(Debug)]
pub enum InsertResult<
    'a,
    const BLOCKSIZE: usize,
    const CAPACITY: usize,
    const MEMORY_CAPACITY: usize,
> {
    KeyExisted(Node<'a, BLOCKSIZE, CAPACITY, MEMORY_CAPACITY>),
    Normal(Node<'a, BLOCKSIZE, CAPACITY, MEMORY_CAPACITY>),
    Splitted(
        Vec<u8>,
        Node<'a, BLOCKSIZE, CAPACITY, MEMORY_CAPACITY>,
        Node<'a, BLOCKSIZE, CAPACITY, MEMORY_CAPACITY>,
    ),
}

impl<'a, const BLOCKSIZE: usize, const CAPACITY: usize, const MEMORY_CAPACITY: usize>
    Node<'a, BLOCKSIZE, CAPACITY, MEMORY_CAPACITY>
{
    pub fn new(
        node_type: NodeType,
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
            node.set_cell_content_start(BLOCKSIZE as CellContentOffset);
        }
        node.set_node_type(node_type);
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
        let node_type = unsafe { NodeHeaderReader::new(self.page().as_ptr()).node_type() };
        NodeType::from(node_type)
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
        let (cell_ptr, cell_size) =
            unsafe { NodeHeaderReader::new(page.as_ptr()).cell_pointer_and_size(cell_num) };
        (cell_ptr as usize, (cell_ptr + cell_size) as usize)
    }

    fn right_child(&self) -> NodePointer {
        assert_eq!(self.node_type(), NodeType::Interior);
        let page = self.page();
        unsafe { NodeHeaderReader::new(page.as_ptr()).right_most_child() }
    }

    pub fn set_right_child(&self, child: NodePointer) {
        assert_eq!(
            self.node_type(),
            NodeType::Interior,
            "Leaf node does not have right child"
        );
        let mut page = self.page();
        unsafe { NodeHeaderWriter::new(page.as_mut_ptr()).set_right_most_child(child) };
    }

    pub fn key_of_cell(&self, cell_num: u32) -> Vec<u8> {
        if cell_num >= self.num_cells() {
            panic!("Cell number out of bound");
        }
        let cell = self.cell_at(cell_num);
        let read_result = cell.key();
        match read_result {
            cell::PayloadReadResult::InPage { payload } => payload.to_vec(),
            cell::PayloadReadResult::InOverflow {
                payload_len,
                partial_payload,
                overflow_page_head,
            } => todo!(),
        }
    }

    fn row_address_of_cell(&self, cell_num: u32) -> RowAddress {
        if self.node_type() != NodeType::Leaf {
            panic!("Only leaf node has row address");
        }
        let cell = self.cell_at(cell_num);
        let row_address = cell.row_address();
        row_address
    }

    fn child_pointer_of_cell(&self, cell_num: u32) -> NodePointer {
        if self.node_type() != NodeType::Interior {
            panic!("Only interior node has child");
        }
        let cell = self.cell_at(cell_num);
        let child = cell.child_pointer();
        child
    }

    pub fn search(&self, search_key: &[u8]) -> Slot {
        let num_cells = self.num_cells();
        if num_cells == 0 {
            return Slot::Hole(0);
        }
        let mut hi = num_cells;
        let mut lo = 0;
        loop {
            let mid = (lo + hi) / 2;
            let mid_key = self.key_of_cell(mid);
            if search_key < &mid_key {
                if mid == 0 {
                    return Slot::Hole(0);
                } else if search_key > &self.key_of_cell(mid - 1) {
                    return Slot::Hole(mid);
                }
                hi = mid;
            } else if search_key > &mid_key {
                if mid == num_cells - 1 {
                    return Slot::Hole(num_cells);
                }
                lo = mid;
            } else {
                return Slot::Cell(mid);
            }
        }
    }

    fn insert_cell_pointer(&mut self, hole: u32, pointer: u16, size: u16) {
        self.set_num_cells(self.num_cells() + 1);
        let cell_num = self.num_cells();
        for cell_i in (hole + 1..cell_num).rev() {
            let (ptr, size) = self.cell_pointer_and_size(cell_i - 1);
            self.set_cell_pointer_and_size(cell_i, ptr, size);
        }
        self.set_cell_pointer_and_size(hole, pointer, size);
    }

    /// Insert a payload into a leaf node
    /// Return a normal node if insert normally
    /// Return a pair of node if need split
    fn leaf_insert(
        mut self,
        key: &[u8],
        row_address: RowAddress,
        overflow_head: Option<NodePointer>,
    ) -> InsertResult<'a, BLOCKSIZE, CAPACITY, MEMORY_CAPACITY> {
        if self.node_type() != NodeType::Leaf {
            panic!("Inserting into a non-leaf node");
        }
        match self.insert_decision(key.len()) {
            InsertDecision::Normal => {
                log::debug!("Inserting normally");
                let hole = self.search(key);
                let hole = match hole {
                    Slot::Hole(hole) => hole,
                    Slot::Cell(_cell) => panic!("Key already exists"),
                };
                let allocated_size = Cell::leaf_header_size() + key.len();
                let cell_start = self.cell_content_start() - allocated_size as u16;
                self.insert_cell_pointer(hole, cell_start as u16, allocated_size as u16);
                let mut cell = self.cell_mut_at(hole);
                cell.write_key(key);
                cell.set_row_address(row_address);
                cell.set_overflow_page_head(overflow_head);
                self.set_cell_content_start(cell_start as u16);
                InsertResult::Normal(self)
            }
            InsertDecision::Overflow(kept_size) => {
                todo!();
                // let hole = self.search(key);
                // let hole = match hole {
                //     Slot::Hole(hole) => hole,
                //     Slot::Cell(_cell) => panic!(),
                // };
                // let page = self.page();
                // let payload_len = payload.len();
                // let (non_overflow, overflow) = payload.split_at(kept_size);
                // let cell_start = self.cell_content_start();
                // let new_page = self.disk_manager.allocate().unwrap();
                // let cell = Cell::insert_table_leaf(
                //     &page,
                //     cell_start as usize,
                //     key,
                //     payload_len as u32,
                //     Some(new_page as u32),
                //     non_overflow,
                // );
                // // TODO: Handle remain payload
                // let cell_start = cell_start - (cell.header_size() as u32) - payload.len() as u32;
                // self.set_cell_content_start(cell_start);
                // self.insert_cell_pointer(hole, cell_start as u32);
                // InsertResult::Normal(self)
            }
            InsertDecision::Split => {
                log::debug!("Begin splitting on node: {:?}", self.page_number);
                let mut new_node =
                    Node::new(NodeType::Leaf, self.buffer_manager, self.disk_manager);
                let mid = self.num_cells() / 2;
                for i in mid..self.num_cells() {
                    let cell = self.cell_at(i);
                    new_node = if let InsertResult::Normal(node) = new_node.leaf_insert(
                        &self.key_of_cell(i),
                        self.row_address_of_cell(i),
                        cell.overflow_page_head(),
                    ) {
                        node
                    } else {
                        unreachable!("New node should not overflow")
                    };
                }
                log::debug!(
                    "Moved {} cell(s) to node: {}",
                    self.num_cells() - mid,
                    new_node.page_number
                );
                log::debug!("{} cell(s) remain", mid);
                // TODO: Handle hole after split
                let mid_key = self.key_of_cell(mid);
                self.set_num_cells(mid);
                let mut min_start = BLOCKSIZE;
                for i in 0..mid {
                    let bound = self.cell_pointer_and_size(i);
                    min_start = std::cmp::min(min_start, bound.0 as usize);
                }
                self.set_cell_content_start(min_start as u16);

                if key >= &mid_key {
                    new_node = match new_node.leaf_insert(key, row_address, overflow_head) {
                        InsertResult::Normal(node) => node,
                        _ => unreachable!("Maybe overflow calculation go wrong"),
                    }
                } else {
                    self = match self.leaf_insert(key, row_address, overflow_head) {
                        InsertResult::Normal(node) => node,
                        _ => unreachable!("Maybe overflow calculation go wrong"),
                    }
                };
                log::debug!("Splitting done on node: {}", self.page_number);
                InsertResult::Splitted(mid_key.to_vec(), self, new_node)
            }
        }
    }

    pub fn cell_at(&self, cell_num: u32) -> Cell {
        let page = self.page();
        let (offset, size) = self.cell_pointer_and_size(cell_num);
        let node_type = self.node_type();
        let buffer = unsafe {
            slice_from_raw_parts(
                (page.as_ptr() as *const u8).add(offset as usize),
                size as usize,
            )
        };
        match node_type {
            NodeType::Leaf => unsafe { Cell::leaf(&*buffer) },
            NodeType::Interior => unsafe { Cell::interior(&*buffer) },
        }
    }

    pub fn cell_mut_at(&mut self, cell_num: u32) -> CellMut {
        let mut page = self.page();
        let (offset, size) = self.cell_pointer_and_size(cell_num);
        let node_type = self.node_type();
        let buffer = unsafe {
            slice_from_raw_parts_mut(
                (page.as_mut_ptr() as *mut u8).add(offset as usize),
                size as usize,
            )
        };
        match node_type {
            NodeType::Leaf => unsafe { CellMut::leaf(&mut *buffer) },
            NodeType::Interior => unsafe { CellMut::interior(&mut *buffer) },
        }
    }

    pub fn interior_insert(
        mut self,
        key: &[u8],
        child: NodePointer,
        overflow_head: Option<NodePointer>,
    ) -> InsertResult<'a, BLOCKSIZE, CAPACITY, MEMORY_CAPACITY> {
        if self.node_type() != NodeType::Interior {
            panic!("Not interior node");
        }
        match self.insert_decision(key.len()) {
            InsertDecision::Normal => {
                let hole = self.search(key);
                let hole = match hole {
                    Slot::Hole(hole) => hole,
                    Slot::Cell(_cell) => return InsertResult::KeyExisted(self),
                };
                let allocated_size = Cell::interior_header_size() + key.len();
                let cell_start = self.cell_content_start() - allocated_size as u16;
                self.insert_cell_pointer(hole, cell_start as u16, allocated_size as u16);
                let mut cell = self.cell_mut_at(hole);
                cell.write_key(key);
                cell.set_child_pointer(child);
                cell.set_overflow_page_head(overflow_head);
                self.set_cell_content_start(cell_start as u16);
                InsertResult::Normal(self)
            }
            InsertDecision::Overflow(_kept_size) => {
                unreachable!()
            }
            InsertDecision::Split => {
                let mut new_node =
                    Node::new(NodeType::Interior, self.buffer_manager, self.disk_manager);
                new_node.set_node_type(NodeType::Interior);
                let mid = self.num_cells() / 2;
                for i in mid..self.num_cells() {
                    new_node = match new_node.interior_insert(
                        &self.key_of_cell(i),
                        self.child_pointer_of_cell(i),
                        None,
                    ) {
                        InsertResult::Normal(node) => node,
                        _ => unreachable!(),
                    };
                }
                // TODO: Handle hole after split
                let mid_key = self.key_of_cell(mid);
                self.set_num_cells(mid);

                let mut min_start = BLOCKSIZE;
                for i in 0..mid {
                    let bound = self.cell_pointer_and_size(i);
                    min_start = std::cmp::min(min_start, bound.0 as usize);
                }
                self.set_cell_content_start(min_start as u16);

                if key >= &mid_key {
                    new_node = match new_node.interior_insert(key, child, None) {
                        InsertResult::Normal(node) => node,
                        _ => unreachable!("Maybe overflow calculation go wrong"),
                    }
                } else {
                    self = match self.interior_insert(key, child, None) {
                        InsertResult::Normal(node) => node,
                        _ => unreachable!("Maybe overflow calculation go wrong"),
                    }
                };
                InsertResult::Splitted(mid_key, self, new_node)
            }
        }
    }

    fn clean_holes(&mut self) {
        let bounds = self.cell_bounds();
        let mut collected_cells = Vec::new();
        for i in 0..bounds.len() - 1 {
            let bound = bounds[i];
            let next_bound = bounds[i + 1];
            collected_cells.push(bound);
            let hole_size = next_bound.1 - (bound.1 + bound.2);
            if hole_size > 0 {
                let start = collected_cells[0].1;
                let size = collected_cells[collected_cells.len() - 1].1
                    + collected_cells[collected_cells.len() - 1].2
                    - start;
                unsafe {
                    self.shift_slice(start, size, hole_size as isize);
                }
                for cell in &collected_cells {
                    self.set_cell_pointer_and_size(
                        cell.0 as u32,
                        cell.1 as u16 + hole_size as u16,
                        cell.2,
                    );
                }
                collected_cells.clear();
            }
        }
    }

    fn children(&self) -> Vec<Node<'a, BLOCKSIZE, CAPACITY, MEMORY_CAPACITY>> {
        let mut children = Vec::new();
        for i in 0..self.num_cells() {
            children.push(self.child_pointer_of_cell(i));
        }
        children
            .iter()
            .map(|child| Node::from(self.buffer_manager, self.disk_manager, *child))
            .collect()
    }

    fn cell_bounds(&self) -> Vec<(usize, u16, u16)> {
        let mut bounds = Vec::new();
        for i in 0..self.num_cells() {
            let bound = self.cell_pointer_and_size(i);
            bounds.push((i as usize, bound.0, bound.1));
        }
        bounds.sort_by_key(|bound| bound.1);
        bounds
    }

    /// Shift a cell by an offset
    /// ### Safety: This function does not check for boundary
    /// of cell, so it is possible to overwrite other cells.
    /// Will panic if the shift out of page.
    pub unsafe fn shift_cell(&mut self, idx: u16, offset: isize) {
        let (ptr, size) = self.cell_pointer_and_size(idx as u32);
        assert!(
            ptr as isize + size as isize + offset <= BLOCKSIZE as isize,
            "Shift out of page"
        );
        let mut page = self.page();
        let cell_slice = page.as_mut_ptr().add(ptr as usize);
        let new_cell_slice = page.as_mut_ptr().add((ptr as isize + offset) as usize);
        cell_slice.copy_to(new_cell_slice, size as usize);
        self.set_cell_pointer_and_size(idx as u32, (ptr as isize + offset) as u16, size);
    }

    pub unsafe fn shift_slice(&mut self, ptr: u16, size: u16, offset: isize) {
        assert!(
            ptr as isize + size as isize + offset <= BLOCKSIZE as isize,
            "Shift out of page"
        );
        let mut page = self.page();
        let cell_slice = page.as_mut_ptr().add(ptr as usize);
        let new_cell_slice = page.as_mut_ptr().add((ptr as isize + offset) as usize);
        cell_slice.copy_to(new_cell_slice, size as usize);
    }

    pub fn node_insert(
        mut self,
        key: &[u8],
        row_address: RowAddress,
    ) -> InsertResult<'a, BLOCKSIZE, CAPACITY, MEMORY_CAPACITY> {
        let node_type = self.node_type();
        match node_type {
            NodeType::Leaf => return self.leaf_insert(key, row_address, None),
            NodeType::Interior => {
                // Find the child to insert the payload into
                let hole = self.search(key);
                let hole = match hole {
                    Slot::Hole(hole) => hole,
                    Slot::Cell(cell) => cell,
                };
                let child = {
                    if hole >= self.num_cells() {
                        self.right_child()
                    } else {
                        self.child_pointer_of_cell(hole)
                    }
                };
                let result = {
                    let to_insert_node = Node::from(self.buffer_manager, self.disk_manager, child);
                    // Recursively insert the key payload until it reaches a leaf node
                    to_insert_node.node_insert(key, row_address)
                };
                match result {
                    InsertResult::Normal(_node) => InsertResult::Normal(self),
                    // if the under layer node is splitted, we need to update the child pointer
                    InsertResult::Splitted(returned_key, left, right) => {
                        let num_cells = self.num_cells();
                        if hole >= num_cells {
                            self.set_right_child(left.page_number);
                        } else {
                            let mut cell = self.cell_mut_at(hole);
                            cell.set_child_pointer(left.page_number);
                        }
                        self.interior_insert(&returned_key, right.page_number, None)
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
        let m = ((BLOCKSIZE - 12) * 32 / 255) - 23;
        m
    }

    fn cell_content_start(&self) -> CellContentOffset {
        let page = self.page();
        unsafe { NodeHeaderReader::new(page.as_ptr()).cell_content_start() }
    }

    fn set_cell_content_start(&mut self, val: CellContentOffset) {
        let mut page = self.page();
        unsafe { NodeHeaderWriter::new(page.as_mut_ptr()).set_cell_content_start(val) }
    }

    fn set_cell_pointer_and_size(&mut self, cell_num: u32, cell_pointer: u16, cell_size: u16) {
        if cell_pointer + cell_size > BLOCKSIZE as u16 {
            panic!("Cell pointer and size is too large");
        }
        let mut page = self.page();
        unsafe {
            NodeHeaderWriter::new(page.as_mut_ptr()).set_cell_pointer_and_size(
                cell_num,
                cell_pointer,
                cell_size,
            )
        }
    }

    fn free_size(&self) -> usize {
        self.cell_content_start() as usize - self.cell_pointer_offset(self.num_cells())
    }

    fn cell_pointer_and_size(&self, cell_num: u32) -> (CellPointer, CellSize) {
        let page = self.page();
        unsafe { NodeHeaderReader::new(page.as_ptr()).cell_pointer_and_size(cell_num) }
    }

    fn insert_decision(&self, payload_size: usize) -> InsertDecision {
        let free_size = self.free_size();
        let node_type = self.node_type();
        match node_type {
            NodeType::Interior => {
                if free_size < payload_size {
                    return InsertDecision::Split;
                } else {
                    return InsertDecision::Normal;
                }
            }
            NodeType::Leaf => {
                if free_size < 100 {
                    return InsertDecision::Split;
                // } else if free_size < payload_size {
                //     return InsertDecision::Overflow(free_size);
                } else {
                    return InsertDecision::Normal;
                }
            }
        }
    }
}
