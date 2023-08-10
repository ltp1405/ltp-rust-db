use std::mem::size_of;

use crate::btree_index::btree::node::node_header::NodePointer;

pub type PayloadSize = u32;

/// (<offset>, <size>)
const KEY: (usize, usize) = (0, size_of::<u32>());
/// (<offset>, <size>)
const CHILD: (usize, usize) = (KEY.0 + KEY.1, size_of::<NodePointer>());
/// (<offset>, <size>)
const PAYLOAD_SIZE: (usize, usize) = (KEY.0 + KEY.1, size_of::<PayloadSize>());
/// (<offset>, <size>)
const OVERFLOW_PAGE_HEAD: (usize, usize) = (PAYLOAD_SIZE.0 + PAYLOAD_SIZE.1, size_of::<u32>());
/// (<offset>, <size>)
const CELL_SIZE: (usize, usize) = (
    OVERFLOW_PAGE_HEAD.0 + OVERFLOW_PAGE_HEAD.1,
    size_of::<u32>(),
);

const PAYLOAD_START: usize = CELL_SIZE.0 + CELL_SIZE.1;

pub struct CellHeaderReader {
    start: *const u8,
}

impl CellHeaderReader {
    pub unsafe fn new(start: *const u8) -> Self {
        Self { start }
    }

    pub fn key(&self) -> u32 {
        unsafe { *(self.start.add(KEY.0) as *const u32) }
    }

    pub fn child(&self) -> NodePointer {
        unsafe { *(self.start.add(CHILD.0) as *const NodePointer) }
    }

    pub fn payload_size(&self) -> PayloadSize {
        unsafe { *(self.start.add(PAYLOAD_SIZE.0) as *const PayloadSize) }
    }

    pub fn overflow_page_head(&self) -> u32 {
        unsafe { *(self.start.add(OVERFLOW_PAGE_HEAD.0) as *const u32) }
    }

    pub fn cell_size(&self) -> u32 {
        unsafe { *(self.start.add(CELL_SIZE.0) as *const u32) }
    }
}

pub struct CellHeaderWriter {
    start: *mut u8,
}

impl CellHeaderWriter {
    pub unsafe fn new(start: *mut u8) -> Self {
        Self { start }
    }

    pub fn set_key(&mut self, key: u32) {
        unsafe { *(self.start.add(KEY.0) as *mut u32) = key }
    }

    pub fn set_child(&mut self, child: NodePointer) {
        unsafe { *(self.start.add(CHILD.0) as *mut NodePointer) = child }
    }

    pub fn set_payload_size(&mut self, payload_size: PayloadSize) {
        unsafe { *(self.start.add(PAYLOAD_SIZE.0) as *mut PayloadSize) = payload_size }
    }

    pub fn set_overflow_page_head(&mut self, overflow_page_head: u32) {
        unsafe { *(self.start.add(OVERFLOW_PAGE_HEAD.0) as *mut u32) = overflow_page_head }
    }

    pub fn set_cell_size(&mut self, cell_size: u32) {
        unsafe { *(self.start.add(CELL_SIZE.0) as *mut u32) = cell_size }
    }
}
