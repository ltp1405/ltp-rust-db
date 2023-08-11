use std::mem::size_of;

use crate::buffer_manager::Page;

pub type LeafNodeKey = u32;
pub type NodePointer = u32;
pub type CellsCount = u32;
pub type CellPointerArray = u32;
pub type CellPointer = u16;
pub type CellSize = u16;
pub type CellContentOffset = u32;

/// Common Node Header Layout
/// (<offset>, <size>)
const NODE_TYPE: (usize, usize) = (0, size_of::<NodeType>());
/// (<offset>, <size>)
const CELL_NUMS: (usize, usize) = (NODE_TYPE.0 + NODE_TYPE.1, size_of::<CellPointerArray>());
/// (<offset>, <size>)
const CELL_CONTENT_START: (usize, usize) = (CELL_NUMS.0 + CELL_NUMS.1, size_of::<u32>());
/// (<offset>, <size>)
const RIGHT_MOST_CHILD_POINTER: (usize, usize) = (
    CELL_CONTENT_START.0 + CELL_CONTENT_START.1,
    size_of::<NodePointer>(),
);

const CELL_POINTERS_ARRAY_OFFSET: usize = RIGHT_MOST_CHILD_POINTER.0 + RIGHT_MOST_CHILD_POINTER.1;

const CELL_POINTER_SIZE: usize = size_of::<CellPointer>();

#[derive(Debug, PartialEq, Copy, Clone)]
#[non_exhaustive]
pub enum NodeType {
    Interior,
    Leaf,
}

impl NodeType {
    pub fn from_u8(byte: u8) -> Self {
        match byte {
            0 => Self::Interior,
            1 => Self::Leaf,
            _ => panic!("Invalid node type byte: {}", byte),
        }
    }

    pub fn to_u8(&self) -> u8 {
        match self {
            Self::Interior => 0,
            Self::Leaf => 1,
        }
    }
}

pub struct NodeHeaderReader {
    start: *const u8,
}

impl NodeHeaderReader {
    pub unsafe fn new(start: *const u8) -> Self {
        Self { start }
    }

    pub fn node_type(&self) -> NodeType {
        NodeType::from_u8(unsafe { *(self.start.add(NODE_TYPE.0)) })
    }

    pub fn num_cells(&self) -> CellsCount {
        unsafe { *(self.start.add(CELL_NUMS.0) as *const CellsCount) }
    }

    pub fn cell_content_start(&self) -> CellContentOffset {
        todo!()
    }

    fn cell_pointers_array_start(&self) -> *const u8 {
        unsafe { self.start.add(CELL_POINTERS_ARRAY_OFFSET) }
    }

    pub fn cell_pointer_offset(&self, cell_idx: u32) -> usize {
        CELL_POINTERS_ARRAY_OFFSET + (cell_idx as usize) * CELL_POINTER_SIZE
    }

    pub fn cell_pointer_and_size(&self, idx: u32) -> (CellPointer, CellSize) {
        let cell_pointer =
            unsafe { *(self.cell_pointers_array_start().add(idx as usize) as *const CellPointer) };
        let cell_size = unsafe {
            *(self
                .cell_pointers_array_start()
                .add(idx as usize + size_of::<CellPointer>()) as *const CellSize)
        };
        (cell_pointer, cell_size)
    }

    pub fn right_most_child(&self) -> NodePointer {
        unsafe { *(self.start.add(RIGHT_MOST_CHILD_POINTER.0) as *const NodePointer) }
    }

    pub fn cell_pointer_array(&self) -> CellPointerArray {
        unsafe { *(self.start.add(CELL_POINTERS_ARRAY_OFFSET) as *const CellPointerArray) }
    }
}

pub struct NodeHeaderWriter {
    start: *mut u8,
}

impl NodeHeaderWriter {
    pub unsafe fn new(start: *mut u8) -> Self {
        Self { start }
    }

    fn cell_pointer_offset(&self, cell_idx: u32) -> usize {
        CELL_POINTERS_ARRAY_OFFSET + (cell_idx as usize) * CELL_POINTER_SIZE
    }

    pub fn set_node_type(&mut self, node_type: NodeType) {
        unsafe {
            *(self.start.add(NODE_TYPE.0) as *mut u8) = node_type.to_u8();
        }
    }

    pub fn set_num_cells(&mut self, cell_nums: CellsCount) {
        unsafe {
            *(self.start.add(CELL_NUMS.0) as *mut CellsCount) = cell_nums;
        }
    }

    pub fn set_cell_content_start(&mut self, cell_content_start: CellContentOffset) {
        unsafe {
            *(self.start.add(CELL_CONTENT_START.0) as *mut CellContentOffset) = cell_content_start;
        }
    }

    pub fn set_cell_pointer(&mut self, idx: u32, cell_pointer: CellPointer) {
        unsafe {
            *(self.start.add(self.cell_pointer_offset(idx)) as *mut CellPointer) = cell_pointer;
        }
    }

    pub fn set_right_most_child(&mut self, right_most_child_pointer: NodePointer) {
        unsafe {
            *(self.start.add(RIGHT_MOST_CHILD_POINTER.0) as *mut NodePointer) =
                right_most_child_pointer;
        }
    }

    pub fn set_cell_pointer_array(&mut self, cell_pointer_array: CellPointerArray) {
        unsafe {
            *(self.start.add(CELL_POINTERS_ARRAY_OFFSET) as *mut CellPointerArray) =
                cell_pointer_array;
        }
    }
}
