mod cell;
mod node;
mod btree;

#[cfg(test)]
mod tests;

use std::mem::size_of;

pub type LeafNodeKey = u32;
pub type NodePointer = u32;
pub type CellsCount = u32;
pub type CellPointerArray = u32;
pub type CellPointer = u32;
pub type CellContentOffset = u32;

/// Common Node Header Layout
/// (<offset>, <size>)
const NODE_TYPE: (usize, usize) = (0, size_of::<NodeType>());
/// (<offset>, <size>)
const IS_ROOT: (usize, usize) = (NODE_TYPE.1, size_of::<bool>());
/// (<offset>, <size>)
const PARENT_POINTER: (usize, usize) = (IS_ROOT.0 + IS_ROOT.1, size_of::<NodePointer>());
/// (<offset>, <size>)
const CELL_NUMS: (usize, usize) = (
    PARENT_POINTER.0 + PARENT_POINTER.1,
    size_of::<CellPointerArray>(),
);
const CELL_CONTENT_START: (usize, usize) = (CELL_NUMS.0 + CELL_NUMS.1, size_of::<u32>());

const CELL_POINTERS_ARRAY_OFFSET: usize = CELL_CONTENT_START.0 + CELL_CONTENT_START.1;

/// (<offset>, <size>)
static COMMON_NODE_HEADER_SIZE: usize = NODE_TYPE.1 + PARENT_POINTER.1 + IS_ROOT.1;

const CELL_POINTER_SIZE: usize = size_of::<CellPointer>();

#[repr(u8)]
#[derive(Debug, PartialEq)]
pub enum NodeType {
    Interior,
    Leaf,
}
