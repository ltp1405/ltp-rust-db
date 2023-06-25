mod btree;
mod cell;
mod node;

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
    Interior = 0x2,
    Leaf = 0x5,
}
