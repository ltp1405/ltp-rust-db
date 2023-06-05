mod cell;
mod node;

pub use node::Node;

use std::mem::size_of;
use std::rc::Rc;
use std::sync::{Arc, Mutex};

use crate::page::{Page, Pager, PAGE_SIZE};
use crate::table::ROW_SIZE;

pub type LeafNodeKey = u32;
pub type NodePointer = u32;
pub type CellsCount = u32;
pub type CellPointerArray = u32;
pub type CellPointer = u32;

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
const CELL_POINTERS_ARRAY_OFFSET: usize = CELL_NUMS.0 + CELL_NUMS.1;
const CELL_POINTER_SIZE: usize = size_of::<CellPointer>();
/// (<offset>, <size>)
static COMMON_NODE_HEADER_SIZE: usize = NODE_TYPE.1 + PARENT_POINTER.1 + IS_ROOT.1;

/// Leaf Node Header Layout
static LEAF_NODE_HEADER_SIZE: usize = COMMON_NODE_HEADER_SIZE;

/// Leaf Node Body Layout
/// (<offset>, <size>)
static LEAF_NODE_KEY: (usize, usize) = (0, size_of::<LeafNodeKey>());
/// (<offset>, <size>)
static LEAF_NODE_VAL: (usize, usize) = (LEAF_NODE_KEY.1, ROW_SIZE);
static LEAF_NODE_CELL_SIZE: usize = LEAF_NODE_KEY.1 + LEAF_NODE_VAL.1;
static LEAF_NODE_SPACE_FOR_CELLS: usize = PAGE_SIZE - LEAF_NODE_HEADER_SIZE;
static LEAF_NODE_MAX_CELLS: usize = LEAF_NODE_SPACE_FOR_CELLS / LEAF_NODE_CELL_SIZE;

#[repr(u8)]
#[derive(Debug, PartialEq)]
pub enum NodeType {
    Interior,
    Leaf,
}
