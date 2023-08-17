use std::mem::size_of;

use crate::buffer_manager::Page;

pub type LeafNodeKey = u32;
pub type NodePointer = u32;
pub type CellsCount = u32;
pub type CellPointerArray = u32;
pub type CellPointer = u16;
pub type CellSize = u16;
pub type CellContentOffset = u16;

/// Common Node Header Layout
/// (<offset>, <size>)
const NODE_TYPE: (usize, usize) = (0, size_of::<NodeType>());
/// (<offset>, <size>)
const CELL_NUMS: (usize, usize) = (NODE_TYPE.0 + NODE_TYPE.1, size_of::<CellsCount>());
/// (<offset>, <size>)
const CELL_CONTENT_START: (usize, usize) =
    (CELL_NUMS.0 + CELL_NUMS.1, size_of::<CellContentOffset>());
/// (<offset>, <size>)
const RIGHT_MOST_CHILD_POINTER: (usize, usize) = (
    CELL_CONTENT_START.0 + CELL_CONTENT_START.1,
    size_of::<NodePointer>(),
);

const CELL_POINTERS_ARRAY_OFFSET: usize = RIGHT_MOST_CHILD_POINTER.0 + RIGHT_MOST_CHILD_POINTER.1;

const CELL_POINTER_SIZE: usize = size_of::<CellPointer>() + size_of::<CellSize>();

#[derive(Debug, PartialEq, Copy, Clone)]
#[non_exhaustive]
pub enum NodeType {
    Interior,
    Leaf,
}

impl NodeType {
    pub fn from_u8(byte: u8) -> Self {
        match byte {
            0x2 => Self::Interior,
            0x5 => Self::Leaf,
            _ => panic!("Invalid node type: {}", byte),
        }
    }

    pub fn to_u8(&self) -> u8 {
        match self {
            Self::Interior => 0x2,
            Self::Leaf => 0x5,
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
        CellsCount::from_be_bytes(unsafe { *(self.start.add(CELL_NUMS.0) as *const [u8; 4]) })
    }

    pub fn cell_content_start(&self) -> CellContentOffset {
        u16::from_be_bytes(unsafe { *(self.start.add(CELL_CONTENT_START.0) as *const [u8; 2]) })
            as CellContentOffset
    }

    pub fn cell_pointers_array_start(&self) -> usize {
        CELL_POINTERS_ARRAY_OFFSET
    }

    pub fn cell_pointer_offset(&self, cell_idx: u32) -> usize {
        CELL_POINTERS_ARRAY_OFFSET + (cell_idx as usize) * CELL_POINTER_SIZE
    }

    pub fn cell_pointer_and_size(&self, idx: u32) -> (CellPointer, CellSize) {
        let cell_pointer =
            unsafe { *(self.start.add(self.cell_pointer_offset(idx)) as *const [u8; 2]) };
        let cell_size = unsafe {
            *(self
                .start
                .add(self.cell_pointer_offset(idx) + size_of::<CellPointer>())
                as *const [u8; 2])
        };
        (
            CellPointer::from_be_bytes(cell_pointer),
            CellSize::from_be_bytes(cell_size),
        )
    }

    pub fn right_most_child(&self) -> NodePointer {
        NodePointer::from_be_bytes(unsafe {
            *(self.start.add(RIGHT_MOST_CHILD_POINTER.0) as *const [u8; 4])
        })
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
        let bytes = cell_nums.to_be_bytes();
        unsafe {
            *(self.start.add(CELL_NUMS.0) as *mut [u8; 4]) = bytes;
        }
    }

    pub fn set_cell_content_start(&mut self, cell_content_start: CellContentOffset) {
        let bytes = cell_content_start.to_be_bytes();
        unsafe {
            *(self.start.add(CELL_CONTENT_START.0) as *mut [u8; 2]) = bytes;
        }
    }

    pub fn set_cell_pointer_and_size(
        &mut self,
        idx: u32,
        cell_pointer: CellPointer,
        size: CellSize,
    ) {
        unsafe {
            *(self.start.add(self.cell_pointer_offset(idx)) as *mut [u8; 2]) =
                cell_pointer.to_be_bytes();
            *(self
                .start
                .add(self.cell_pointer_offset(idx) + size_of::<CellPointer>())
                as *mut [u8; 2]) = size.to_be_bytes();
        }
    }

    pub fn set_right_most_child(&mut self, right_most_child_pointer: NodePointer) {
        unsafe {
            *(self.start.add(RIGHT_MOST_CHILD_POINTER.0) as *mut [u8; 4]) =
                right_most_child_pointer.to_be_bytes();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_node_header_reader() {
        let mut buf = [0; 512];
        let mut writer = NodeHeaderWriter {
            start: buf.as_mut_ptr(),
        };
        writer.set_node_type(NodeType::Interior);
        writer.set_num_cells(0x12345678);
        writer.set_cell_content_start(0x1234);
        writer.set_right_most_child(0x12345678);
        writer.set_cell_pointer_and_size(0, 12, 34);
        writer.set_cell_pointer_and_size(1, 34, 12);
        println!("{:?}", buf);

        let reader = unsafe { NodeHeaderReader::new(buf.as_ptr()) };
        assert_eq!(reader.node_type(), NodeType::Interior);
        assert_eq!(reader.num_cells(), 0x12345678);
        assert_eq!(reader.cell_content_start(), 0x1234);
        assert_eq!(reader.right_most_child(), 0x12345678);
        assert_eq!(reader.cell_pointer_and_size(0), (12, 34));
        assert_eq!(reader.cell_pointer_and_size(1), (34, 12));
    }
}
