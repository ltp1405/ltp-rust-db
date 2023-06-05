use std::{mem::size_of, slice};

use crate::page::Page;

use super::{NodePointer, NodeType};

pub struct CellData((u32, Vec<u8>));

/// A key-value pair stored in a page
pub enum Cell {
    Interior(*const u8),
    Leaf(*const u8),
}

type PayloadSize = u32;

/// (<offset>, <size>)
const LEFT_CHILD_PTR: (usize, usize) = (KEY.1, size_of::<NodePointer>());
/// (<offset>, <size>)
const KEY: (usize, usize) = (0, size_of::<u32>());
/// (<offset>, <size>)
const PAYLOAD_SIZE: (usize, usize) = (KEY.0, size_of::<PayloadSize>());
const PAYLOAD_START: usize = PAYLOAD_SIZE.0 + PAYLOAD_SIZE.1;

impl Cell {
    pub unsafe fn at(page: &Page, offset: usize, payload_size: usize) -> Self {
        let ptr = page.as_ref() as *const [u8];
        let ptr = ptr as *const u8;
        let ptr = unsafe { ptr.add(offset) };
        let node_type = unsafe { ptr.read_unaligned() };
        if node_type == NodeType::Leaf as u8 {
            Self::Leaf(ptr)
        } else if node_type == NodeType::Interior as u8 {
            Self::Interior(ptr)
        } else {
            panic!("Unvalid Node Type");
        }
    }

    fn ptr(&self) -> *const u8 {
        match self {
            Self::Interior(ptr) => *ptr,
            Self::Leaf(ptr) => *ptr,
        }
    }

    pub unsafe fn key(&self) -> u32 {
        let ptr = self.ptr() as *const u32;
        unsafe { ptr.read_unaligned() }
    }

    pub unsafe fn set_payload_size(&mut self, size: u32) {
        let ptr = unsafe { self.ptr().add(PAYLOAD_SIZE.0) } as *mut u32;
        unsafe { ptr.write_unaligned(size) }
    }

    pub unsafe fn payload_size(&self) -> u32 {
        let ptr = unsafe { self.ptr().add(PAYLOAD_SIZE.0) } as *const u32;
        unsafe { ptr.read_unaligned() }
    }

    pub unsafe fn payload(&self) -> &[u8] {
        if let Self::Interior(_) = self {
            panic!("Interior node cell does not have payload");
        }
        let ptr = self.ptr();
        let ptr = unsafe { ptr.add(PAYLOAD_START) };
        unsafe { slice::from_raw_parts(ptr, self.payload_size() as usize) }
    }

    pub fn left_child(&self) -> NodePointer {
        if let Self::Leaf(_) = self {
            panic!("Leaf not does not have left child");
        }

        let ptr = self.ptr();
        let ptr = unsafe { ptr.add(LEFT_CHILD_PTR.0) } as *const NodePointer;
        unsafe { ptr.read() }
    }

    pub unsafe fn key_size(&self) -> u32 {
        KEY.1 as u32
    }

    pub unsafe fn size(&self) -> u32 {
        unsafe { self.payload_size() + self.key_size() }
    }
}

#[cfg(test)]
mod tests {}
