use crate::btree_index::btree::node::node_header::NodePointer;
use crate::buffer_manager::Page;
use std::fmt::Debug;
use std::fmt::Display;

use super::cell_header::CellHeaderReader;
use super::cell_header::CellHeaderWriter;
use super::cell_header::PayloadSize;

pub struct CellData((u32, Vec<u8>));

impl Debug for Cell {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::TableLeaf(_page, _offset) => f
                .debug_struct("Cell::TableLeaf")
                .field("Key", &self.key())
                .field("Size", &self.cell_size())
                .field("Payload Size", &self.payload_size())
                .field("Kept Payload Size", &self.kept_payload().len())
                .field("Overflow Head", &self.overflow_page_head())
                .finish(),
            Self::TableInterior(_page, _offset) => f
                .debug_struct("Cell::TableInterior")
                .field("Key", &self.key())
                .field("child", &self.child())
                .finish(),
            _ => todo!(),
        }
    }
}

/// A key-value pair stored in a page
pub enum Cell {
    Interior(*const u8),
    Leaf(*const u8),
}

impl Cell {
    pub unsafe fn leaf_at(start: *const u8) -> Self {
        Self::Leaf(start)
    }
    
    pub unsafe fn interior_at(start: *const u8) -> Self {
        Self::Interior(start)
    }

    // pub fn insert_table_interior(
    //     page: &'a Page,
    //     tail: usize,
    //     key: u32,
    //     child: NodePointer,
    // ) -> Cell {
    //     let size = KEY.1 + CHILD.1;
    //     let offset = tail - size;
    //     unsafe {
    //         page.write_val_at(offset + KEY.0, key as u32);
    //         page.write_val_at(offset + CHILD.0, child as u32);
    //     }
    //     Cell::interior_at(page, offset)
    // }

    // pub fn insert_table_leaf(
    //     page: &'a Page,
    //     tail: usize,
    //     key: u32,
    //     payload_size: u32,
    //     overflow_page_head: Option<u32>,
    //     not_overflowed_payload: &[u8],
    // ) -> Cell<'a> {
    //     let size = KEY.1
    //         + PAYLOAD_SIZE.1
    //         + OVERFLOW_PAGE_HEAD.1
    //         + CELL_SIZE.1
    //         + not_overflowed_payload.len();
    //     let offset = tail - size;
    //     unsafe {
    //         page.write_val_at(offset + KEY.0, key as u32);
    //         page.write_val_at(offset + PAYLOAD_SIZE.0, payload_size as u32);
    //         match overflow_page_head {
    //             Some(head) => page.write_val_at(offset + OVERFLOW_PAGE_HEAD.0, head as u32),
    //             None => page.write_val_at(offset + OVERFLOW_PAGE_HEAD.0, 0 as u32),
    //         }
    //         let size = tail - offset;
    //         page.write_val_at(offset + CELL_SIZE.0, size as u32);
    //         page.write_buf_at(offset + PAYLOAD_START, not_overflowed_payload);
    //     }
    //     Cell::leaf_at(page, offset)
    // }

    pub fn child(&self) -> NodePointer {
        match self {
            Self::Interior(start) => unsafe { CellHeaderReader::new(*start).child() },
            _ => unreachable!("Only interior Node have children"),
        }
    }

    pub fn set_child(&self, child: NodePointer) {
        match self {
            Self::Interior(start) => unsafe {
                CellHeaderWriter::new(start).set_child(child);
            },
            _ => todo!(),
        }
    }

    pub fn cell_size(&self) -> u32 {
        match self {
            _ => todo!(),
        }
    }

    pub fn key(&self) -> u32 {
        match self {
            _ => todo!(),
        }
    }

    pub const fn header_size(&self) -> usize {
        match self {
            _ => todo!(),
        }
    }

    pub fn payload_size(&self) -> PayloadSize {
        match self {
            Self::Leaf(start) => unsafe { CellHeaderReader::new(*start).payload_size() },
            _ => todo!(),
        }
    }

    pub fn kept_payload(&self) -> &[u8] {
        match self {
            _ => todo!(),
        }
    }

    pub fn overflow_page_head(&self) -> Option<NodePointer> {
        match self {
            _ => todo!(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::Cell;

    #[test]
    fn simple_leaf_cell() {
        let payload: Vec<u8> = vec![1, 2, 3];
        let mut pager = PAGER.lock().unwrap();
        let page = pager.get_page(0).unwrap();
        let cell = Cell::insert_table_leaf(&page, PAGE_SIZE, 12, 3, None, &payload);
        assert_eq!(cell.key(), 12);
        assert_eq!(cell.payload_size(), 3);
        assert_eq!(cell.cell_size() as usize, cell.header_size() + 3);
        assert_eq!(cell.kept_payload(), &[1, 2, 3]);
        let cell2 = Cell::leaf_at(&page, PAGE_SIZE - cell.cell_size() as usize);
        println!("{:#?}", cell2);
        assert_eq!(cell2.payload_size() as usize, payload.len());
        assert_eq!(cell2.kept_payload(), payload);
        assert_eq!(cell2.key(), 12);
    }

    #[test]
    fn simple_interior_cell() {
        let mut pager = PAGER.lock().unwrap();
        let page = pager.get_page(0).unwrap();
        let cell = Cell::insert_table_interior(&page, PAGE_SIZE, 12, 3);
        assert_eq!(cell.key(), 12);
        let cell2 = Cell::interior_at(&page, PAGE_SIZE - cell.cell_size() as usize);
        println!("{:#?}", cell2);
        assert_eq!(cell2.key(), 12);
        assert_eq!(cell2.child(), 3);
    }
}
