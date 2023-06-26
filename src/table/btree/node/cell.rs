use std::{
    fmt::{Debug, Display},
    mem::size_of,
    ptr::{slice_from_raw_parts, slice_from_raw_parts_mut},
};

use ltp_rust_db_page::page::{Page, PAGE_SIZE};

use super::{NodePointer, NodeType};

pub struct CellData((u32, Vec<u8>));

impl<'a> Debug for Cell<'a> {
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
pub enum Cell<'a> {
    TableLeaf(&'a Page, usize),
    TableInterior(&'a Page, usize),
    IndexInterior,
    IndexLeaf,
}

type PayloadSize = u32;

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

impl<'a> Cell<'a> {
    pub fn table_leaf_at(page: &'a Page, offset: usize) -> Self {
        Self::TableLeaf(page, offset)
    }

    pub fn table_interior_at(page: &'a Page, offset: usize) -> Self {
        Self::TableInterior(page, offset)
    }

    pub fn insert_table_interior(
        page: &'a Page,
        tail: usize,
        key: u32,
        child: NodePointer,
    ) -> Cell<'a> {
        let size = KEY.1 + CHILD.1;
        let offset = tail - size;
        unsafe {
            page.write_val_at(offset + KEY.0, key as u32);
            page.write_val_at(offset + CHILD.0, child as u32);
        }
        Cell::table_interior_at(page, offset)
    }

    pub fn insert_table_leaf(
        page: &'a Page,
        tail: usize,
        key: u32,
        payload_size: u32,
        overflow_page_head: Option<u32>,
        not_overflowed_payload: &[u8],
    ) -> Cell<'a> {
        let size = KEY.1
            + PAYLOAD_SIZE.1
            + OVERFLOW_PAGE_HEAD.1
            + CELL_SIZE.1
            + not_overflowed_payload.len();
        let offset = tail - size;
        unsafe {
            page.write_val_at(offset + KEY.0, key as u32);
            page.write_val_at(offset + PAYLOAD_SIZE.0, payload_size as u32);
            match overflow_page_head {
                Some(head) => page.write_val_at(offset + OVERFLOW_PAGE_HEAD.0, head as u32),
                None => page.write_val_at(offset + OVERFLOW_PAGE_HEAD.0, 0 as u32),
            }
            let size = tail - offset;
            page.write_val_at(offset + CELL_SIZE.0, size as u32);
            page.write_buf_at(offset + PAYLOAD_START, not_overflowed_payload);
        }
        Cell::table_leaf_at(page, offset)
    }

    pub fn child(&self) -> NodePointer {
        match self {
            Self::TableInterior(p, off) => unsafe { p.read_val_at::<NodePointer>(*off + CHILD.0) },
            _ => unreachable!("Only interior Node have children"),
        }
    }

    pub fn set_child(&self, child: NodePointer) {
        match self {
            Self::TableInterior(p, off) => unsafe {
                p.write_val_at::<NodePointer>(*off + CHILD.0, child)
            },
            _ => todo!(),
        }
    }

    pub fn cell_size(&self) -> u32 {
        match self {
            Self::TableLeaf(p, off) => unsafe { p.read_val_at::<u32>(*off + CELL_SIZE.0) },
            Self::TableInterior(_, _) => self.header_size() as u32,
            _ => todo!(),
        }
    }

    pub fn key(&self) -> u32 {
        match self {
            Self::TableLeaf(p, off) => unsafe { p.read_val_at(*off + KEY.0) },
            Self::TableInterior(p, off) => unsafe { p.read_val_at(*off + KEY.0) },
            _ => todo!(),
        }
    }

    pub const fn header_size(&self) -> usize {
        match self {
            Self::TableLeaf(_, _) => KEY.1 + PAYLOAD_SIZE.1 + OVERFLOW_PAGE_HEAD.1 + CELL_SIZE.1,
            Self::TableInterior(_, _) => KEY.1 + CHILD.1,
            _ => todo!(),
        }
    }

    pub fn payload_size(&self) -> PayloadSize {
        match self {
            Self::TableLeaf(page, offset) => unsafe { page.read_val_at(*offset + PAYLOAD_SIZE.0) },
            _ => todo!(),
        }
    }

    pub fn kept_payload(&self) -> &[u8] {
        match self {
            Self::TableLeaf(page, offset) => page.read_buf_at(
                *offset + PAYLOAD_START,
                self.cell_size() as usize - self.header_size(),
            ),
            _ => todo!(),
        }
    }

    pub fn overflow_page_head(&self) -> Option<NodePointer> {
        match self {
            Self::TableLeaf(page, offset) => {
                let head = unsafe { page.read_val_at(*offset + OVERFLOW_PAGE_HEAD.0) };
                if head > 0 {
                    Some(head)
                } else {
                    None
                }
            }
            _ => todo!(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::Cell;

    lazy_static! {
        static ref PAGER: Arc<Mutex<Pager>> = Arc::new(Mutex::new(Pager::init("celltest")));
    }

    use std::sync::{Arc, Mutex};
    use lazy_static::lazy_static;
    use ltp_rust_db_page::{page::PAGE_SIZE, pager::Pager};

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
        let cell2 = Cell::table_leaf_at(&page, PAGE_SIZE - cell.cell_size() as usize);
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
        let cell2 = Cell::table_interior_at(&page, PAGE_SIZE - cell.cell_size() as usize);
        println!("{:#?}", cell2);
        assert_eq!(cell2.key(), 12);
        assert_eq!(cell2.child(), 3);
    }
}
