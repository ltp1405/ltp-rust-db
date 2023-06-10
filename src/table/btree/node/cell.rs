use std::{
    mem::size_of,
    ptr::{slice_from_raw_parts, slice_from_raw_parts_mut},
};

use crate::page::Page;

use super::{NodePointer, NodeType};

pub struct CellData((u32, Vec<u8>));

/// A key-value pair stored in a page
#[derive(Debug)]
pub enum Cell {
    TableLeaf {
        key: u32,
        payload_size: u32,
        not_overflowed_payload: Vec<u8>,
        overflow_page_head: Option<u32>,
    },
    TableInterior {
        left_child_addr: u32,
        key: u32,
    },
    IndexInterior,
    IndexLeaf,
}

type PayloadSize = u32;

/// (<offset>, <size>)
const LEFT_CHILD_PTR: (usize, usize) = (KEY.1, size_of::<NodePointer>());
/// (<offset>, <size>)
const KEY: (usize, usize) = (0, size_of::<u32>());
/// (<offset>, <size>)
const PAYLOAD_SIZE: (usize, usize) = (KEY.0 + KEY.1, size_of::<PayloadSize>());
/// (<offset>, <size>)
const OVERFLOW_PAGE_HEAD: (usize, usize) = (PAYLOAD_SIZE.0 + PAYLOAD_SIZE.1, size_of::<u32>());

const PAYLOAD_START: usize = OVERFLOW_PAGE_HEAD.0 + OVERFLOW_PAGE_HEAD.1;

impl Cell {
    pub unsafe fn table_leaf_at(page: &Page, offset: usize, overflow_amount: usize) -> Self {
        let page_ptr = page.as_ptr().add(offset);
        let key = (page_ptr as *const u32).read_unaligned();
        let payload_size_ptr = page_ptr.add(PAYLOAD_SIZE.0);
        let payload_size = (payload_size_ptr as *const PayloadSize).read_unaligned();
        let overflow_head_ptr = page_ptr.add(OVERFLOW_PAGE_HEAD.0);
        let overflow_page_head = Some((overflow_head_ptr as *const u32).read_unaligned());

        let payload_ptr = page_ptr.add(PAYLOAD_START);
        let payload = slice_from_raw_parts(
            payload_ptr,
            payload_size as usize - overflow_amount as usize,
        )
        .as_ref()
        .unwrap();
        Self::TableLeaf {
            key,
            payload_size,
            not_overflowed_payload: payload.to_vec(),
            overflow_page_head,
        }
    }

    pub fn new_table_leaf(
        key: u32,
        payload_size: u32,
        not_overflowed_payload: Vec<u8>,
        overflow_page_head: Option<u32>,
    ) -> Self {
        Self::TableLeaf {
            key,
            payload_size,
            not_overflowed_payload,
            overflow_page_head,
        }
    }

    pub fn new_table_interior(key: u32, left_child_addr: u32) -> Self {
        Self::TableInterior {
            left_child_addr,
            key,
        }
    }

    pub unsafe fn serialize_to(&self, slice: &mut [u8]) {
        match self {
            Self::TableLeaf {
                key,
                payload_size,
                not_overflowed_payload,
                overflow_page_head,
            } => {
                let ptr = slice.as_ptr();
                let key_ptr = ptr as *const u8;
                (key_ptr as *mut u32).write_unaligned(*key);

                let payload_size_ptr = ptr.add(PAYLOAD_SIZE.0);
                (payload_size_ptr as *mut PayloadSize).write_unaligned(*payload_size);

                let overflow_head_ptr = ptr.add(OVERFLOW_PAGE_HEAD.0);
                (overflow_head_ptr as *mut u32).write_unaligned(
                    if let Some(head) = overflow_page_head {
                        *head
                    } else {
                        0x0
                    },
                );
                let payload_ptr = ptr.add(PAYLOAD_START) as *mut u8;
                let payload_slice =
                    slice_from_raw_parts_mut(payload_ptr, not_overflowed_payload.len())
                        .as_mut()
                        .unwrap();
                payload_slice.copy_from_slice(not_overflowed_payload.as_slice());
            }
            _ => {}
        }
    }

    pub fn key(&self) -> u32 {
        match self {
            Self::TableLeaf { key, .. } => *key,
            _ => unimplemented!(),
        }
    }

    pub fn payload(&self) -> &[u8] {
        match self {
            Self::TableLeaf {
                not_overflowed_payload,
                ..
            } => not_overflowed_payload,
            _ => unimplemented!(),
        }
    }

    pub fn payload_size(&self) -> usize {
        match self {
            Self::TableLeaf { payload_size, .. } => *payload_size as usize,
            _ => unimplemented!(),
        }
    }

    pub fn left_child(&self) -> NodePointer {
        match self {
            Self::TableInterior {
                left_child_addr, ..
            } => *left_child_addr,
            _ => unimplemented!(),
        }
    }

    pub fn key_size(&self) -> u32 {
        KEY.1 as u32
    }

    pub fn size(&self) -> u32 {
        match self {
            Self::TableLeaf {
                key,
                payload_size,
                not_overflowed_payload,
                overflow_page_head,
            } => {
                (self.key_size() as usize
                    + size_of::<u32>()
                    + size_of::<u32>()
                    + not_overflowed_payload.len()) as u32
            }
            _ => unimplemented!(),
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::{
        page::Pager,
        table::btree::node::cell::{OVERFLOW_PAGE_HEAD, PAYLOAD_SIZE, PAYLOAD_START},
    };

    use super::Cell;

    lazy_static! {
        static ref PAGER: Arc<Mutex<Pager>> = Arc::new(Mutex::new(Pager::init("celltest")));
    }

    use std::sync::{Arc, Mutex};

    use lazy_static::lazy_static;

    #[test]
    fn simple_node() {
        let payload: Vec<u8> = vec![1, 2, 3];
        let mut pager = PAGER.lock().unwrap();
        let cell = Cell::new_table_leaf(12, payload.len() as u32, payload.clone(), None);
        let page = pager.get_page_mut(0).unwrap();
        let slice = &mut page[0 as usize..0 + cell.size() as usize];
        unsafe { cell.serialize_to(slice) }
        println!("{:?}", PAYLOAD_SIZE);
        println!("{:?}", OVERFLOW_PAGE_HEAD);
        println!("{}", PAYLOAD_START);
        println!("{:?}", slice);
        let cell2 = unsafe { Cell::table_leaf_at(page, 0, 0) };
        println!("{:?}", cell2);
        println!("{:?}", cell);
        assert_eq!(cell2.payload_size(), payload.len());
        assert_eq!(cell2.payload(), payload);
        assert_eq!(cell2.key(), 12);
    }
}
