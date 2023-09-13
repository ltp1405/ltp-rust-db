use self::header::header_size;

use super::node::InsertResult;

mod header {
    use std::mem::size_of;

    /// (<offset>, <size>)
    pub const PAYLOAD_SIZE: (usize, usize) = (0, size_of::<u32>());
    /// (<offset>, <size>)
    pub const DELETE_FLAG: (usize, usize) = (PAYLOAD_SIZE.0 + PAYLOAD_SIZE.1, size_of::<bool>());

    pub const fn header_size() -> usize {
        PAYLOAD_SIZE.1 + DELETE_FLAG.1
    }
}

pub fn insert_cell<'a>(page: &mut [u8], at: usize, payload: &'a [u8]) -> InsertResult<'a> {
    let start = at as usize;
    let end = start + header_size() + payload.len();

    if start + header_size() > page.len() {
        // Not enough space to store the cell's header
        // This cell should be stored in a new page
        return InsertResult::OutOfSpace(payload);
    }

    page[start + header::PAYLOAD_SIZE.0..start + header::PAYLOAD_SIZE.0 + header::PAYLOAD_SIZE.1]
        .copy_from_slice(&(payload.len() as u32).to_be_bytes());
    if end < page.len() {
        // record can be inserted in a single page
        page[start + header_size()..end].copy_from_slice(payload);
        return InsertResult::Normal(end);
    } else {
        // record cannot be inserted in a single page and should be spilled
        let kept_size = page.len() - (start + header_size());
        let page_size = page.len();
        page[start + header_size()..page_size]
            .copy_from_slice(payload[..kept_size as usize].as_ref());
        return InsertResult::Spill(payload, kept_size);
    }
}

#[derive(Debug, Clone, PartialEq)]
pub enum PayloadReadResult<'a> {
    InPage {
        payload: &'a [u8],
    },
    InOverflow {
        initial_payload: &'a [u8],
        remain: usize,
    },
}

#[derive(Debug, Clone, PartialEq)]
pub struct Cell<'a>(&'a [u8]);

impl<'a> Cell<'a> {
    /// ### Safety: Slice should be at correct position so that
    /// header can be read correctly
    pub unsafe fn new(at: usize, page: &'a [u8]) -> Option<Self> {
        let start = at + header::PAYLOAD_SIZE.0;
        let payload_size = u32::from_be_bytes([
            page[start],
            page[start + 1],
            page[start + 2],
            page[start + 3],
        ]) as usize;
        if payload_size == 0 {
            return None;
        }
        if start + header_size() > page.len() {
            panic!()
        }
        let end = start + header_size() + payload_size;
        let end = if end > page.len() { page.len() } else { end };
        Some(Self(&page[start..end]))
    }

    pub fn is_delete(&self) -> bool {
        self.0[header::DELETE_FLAG.0] == 1
    }

    pub fn cell_size(&self) -> usize {
        self.0.len()
    }

    pub fn payload_size(&self) -> usize {
        let start = header::PAYLOAD_SIZE.0;
        u32::from_be_bytes([
            self.0[start],
            self.0[start + 1],
            self.0[start + 2],
            self.0[start + 3],
        ]) as usize
    }

    pub fn in_cell_payload_size(&self) -> usize {
        self.cell_size() - header_size()
    }

    pub fn header_size() -> usize {
        header::header_size()
    }

    pub fn payload(&self) -> PayloadReadResult<'a> {
        if self.0.len() < self.payload_size() + header_size() {
            return PayloadReadResult::InOverflow {
                initial_payload: &self.0[header_size()..],
                remain: self.payload_size() - (self.0.len() - header_size()),
            };
        }
        return PayloadReadResult::InPage {
            payload: &self.0[header_size()..],
        };
    }
}

#[derive(Debug, PartialEq)]
pub struct CellMut<'a>(&'a mut [u8]);

impl<'a> CellMut<'a> {
    /// ### Safety: Slice should be at correct position so that
    /// header can be read correctly
    pub unsafe fn new(at: usize, page: &'a mut [u8]) -> Self {
        let start = at + header::PAYLOAD_SIZE.0;
        let payload_size =
            u32::from_be_bytes([page[at], page[at + 1], page[at + 2], page[at + 3]]) as usize;
        let end = start + header_size() + payload_size;
        let end = if end > page.len() { page.len() } else { end };
        Self(&mut page[start..end])
    }

    pub fn is_delete(&self) -> bool {
        Cell(self.0).is_delete()
    }

    pub fn set_delete(&mut self, flag: bool) {
        let start = header::DELETE_FLAG.0;
        self.0[start] = flag as u8;
    }
}
