mod cell;

pub use cell::Cell;
pub use cell::CellMut;

#[derive(Debug, PartialEq)]
pub enum PayloadReadResult<'a> {
    InPage {
        payload: &'a [u8],
    },
    InOverflow {
        payload_len: usize,
        partial_payload: &'a [u8],
        overflow_page_head: u32,
    },
}

#[derive(Debug, PartialEq)]
pub enum PayloadWriteResult<'a> {
    InPage,
    InOverflow { remain_payload: &'a [u8] },
}
