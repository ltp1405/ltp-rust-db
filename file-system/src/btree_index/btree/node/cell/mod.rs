// mod cell;
mod interior_cell;
mod leaf_cell;

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

// pub use cell::Cell;
