use std::mem::size_of;

use super::PayloadReadResult;

/// (<offset>, <size>)
const RIGHT_CHILD_PTR: (usize, usize) = (0, size_of::<u32>());
/// (<offset>, <size>)
const KEY_SIZE: (usize, usize) = (RIGHT_CHILD_PTR.1, size_of::<u32>());
/// (<offset>, <size>)
const PAYLOAD_START: usize = KEY_SIZE.0 + KEY_SIZE.1;

pub use reader::LeafCellReader;
pub use writer::LeafCellWriter;

const fn static_header_size() -> usize {
    KEY_SIZE.1 + RIGHT_CHILD_PTR.1
}

mod reader {
    use std::mem::size_of;

    use crate::btree_index::btree::node::cell::PayloadReadResult;

    use super::{KEY_SIZE, PAYLOAD_START, RIGHT_CHILD_PTR};

    pub struct LeafCellReader<'a>(&'a [u8]);

    impl<'a> LeafCellReader<'a> {
        pub unsafe fn new(allocated_buffer: &'a [u8]) -> Self {
            Self(allocated_buffer)
        }

        pub fn key_size(&self) -> usize {
            unsafe { *(self.0.as_ptr().add(KEY_SIZE.0) as *const u32) as usize }
        }

        pub fn cell_size(&self) -> usize {
            self.0.len()
        }

        pub fn right_child_ptr(&self) -> u32 {
            let ptr = &self.0[RIGHT_CHILD_PTR.0..RIGHT_CHILD_PTR.0 + RIGHT_CHILD_PTR.1];
            u32::from_be_bytes([ptr[0], ptr[1], ptr[2], ptr[3]])
        }

        pub fn have_overflow(&self) -> bool {
            self.key_size() + super::static_header_size() > self.cell_size()
        }

        pub fn key(&self) -> PayloadReadResult {
            if !self.have_overflow() {
                return PayloadReadResult::InPage {
                    payload: &self.0[PAYLOAD_START..],
                };
            }
            let key_size = self.key_size();
            let cell_size = self.cell_size();
            let payload = &self.0[PAYLOAD_START..cell_size - size_of::<u32>()];
            let overflow_page_head = &self.0[cell_size - size_of::<u32>()..cell_size];
            let overflow_page_head = u32::from_be_bytes([
                overflow_page_head[0],
                overflow_page_head[1],
                overflow_page_head[2],
                overflow_page_head[3],
            ]);
            PayloadReadResult::InOverflow {
                payload_len: key_size,
                partial_payload: payload,
                overflow_page_head,
            }
        }

        pub fn overflow_page_head(&self) -> Option<u32> {
            if !self.have_overflow() {
                return None;
            }
            let cell_size = self.cell_size();
            let head = &self.0[cell_size - size_of::<u32>()..];
            let head = u32::from_be_bytes([head[0], head[1], head[2], head[3]]);
            Some(head)
        }
    }
}
mod writer {
    use std::mem::size_of;

    use crate::btree_index::btree::node::cell::PayloadWriteResult;

    use super::{LeafCellReader, KEY_SIZE, PAYLOAD_START, RIGHT_CHILD_PTR};

    pub struct LeafCellWriter<'a>(&'a mut [u8]);

    impl<'a> LeafCellWriter<'a> {
        pub unsafe fn new(allocated_buffer: &'a mut [u8]) -> Self {
            Self(allocated_buffer)
        }

        pub fn set_key_size(&mut self, key_size: usize) {
            unsafe {
                *(self.0.as_mut_ptr().add(KEY_SIZE.0) as *mut u32) = key_size as u32;
            }
        }

        pub fn cell_size(&self) -> usize {
            self.0.len()
        }

        pub fn set_overflow_page_head(&mut self, overflow_page_head: Option<u32>) {
            if unsafe { !LeafCellReader::new(self.0).have_overflow() }
                && !overflow_page_head.is_none()
            {
                panic!("overflow page head must be set if cell has overflow");
            }
            let cell_size = self.cell_size();
            if let Some(head) = overflow_page_head {
                let head = head.to_be_bytes();
                self.0[cell_size - 4..cell_size].copy_from_slice(&head);
            }
        }

        pub fn set_right_child_ptr(&mut self, child_ptr: u32) {
            let ptr = child_ptr.to_be_bytes();
            self.0[RIGHT_CHILD_PTR.0..RIGHT_CHILD_PTR.0 + RIGHT_CHILD_PTR.1].copy_from_slice(&ptr);
        }

        pub fn write_key(&mut self, key: &'a [u8]) -> PayloadWriteResult<'a> {
            let key_size = key.len();
            let cell_size = self.cell_size();
            self.set_key_size(key_size);
            if key_size + super::static_header_size() > cell_size {
                // key is too large to fit in this cell
                // last 4 bytes are reserved for overflow page head
                let split_point = cell_size - size_of::<u32>() - super::static_header_size();
                self.0[PAYLOAD_START..cell_size - size_of::<u32>()]
                    .copy_from_slice(&key[..split_point]);
                return PayloadWriteResult::InOverflow {
                    remain_payload: &key[split_point..],
                };
            }
            self.0[PAYLOAD_START..].copy_from_slice(key);
            PayloadWriteResult::InPage
        }
    }
}

#[cfg(test)]
mod tests {
    use super::LeafCellReader;
    use super::LeafCellWriter;
    use crate::btree_index::btree::node::cell::leaf_cell::static_header_size;
    use crate::btree_index::btree::node::cell::PayloadReadResult;
    use crate::btree_index::btree::node::cell::PayloadWriteResult;

    #[test]
    fn simple_leaf_cell() {
        let mut buffer = [0; 64];
        let mut writer = unsafe { LeafCellWriter::new(&mut buffer) };
        let rs = writer.write_key(&[0xe; 64 - static_header_size()]);
        assert_eq!(rs, PayloadWriteResult::InPage);
        writer.set_overflow_page_head(None);

        let cell = unsafe { LeafCellReader::new(&buffer) };
        assert_eq!(cell.key_size(), 64 - static_header_size());
        assert_eq!(cell.cell_size(), 64);
        assert_eq!(cell.have_overflow(), false);
        assert_eq!(
            cell.key(),
            PayloadReadResult::InPage {
                payload: &[0xe; 64 - static_header_size()],
            }
        );
        assert_eq!(cell.overflow_page_head(), None);
    }

    #[test]
    fn leaf_cell_with_overflow() {
        let mut buffer = [0; 64];
        let mut writer = unsafe { LeafCellWriter::new(&mut buffer) };
        let rs = writer.write_key(&[0xe; 100]);
        assert_eq!(
            rs,
            PayloadWriteResult::InOverflow {
                remain_payload: &[0xe; 100 - (64 - static_header_size() - 4)],
            }
        );
        writer.set_overflow_page_head(Some(0x12345678));

        let cell = unsafe { LeafCellReader::new(&buffer) };
        assert_eq!(cell.key_size(), 100);
        assert_eq!(cell.cell_size(), 64);
        assert_eq!(cell.have_overflow(), true);
        assert_eq!(
            cell.key(),
            PayloadReadResult::InOverflow {
                payload_len: 100,
                partial_payload: &[0xe; 64 - static_header_size() - 4],
                overflow_page_head: 0x12345678,
            }
        );
        assert_eq!(cell.overflow_page_head(), Some(0x12345678));
    }
}
