use std::mem::size_of;

use super::PayloadReadResult;

const RIGHT_CHILD_PTR: (usize, usize) = (size_of::<u32>(), size_of::<u32>());
/// (<offset>, <size>)
const KEY_SIZE: (usize, usize) = (RIGHT_CHILD_PTR.1, size_of::<u32>());
/// (<offset>, <size>)
const PAYLOAD_START: usize = KEY_SIZE.0 + KEY_SIZE.1;

pub struct LeafCellReader<'a>(&'a [u8]);

impl<'a> LeafCellReader<'a> {
    pub unsafe fn new(allocated_buffer: &'a [u8]) -> Self {
        Self(allocated_buffer)
    }

    pub fn key_size(&self) -> usize {
        u32::from_be_bytes([
            self.0[KEY_SIZE.0],
            self.0[KEY_SIZE.0 + 1],
            self.0[KEY_SIZE.0 + 2],
            self.0[KEY_SIZE.0 + 3],
        ]) as usize
    }

    pub fn cell_size(&self) -> usize {
        self.0.len()
    }

    pub fn static_header_size() -> usize {
        KEY_SIZE.1 + RIGHT_CHILD_PTR.1
    }

    pub fn right_child_ptr(&self) -> u32 {
        let ptr = &self.0[RIGHT_CHILD_PTR.0..RIGHT_CHILD_PTR.0 + RIGHT_CHILD_PTR.1];
        u32::from_be_bytes([ptr[0], ptr[1], ptr[2], ptr[3]])
    }

    pub fn have_overflow(&self) -> bool {
        self.key_size() + Self::static_header_size() > self.cell_size()
    }

    pub fn key(&self) -> PayloadReadResult {
        if !self.have_overflow() {
            return PayloadReadResult::InPage {
                payload: &self.0[PAYLOAD_START..],
            };
        }
        let key_size = self.key_size();
        let cell_size = self.cell_size();
        let payload = &self.0[PAYLOAD_START..cell_size - Self::static_header_size()];
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
        let head = &self.0[cell_size - Self::static_header_size()..];
        let head = u32::from_be_bytes([head[0], head[1], head[2], head[3]]);
        Some(head)
    }
}

mod write {
    use std::mem::size_of;

    use super::{PayloadWriteResult, KEY_SIZE, PAYLOAD_START, RIGHT_CHILD_PTR};

    pub struct LeafCellWriter<'a>(&'a mut [u8]);

    impl<'a> LeafCellWriter<'a> {
        pub unsafe fn new(allocated_buffer: &'a mut [u8]) -> Self {
            Self(allocated_buffer)
        }

        fn set_key_size(&mut self, key_size: usize) {
            unsafe {
                *(self.0.as_mut_ptr().add(KEY_SIZE.0) as *mut u32) = key_size as u32;
            }
        }

        pub fn cell_size(&self) -> usize {
            self.0.len()
        }

        pub fn set_overflow_page_head(&mut self, overflow_page_head: Option<u32>) {
            let cell_size = self.cell_size();
            if let Some(head) = overflow_page_head {
                let head = head.to_be_bytes();
                self.0[cell_size - 4..cell_size].copy_from_slice(&head);
            } else {
                self.set_key_size(cell_size - 4);
            }
        }

        pub fn static_header_size() -> usize {
            KEY_SIZE.1 + RIGHT_CHILD_PTR.1
        }

        pub fn set_right_child_ptr(&self, child_ptr: u32) {
            let ptr = &self.0[RIGHT_CHILD_PTR.0..RIGHT_CHILD_PTR.0 + RIGHT_CHILD_PTR.1];
            u32::from_be_bytes([ptr[0], ptr[1], ptr[2], ptr[3]])
        }

        pub fn have_overflow(&self) -> bool {
            self.key_size() + Self::static_header_size() > self.cell_size()
        }

        pub fn key(&self) -> PayloadWriteResult {
            if !self.have_overflow() {
                return PayloadWriteResult::InPage;
            }
            let key_size = self.key_size();
            let cell_size = self.cell_size();
            let payload = &self.0[PAYLOAD_START..cell_size - Self::static_header_size()];
            let overflow_page_head = &self.0[cell_size - size_of::<u32>()..cell_size];
            let overflow_page_head = u32::from_be_bytes([
                overflow_page_head[0],
                overflow_page_head[1],
                overflow_page_head[2],
                overflow_page_head[3],
            ]);
            PayloadWriteResult::InOverflow {
                remain_payload: payload,
            }
        }

        pub fn overflow_page_head(&self) -> Option<u32> {
            if !self.have_overflow() {
                return None;
            }
            let cell_size = self.cell_size();
            let head = &self.0[cell_size - Self::static_header_size()..];
            let head = u32::from_be_bytes([head[0], head[1], head[2], head[3]]);
            Some(head)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::LeafCellReader;
    use super::PayloadReadResult;

    #[test]
    fn test_leaf_cell_reader() {
        let cell = [
            0, 0, 0, 0, // right child ptr
            0, 0, 0, 0, // key size
            0, 0, 0, 0, // overflow page head
            1, 2, 3, 4, // payload
        ];
        let reader = unsafe { LeafCellReader::new(&cell) };
        assert_eq!(reader.right_child_ptr(), 0);
        assert_eq!(reader.key_size(), 0);
        assert_eq!(reader.overflow_page_head(), None);
        assert_eq!(
            reader.key(),
            PayloadReadResult::InPage {
                payload: &[1, 2, 3, 4]
            }
        );

        let cell = [
            0, 0, 0, 0, // right child ptr
            0, 0, 0, 4, // key size
            0, 0, 0, 0, // overflow page head
            1, 2, 3, 4, // payload
        ];
        let reader = unsafe { LeafCellReader::new(&cell) };
        assert_eq!(reader.right_child_ptr(), 0);
        assert_eq!(reader.key_size(), 4);
        assert_eq!(reader.overflow_page_head(), None);
        assert_eq!(
            reader.key(),
            PayloadReadResult::InPage {
                payload: &[1, 2, 3, 4]
            }
        );

        let cell = [
            0, 0, 0, 0, // right child ptr
            0, 0, 0, 4, // key size
            0, 0, 0, 0, // overflow page head
            1, 2, 3, 4, // payload
            5, 6, 7, 8, // overflow page head
        ];
        let reader = unsafe { LeafCellReader::new(&cell) };
        assert_eq!(reader.right_child_ptr(), 0);
        assert_eq!(reader.key_size(), 4);
        assert_eq!(reader.overflow_page_head(), Some(0x05060708));
    }
}
