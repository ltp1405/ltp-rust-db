type KeySize = u16;

mod leaf_header {
    use std::mem::size_of;

    pub const fn static_header_size() -> usize {
        KEY_SIZE.1 + PAGE_NUMBER.1 + RECORD_OFFSET.1
    }

    /// (<offset>, <size>)
    pub const KEY_SIZE: (usize, usize) = (0, size_of::<u32>());
    /// (<offset>, <size>)
    pub const PAGE_NUMBER: (usize, usize) = (KEY_SIZE.0 + KEY_SIZE.1, size_of::<u32>());
    /// (<offset>, <size>)
    pub const RECORD_OFFSET: (usize, usize) = (PAGE_NUMBER.0 + PAGE_NUMBER.1, size_of::<u32>());
    /// (<offset>, <size>)
    pub const PAYLOAD_START: usize = KEY_SIZE.0 + KEY_SIZE.1;
}

mod interior_header {
    use std::mem::size_of;
    pub const fn static_header_size() -> usize {
        KEY_SIZE.1 + LEFT_CHILD_PTR.1
    }

    /// Leaf Node Header Layout
    /// (<offset>, <size>)
    pub const LEFT_CHILD_PTR: (usize, usize) = (0, size_of::<u32>());
    /// (<offset>, <size>)
    pub const KEY_SIZE: (usize, usize) = (LEFT_CHILD_PTR.1, size_of::<u32>());
    /// (<offset>, <size>)
    pub const PAYLOAD_START: usize = KEY_SIZE.0 + KEY_SIZE.1;
}

pub use cell::Cell;
pub use cell_mut::CellMut;
mod cell {
    use std::{fmt::Debug, mem::size_of};

    use crate::btree_index::btree::{node::cell::PayloadReadResult, RowAddress};

    use super::{interior_header, leaf_header};

    pub enum Cell<'a> {
        Leaf(&'a [u8]),
        Interior(&'a [u8]),
    }

    impl<'a> Debug for Cell<'a> {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            match self {
                Self::Leaf(b) => {
                    let key_size = self.key_size();
                    let cell_size = self.cell_size();
                    write!(f, "Leaf: key_size: {}, cell_size: {}", key_size, cell_size)
                }
                Self::Interior(b) => {
                    let key_size = self.key_size();
                    let cell_size = self.cell_size();
                    let left_child_ptr = self.child_pointer();
                    write!(
                        f,
                        "Interior: key_size: {}, cell_size: {}, left_child_ptr: {}",
                        key_size, cell_size, left_child_ptr
                    )
                }
            }
        }
    }

    impl<'a> Cell<'a> {
        pub unsafe fn leaf(allocated_buffer: &'a [u8]) -> Self {
            Self::Leaf(allocated_buffer)
        }

        pub const fn leaf_header_size() -> usize {
            leaf_header::static_header_size()
        }

        pub const fn interior_header_size() -> usize {
            interior_header::static_header_size()
        }

        pub unsafe fn interior(allocated_buffer: &'a [u8]) -> Self {
            Self::Interior(allocated_buffer)
        }

        pub fn child_pointer(&self) -> u32 {
            match self {
                Self::Leaf(_) => panic!("Leaf node does not have child pointer"),
                Self::Interior(b) => unsafe {
                    *(b.as_ptr() as *const u32).add(interior_header::LEFT_CHILD_PTR.0)
                },
            }
        }

        pub fn key_size(&self) -> usize {
            match self {
                Self::Leaf(b) => unsafe { *(b.as_ptr() as *const u32) as usize },
                Self::Interior(b) => unsafe {
                    *(b.as_ptr().add(interior_header::KEY_SIZE.0) as *const u32) as usize
                },
            }
        }

        pub fn row_address(&self) -> RowAddress {
            match self {
                Self::Leaf(b) => {
                    let page_number =
                        unsafe { *(b.as_ptr().add(leaf_header::PAGE_NUMBER.0) as *const [u8; 4]) };
                    let record_offset = unsafe {
                        *(b.as_ptr().add(leaf_header::RECORD_OFFSET.0) as *const [u8; 4])
                    };
                    RowAddress {
                        page_number: u32::from_le_bytes(page_number),
                        offset: u32::from_le_bytes(record_offset),
                    }
                }
                Self::Interior(_) => panic!("Interior node does not have row address"),
            }
        }

        pub fn cell_size(&self) -> usize {
            match self {
                Self::Leaf(b) => b.len(),
                Self::Interior(b) => b.len(),
            }
        }

        pub fn header_size(&self) -> usize {
            match self {
                Self::Leaf(_) => leaf_header::static_header_size(),
                Self::Interior(_) => interior_header::static_header_size(),
            }
        }

        pub fn have_overflow(&self) -> bool {
            self.key_size() + self.header_size() > self.cell_size()
        }

        pub fn key(&self) -> PayloadReadResult {
            if !self.have_overflow() {
                return PayloadReadResult::InPage {
                    payload: match { self } {
                        Self::Leaf(b) => &b[leaf_header::PAYLOAD_START..],
                        Self::Interior(b) => &b[interior_header::PAYLOAD_START..],
                    },
                };
            }
            let key_size = self.key_size();
            let cell_size = self.cell_size();
            let payload = match self {
                Self::Leaf(b) => &b[leaf_header::PAYLOAD_START..cell_size - size_of::<u32>()],
                Self::Interior(b) => {
                    &b[interior_header::PAYLOAD_START..cell_size - size_of::<u32>()]
                }
            };
            let overflow_page_head = self.overflow_page_head().unwrap();
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
            let head = match self {
                Self::Leaf(b) => &b[cell_size - size_of::<u32>()..],
                Self::Interior(b) => &b[cell_size - size_of::<u32>()..],
            };
            let head = u32::from_be_bytes([head[0], head[1], head[2], head[3]]);
            Some(head)
        }
    }
}

mod cell_mut {
    use std::mem::size_of;

    use crate::btree_index::btree::node::cell::{PayloadReadResult, PayloadWriteResult};
    use crate::btree_index::btree::node::node_header::NodePointer;
    use crate::btree_index::btree::RowAddress;

    use super::interior_header;
    use super::leaf_header;
    use super::Cell;
    use super::KeySize;

    pub enum CellMut<'a> {
        Leaf(&'a mut [u8]),
        Interior(&'a mut [u8]),
    }

    impl<'a> CellMut<'a> {
        pub unsafe fn leaf(cell_buffer: &'a mut [u8]) -> Self {
            Self::Leaf(cell_buffer)
        }

        pub unsafe fn interior(cell_buffer: &'a mut [u8]) -> Self {
            Self::Interior(cell_buffer)
        }

        fn cell(&self) -> Cell {
            match self {
                Self::Leaf(b) => unsafe { Cell::leaf(b) },
                Self::Interior(b) => unsafe { Cell::interior(b) },
            }
        }

        pub fn key_size(&self) -> usize {
            self.cell().key_size()
        }

        pub fn set_right_child_pointer(&mut self, right_child_pointer: u32) {
            match self {
                Self::Leaf(_) => panic!("Leaf node does not have child pointer"),
                Self::Interior(b) => unsafe {},
            }
        }

        pub fn row_address(&self) -> RowAddress {
            self.cell().row_address()
        }

        pub fn set_row_address(&mut self, row_address: RowAddress) {
            match self {
                Self::Leaf(b) => {
                    b[leaf_header::PAGE_NUMBER.0
                        ..leaf_header::PAGE_NUMBER.0 + leaf_header::PAGE_NUMBER.1]
                        .copy_from_slice(&row_address.page_number.to_be_bytes());
                    b[leaf_header::RECORD_OFFSET.0
                        ..leaf_header::RECORD_OFFSET.0 + leaf_header::RECORD_OFFSET.1]
                        .copy_from_slice(&row_address.offset.to_be_bytes());
                }
                Self::Interior(b) => {
                    panic!("Interior node does not have row address")
                }
            }
        }

        pub fn cell_size(&self) -> usize {
            self.cell().cell_size()
        }

        pub fn header_size(&self) -> usize {
            self.cell().header_size()
        }

        pub fn have_overflow(&self) -> bool {
            self.cell().have_overflow()
        }

        pub fn key(&self) -> PayloadReadResult {
            if !self.have_overflow() {
                return PayloadReadResult::InPage {
                    payload: match { self } {
                        Self::Leaf(b) => &b[leaf_header::PAYLOAD_START..],
                        Self::Interior(b) => &b[interior_header::PAYLOAD_START..],
                    },
                };
            }
            let key_size = self.key_size();
            let cell_size = self.cell_size();
            let payload = match self {
                Self::Leaf(b) => &b[leaf_header::PAYLOAD_START..cell_size - size_of::<u32>()],
                Self::Interior(b) => {
                    &b[interior_header::PAYLOAD_START..cell_size - size_of::<u32>()]
                }
            };
            let overflow_page_head = self.overflow_page_head().unwrap();
            PayloadReadResult::InOverflow {
                payload_len: key_size,
                partial_payload: payload,
                overflow_page_head,
            }
        }

        pub fn overflow_page_head(&self) -> Option<NodePointer> {
            self.cell().overflow_page_head()
        }

        pub fn write_key(&mut self, key: &'a [u8]) -> PayloadWriteResult<'a> {
            let key_size = key.len();
            self.set_key_size(key_size as u16);
            if self.have_overflow() {
                // key is too large to fit in this cell
                let cell_size = self.cell_size();
                // last 4 bytes are reserved for overflow page head
                let split_point = cell_size - size_of::<u32>() - self.header_size();
                match self {
                    Self::Leaf(b) => {
                        b[leaf_header::PAYLOAD_START..cell_size - size_of::<u32>()]
                            .copy_from_slice(&key[..split_point]);
                    }
                    Self::Interior(b) => {
                        b[interior_header::PAYLOAD_START..cell_size - size_of::<u32>()]
                            .copy_from_slice(&key[..split_point]);
                    }
                }
                return PayloadWriteResult::InOverflow {
                    remain_payload: &key[split_point..],
                };
            }
            match self {
                Self::Leaf(b) => {
                    b[leaf_header::PAYLOAD_START..].copy_from_slice(key);
                }
                Self::Interior(b) => {
                    b[interior_header::PAYLOAD_START..].copy_from_slice(key);
                }
            }
            PayloadWriteResult::InPage
        }

        fn set_key_size(&mut self, key_size: KeySize) {
            match self {
                Self::Leaf(b) => b
                    [leaf_header::KEY_SIZE.0..leaf_header::KEY_SIZE.0 + leaf_header::KEY_SIZE.1]
                    .copy_from_slice(&key_size.to_be_bytes()),
                Self::Interior(b) => b[interior_header::KEY_SIZE.0
                    ..interior_header::KEY_SIZE.0 + interior_header::KEY_SIZE.1]
                    .copy_from_slice(&key_size.to_be_bytes()),
            }
        }

        pub fn set_overflow_page_head(&mut self, overflow_page_head: Option<u32>) {
            if !self.have_overflow() && !overflow_page_head.is_none() {
                panic!("overflow page head can only be set on a cell without overflow");
            }
            let cell_size = self.cell_size();
            if let Some(head) = overflow_page_head {
                let head = head.to_be_bytes();
                let buf = match { self } {
                    Self::Leaf(b) => &mut b[cell_size - size_of::<NodePointer>()..],
                    Self::Interior(b) => &mut b[cell_size - size_of::<NodePointer>()..],
                };
                buf.copy_from_slice(&head);
            } else {
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::btree_index::btree::node::cell::cell::interior_header;
    use crate::btree_index::btree::node::cell::Cell;
    use crate::btree_index::btree::node::cell::CellMut;
    use crate::btree_index::btree::node::cell::PayloadReadResult;
    use crate::btree_index::btree::node::cell::PayloadWriteResult;

    #[test]
    fn simple_leaf_cell() {
        let mut buffer = [0; 64];
        let mut cell = unsafe { CellMut::leaf(&mut buffer) };
        let rs = cell.write_key(&[0xe; 64 - interior_header::static_header_size()]);
        assert_eq!(rs, PayloadWriteResult::InPage);
        cell.set_overflow_page_head(None);

        let cell = unsafe { Cell::leaf(&buffer) };
        assert_eq!(cell.key_size(), 64 - interior_header::static_header_size());
        assert_eq!(cell.cell_size(), 64);
        assert_eq!(cell.have_overflow(), false);
        assert_eq!(
            cell.key(),
            PayloadReadResult::InPage {
                payload: &[0xe; 64 - interior_header::static_header_size()],
            }
        );
        assert_eq!(cell.overflow_page_head(), None);
    }

    #[test]
    fn leaf_cell_with_overflow() {
        let mut buffer = [0; 64];
        let mut writer = unsafe { CellMut::leaf(&mut buffer) };
        let rs = writer.write_key(&[0xe; 100]);
        assert_eq!(
            rs,
            PayloadWriteResult::InOverflow {
                remain_payload: &[0xe; 100 - (64 - interior_header::static_header_size() - 4)],
            }
        );
        writer.set_overflow_page_head(Some(0x12345678));
        assert_eq!(writer.overflow_page_head(), Some(0x12345678));
        assert_eq!(writer.key_size(), 100);
        assert_eq!(writer.have_overflow(), true);

        let cell = unsafe { Cell::leaf(&buffer) };
        assert_eq!(cell.key_size(), 100);
        assert_eq!(cell.cell_size(), 64);
        assert_eq!(cell.have_overflow(), true);
        assert_eq!(
            cell.key(),
            PayloadReadResult::InOverflow {
                payload_len: 100,
                partial_payload: &[0xe; 64 - interior_header::static_header_size() - 4],
                overflow_page_head: 0x12345678,
            }
        );
        assert_eq!(cell.overflow_page_head(), Some(0x12345678));
    }

    #[test]
    fn simple_interior_cell() {
        let mut buffer = [0; 64];
        let mut writer = unsafe { CellMut::interior(&mut buffer) };
        let rs = writer.write_key(&[0xe; 64 - 4]);
        assert_eq!(rs, PayloadWriteResult::InPage);
        writer.set_overflow_page_head(None);

        let cell = unsafe { Cell::interior(&buffer) };
        assert_eq!(cell.key_size(), 64 - 4);
        assert_eq!(cell.cell_size(), 64);
        assert_eq!(cell.have_overflow(), false);
        assert_eq!(
            cell.key(),
            PayloadReadResult::InPage {
                payload: &[0xe; 64 - 4],
            }
        );
        assert_eq!(cell.overflow_page_head(), None);
    }

    #[test]
    fn simple_interior_cell_with_overflow() {
        let mut buffer = [0; 64];
        let mut writer = unsafe { CellMut::interior(&mut buffer) };
        let rs = writer.write_key(&[0xe; 100]);
        assert_eq!(
            rs,
            PayloadWriteResult::InOverflow {
                remain_payload: &[0xe; 100 - (64 - 4 - 4)],
            }
        );
        writer.set_overflow_page_head(Some(0x12345678));

        let cell = unsafe { Cell::interior(&buffer) };
        assert_eq!(cell.key_size(), 100);
        assert_eq!(cell.cell_size(), 64);
        assert_eq!(cell.have_overflow(), true);
        assert_eq!(
            cell.key(),
            PayloadReadResult::InOverflow {
                payload_len: 100,
                partial_payload: &[0xe; 64 - 4 - 4],
                overflow_page_head: 0x12345678,
            }
        );
        assert_eq!(cell.overflow_page_head(), Some(0x12345678));
    }
}
