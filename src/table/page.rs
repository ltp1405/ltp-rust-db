use super::PAGE_SIZE;

#[derive(Debug, PartialEq, Eq)]
pub struct Page {
    pub buffer: Box<[u8; PAGE_SIZE]>,
}

impl Page {
    pub fn init() -> Self {
        Self {
            buffer: Box::new([0; PAGE_SIZE]),
        }
    }
}
