use std::ops::{Deref, DerefMut};

use super::PAGE_SIZE;

#[derive(Debug, PartialEq, Eq)]
pub struct Page {
    buffer: Box<[u8; PAGE_SIZE]>,
}

impl Deref for Page {
    type Target = [u8; PAGE_SIZE];
    fn deref(&self) -> &Self::Target {
        &self.buffer
    }
}

impl DerefMut for Page {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.buffer
    }
}

impl Page {
    pub fn init() -> Self {
        Self {
            buffer: Box::new([0; PAGE_SIZE]),
        }
    }
}
