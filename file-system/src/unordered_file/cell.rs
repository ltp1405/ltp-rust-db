use std::mem::size_of;

#[derive(Debug, Clone, PartialEq)]
pub struct Cell {
    pub buf: Vec<u8>,
}

impl Cell {
    pub fn to_bytes(self) -> Vec<u8> {
        self.buf
    }

    pub fn from_bytes(bytes: Vec<u8>) -> Self {
        Self { buf: bytes }
    }

    pub fn new(buf: Vec<u8>) -> Self {
        Self { buf }
    }

    /// Include header
    pub fn size(&self) -> usize {
        self.buf.len() + size_of::<u32>()
    }

    pub fn serialize(&self) -> &[u8] {
        &self.buf[..]
    }

    pub fn deserialize(buf: &[u8]) -> Self {
        Self { buf: buf.to_vec() }
    }
}
