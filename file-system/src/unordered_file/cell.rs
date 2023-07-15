use std::{mem::size_of, ops::Deref};

#[derive(Debug, Clone, PartialEq)]
pub struct Cell {
    delete_flag: bool,
    payload: Vec<u8>,
}

impl Deref for Cell {
    type Target = [u8];

    fn deref(&self) -> &Self::Target {
        &self.payload
    }
}

impl Cell {
    pub fn to_bytes(self) -> Vec<u8> {
        self.payload
    }

    pub fn from_bytes(bytes: Vec<u8>) -> Self {
        Self {
            payload: bytes,
            delete_flag: false,
        }
    }

    pub fn new(payload: Vec<u8>) -> Self {
        Self {
            payload,
            delete_flag: false,
        }
    }

    pub fn is_delete(&self) -> bool {
        self.delete_flag
    }

    pub fn set_delete(&mut self) {
        self.delete_flag = true;
    }

    /// Include header
    pub fn size(&self) -> usize {
        self.payload.len() + Self::header_size()
    }

    pub fn payload_len(&self) -> usize {
        self.payload.len()
    }

    pub fn header_size() -> usize {
        size_of::<u32>() + size_of::<u8>()
    }

    pub fn serialize(self) -> Vec<u8> {
        let mut buf = (self.size() as u32).to_be_bytes().to_vec();
        buf.push(self.delete_flag as u8);
        buf.extend_from_slice(&self.payload);
        buf
    }

    pub fn deserialize(buf: &[u8]) -> Self {
        Self {
            payload: buf.to_vec(),
            delete_flag: false,
        }
    }
}
