#[derive(Debug, Clone, PartialEq)]
pub struct Record {
    pub buf: Vec<u8>,
}

impl Record {
    pub fn new(buf: Vec<u8>) -> Self {
        Self { buf }
    }

    pub fn serialize(&self) -> &[u8] {
        &self.buf[..]
    }

    pub fn deserialize(buf: &[u8]) -> Self {
        Self { buf: buf.to_vec() }
    }
}
