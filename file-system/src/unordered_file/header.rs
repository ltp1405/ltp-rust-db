use std::mem::size_of;

#[derive(Debug)]
pub struct FileHeader {
    pub cell_count: u64,
    pub head_page_num: u32,
    pub tail_page_num: u32,
}

impl FileHeader {
    pub const fn size() -> usize {
        size_of::<u64>() + size_of::<u32>() + size_of::<u32>()
    }

    pub fn read_from(buffer: &[u8]) -> Self {
        let mut offset = 0;
        let cell_count = u64::from_be_bytes(
            buffer[offset..offset + size_of::<u64>()]
                .try_into()
                .unwrap(),
        );
        offset += size_of::<u64>();
        let head_page_num = u32::from_be_bytes(
            buffer[offset..offset + size_of::<u32>()]
                .try_into()
                .unwrap(),
        );
        offset += size_of::<u32>();
        let tail_page_num = u32::from_be_bytes(
            buffer[offset..offset + size_of::<u32>()]
                .try_into()
                .unwrap(),
        );
        Self {
            cell_count,
            head_page_num,
            tail_page_num,
        }
    }

    pub fn write_to(&self, buffer: &mut [u8]) {
        let mut offset = 0;
        buffer[offset..offset + size_of::<u64>()].copy_from_slice(&self.cell_count.to_be_bytes());
        offset += size_of::<u64>();
        buffer[offset..offset + size_of::<u32>()]
            .copy_from_slice(&self.head_page_num.to_be_bytes());
        offset += size_of::<u32>();
        buffer[offset..offset + size_of::<u32>()]
            .copy_from_slice(&self.tail_page_num.to_be_bytes());
    }
}

#[derive(Debug)]
pub struct FilePageHeader {
    pub free_space_start: u32,
    /// Use to swizzle the address 
    pub address_type: u8,
    pub next: u32,
}

impl FilePageHeader {
    pub fn new(next: u32, free_space_start: u32) -> Self {
        Self {
            free_space_start,
            next,
        }
    }

    pub const fn size() -> usize {
        size_of::<u32>() * 2
    }

    pub fn read_from(is_head: bool, buffer: &[u8]) -> Self {
        let mut offset = if is_head { FileHeader::size() } else { 0 };
        let free_space_start = u32::from_be_bytes(
            buffer[offset..offset + size_of::<u32>()]
                .try_into()
                .unwrap(),
        );
        offset += size_of::<u32>();
        let next = u32::from_be_bytes(
            buffer[offset..offset + size_of::<u32>()]
                .try_into()
                .unwrap(),
        );
        Self {
            free_space_start,
            next,
        }
    }

    pub fn write_to(&self, is_head: bool, page: &mut [u8]) {
        let mut offset = if is_head { FileHeader::size() } else { 0 };
        page[offset..offset + size_of::<u32>()]
            .copy_from_slice(&self.free_space_start.to_be_bytes());
        offset += size_of::<u32>();
        page[offset..offset + size_of::<u32>()].copy_from_slice(&self.next.to_be_bytes());
    }
}
