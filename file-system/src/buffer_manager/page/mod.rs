use std::ptr::{slice_from_raw_parts, slice_from_raw_parts_mut};

pub use self::frame_allocator::FrameAllocator;
pub use self::page::Page;

mod frame_allocator;
mod page;

/// This table map the disk block (in the form of page) to the frame in physical memory
/// Each entry represent a map from page ---> frame
struct PageTableEntry {
    /// | page number: u32 | frame number: u32 | pin: u8 | dirty: u8 |
    entry: [u8; 10],
}

impl PageTableEntry {
    fn zero() -> Self {
        PageTableEntry { entry: [0; 10] }
    }

    fn get_pin(&self) -> u8 {
        self.entry[8]
    }

    fn is_empty(&self) -> bool {
        self.entry == [0; 10]
    }

    fn from_bytes(bytes: &[u8]) -> Self {
        let mut entry = [0; 10];
        entry.copy_from_slice(bytes);
        PageTableEntry { entry }
    }

    fn pin(&mut self) {
        self.entry[8] += 1;
    }

    fn unpin(&mut self) {
        if self.entry[8] == 0 {
            panic!("Page is not pinned");
        }
        self.entry[8] -= 1;
    }

    fn get_frame_number(&self) -> u32 {
        u32::from_be_bytes(self.entry[4..8].try_into().unwrap())
    }

    fn set_page_number(&mut self, page_number: u32) {
        self.entry[0..4].copy_from_slice(&page_number.to_be_bytes());
    }

    fn set_frame_number(&mut self, frame_number: u32) {
        self.entry[4..8].copy_from_slice(&frame_number.to_be_bytes());
    }
}

pub struct PageTable<'a> {
    table_buffer: &'a [u8],
}

impl<'a> PageTable<'a> {
    pub fn read(table_buffer: &'a [u8]) -> Self {
        Self { table_buffer }
    }

    fn get_entry(&self, page_number: u32) -> Option<PageTableEntry> {
        let entry_ptr = unsafe { self.table_buffer.as_ptr().add((page_number * 10) as usize) };
        let entry_slice = unsafe { slice_from_raw_parts(entry_ptr, 10).as_ref().unwrap() };
        let entry = PageTableEntry::from_bytes(entry_slice);
        if entry.is_empty() {
            None
        } else {
            Some(entry)
        }
    }

    pub fn get_frame_number(&self, page_number: u32) -> Option<u32> {
        let entry = self.get_entry(page_number)?;
        Some(entry.get_frame_number())
    }

    pub fn set_dirty(&mut self, page_number: u32) {
        let mut entry = self.get_entry(page_number).unwrap();
        entry.entry[9] = 1;
        self.write_entry(page_number, entry);
    }

    pub fn is_dirty(&self, page_number: u32) -> Option<bool> {
        let entry = self.get_entry(page_number)?;
        Some(entry.entry[9] == 1)
    }

    pub fn is_pinned(&self, page_number: u32) -> Option<bool> {
        let entry = self.get_entry(page_number)?;
        Some(entry.get_pin() > 0)
    }

    pub fn map_to_frame(&mut self, page_number: u32, frame_number: u32) {
        let mut entry = PageTableEntry::zero();
        entry.set_frame_number(frame_number);
        self.write_entry(page_number, entry);
    }

    fn write_entry(&mut self, page_number: u32, entry: PageTableEntry) {
        let entry_ptr = unsafe { self.table_buffer.as_ptr().add((page_number * 10) as usize) };
        let entry_slice = unsafe {
            slice_from_raw_parts_mut(entry_ptr as *mut u8, 10)
                .as_mut()
                .unwrap()
        };
        entry_slice.copy_from_slice(&entry.entry);
    }

    pub fn get_page<const PAGE_SIZE: usize>(
        &mut self,
        page_number: u32,
    ) -> Option<Page<'a, PAGE_SIZE>> {
        let mut entry = self.get_entry(page_number)?;
        let frame_number = entry.get_frame_number();
        entry.pin();
        self.write_entry(page_number, entry);
        let page = Some(Page::init(page_number, frame_number, self.table_buffer));
        page
    }

    fn drop_page(&mut self, page_number: u32) {
        println!("DROP {}", page_number);
        let mut entry = self.get_entry(page_number).unwrap();
        entry.unpin();
        self.write_entry(page_number, entry);
        let pin = self.get_entry(page_number).unwrap().get_pin();
        println!("PIN {}", pin);
    }
}

#[cfg(test)]
mod tests {
    use crate::buffer_manager::page::PageTableEntry;

    use super::{Page, PageTable};

    #[test]
    fn create_mapping() {
        let memory = [0; 4096];
        let mut table = PageTable::read(&memory);
        table.map_to_frame(12, 43);
        table.map_to_frame(4, 45);
        let entry = table.get_entry(12).unwrap();
        assert_eq!(entry.get_frame_number(), 43);
        let entry = table.get_entry(4).unwrap();
        assert_eq!(entry.get_frame_number(), 45);

        table.map_to_frame(12, 49);
        let entry = table.get_entry(12).unwrap();
        assert_eq!(entry.get_frame_number(), 49);
    }

    #[test]
    fn read_write_entry() {
        let memory = [0; 4096];
        let mut table = PageTable::read(&memory);
        let mut entry = PageTableEntry::zero();
        entry.set_frame_number(43);
        println!("{:?}", entry.entry);
        table.write_entry(12, entry);
        let entry = table.get_entry(12).unwrap();
        assert_eq!(entry.get_frame_number(), 43);
    }

    #[test]
    fn create_mapping_and_get_page() {
        let memory = [0; 512 * 16];
        let mut table = PageTable::read(&memory);
        table.map_to_frame(1, 3);
        println!("{:?}", table.get_entry(1).unwrap().get_frame_number());
        println!("{:?}", &memory[0..100]);
        let page: Page<512> = table.get_page(1).unwrap();
        println!("Pin {:?}", table.get_entry(1).unwrap().get_pin());
    }
}
