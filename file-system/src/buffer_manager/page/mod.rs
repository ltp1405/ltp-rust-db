use std::mem::size_of;
use std::sync::{Arc, Mutex};
use std::time;

pub use self::frame_allocator::FrameAllocator;

mod frame_allocator;
mod page;

/// This table map the disk block (in the form of page) to the frame in physical memory
/// Each entry represent a map from page ---> frame
#[derive(Clone, Copy)]
struct PageTableEntry {
    /// | timestamp: f32 | frame number: u32 | pin: u8 | dirty: u8 |
    entry: [u8; 10],
}

impl PageTableEntry {
    fn size() -> usize {
        size_of::<PageTableEntry>()
    }

    fn zero() -> Self {
        PageTableEntry { entry: [0; 10] }
    }

    fn get_pin(&self) -> u8 {
        self.entry[8]
    }

    fn from_bytes(bytes: &[u8]) -> Self {
        let mut entry = [0; 10];
        entry.copy_from_slice(bytes);
        PageTableEntry { entry }
    }

    fn pin(&mut self) {
        self.entry[8] += 1;
    }

    fn set_timestamp(&mut self, timestamp: f32) {
        self.entry[0..4].copy_from_slice(&timestamp.to_be_bytes());
    }

    fn timestamp(&self) -> f32 {
        f32::from_be_bytes(self.entry[0..4].try_into().unwrap())
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

    fn set_frame_number(&mut self, frame_number: u32) {
        self.entry[4..8].copy_from_slice(&frame_number.to_be_bytes());
    }
}

pub struct PageTable<const BLOCKSIZE: usize, const CAPACITY: usize> {
    created_at: time::Instant,
    entries: Arc<Mutex<Vec<Option<PageTableEntry>>>>,
}

impl<const BLOCKSIZE: usize, const CAPACITY: usize> PageTable<BLOCKSIZE, CAPACITY> {
    pub fn init() -> Self {
        let mut entries = Vec::with_capacity(CAPACITY / BLOCKSIZE);
        for _ in 0..CAPACITY / BLOCKSIZE {
            entries.push(None);
        }
        Self {
            created_at: time::Instant::now(),
            entries: Arc::new(Mutex::new(entries)),
        }
    }

    pub fn pin_page(&self, page_number: u32) {
        let mut entries = self.entries.lock().unwrap();
        let mut entry = entries.get_mut(page_number as usize).unwrap().unwrap();
        drop(entries);
        entry.pin();
        self.write_entry(page_number, entry);
    }

    pub fn update_timestamp(&self, page_number: u32) {
        let mut entry = self.get_entry(page_number).unwrap();
        let duration = time::Instant::now().duration_since(self.created_at);
        entry.set_timestamp(duration.as_secs_f32());
        self.write_entry(page_number, entry);
    }

    pub fn get_oldest_page(&self) -> Option<u32> {
        let entries = self.entries.lock().unwrap();
        let mut oldest_page = None;
        let mut oldest_timestamp = f32::MAX;
        for (page_number, entry) in entries.iter().enumerate() {
            if let Some(entry) = entry {
                if entry.get_pin() > 0 {
                    continue;
                }
                if entry.timestamp() < oldest_timestamp {
                    oldest_timestamp = entry.timestamp();
                    oldest_page = Some(page_number as u32);
                }
            }
        }
        oldest_page
    }

    fn get_entry(&self, page_number: u32) -> Option<PageTableEntry> {
        let entries = self.entries.lock().unwrap();
        *entries.get(page_number as usize)?
    }

    pub fn get_frame(&self, page_number: u32) -> Option<u32> {
        let entry = self.get_entry(page_number)?;
        Some(entry.get_frame_number())
    }

    pub fn set_dirty(&self, page_number: u32) {
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

    pub fn map_to_frame(&self, page_number: u32, frame_number: u32) {
        let mut entry = PageTableEntry::zero();
        entry.set_frame_number(frame_number);
        self.write_entry(page_number, entry);
    }

    fn write_entry(&self, page_number: u32, entry: PageTableEntry) {
        self.entries.lock().unwrap()[page_number as usize] = Some(entry);
    }

    pub fn drop_page(&self, page_number: u32) {
        let mut entry = self.get_entry(page_number).unwrap();
        entry.unpin();
        self.write_entry(page_number, entry);
    }

    pub(crate) fn unmap_page(&self, page_to_evict: u32) {
        let mut entries = self.entries.lock().unwrap();
        entries[page_to_evict as usize] = None;
    }
}

#[cfg(test)]
mod tests {
    use crate::buffer_manager::page::PageTableEntry;

    use super::PageTable;

    #[test]
    fn create_mapping() {
        const BLOCKSIZE: usize = 512;
        const CAPACITY: usize = 512 * 64;
        let table: PageTable<BLOCKSIZE, CAPACITY> = PageTable::init();
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
        const BLOCKSIZE: usize = 512;
        const CAPACITY: usize = 512 * 64;
        let table: PageTable<BLOCKSIZE, CAPACITY> = PageTable::init();
        let mut entry = PageTableEntry::zero();
        entry.set_frame_number(43);
        table.write_entry(12, entry);
        let entry = table.get_entry(12).unwrap();
        assert_eq!(entry.get_frame_number(), 43);
    }

    #[test]
    fn create_mapping_and_get_page() {
        let table: PageTable<512, 4096> = PageTable::init();
        table.map_to_frame(1, 3);
        let page = table.get_frame(1).unwrap();
        assert_eq!(page, 3);
    }
}
