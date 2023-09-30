use super::PageTableEntry;

pub struct PageTableIterator<const BLOCKSIZE: usize, const CAPACITY: usize> {
    current: usize,
    entries: Vec<Option<PageTableEntry>>,
}

impl<const BLOCKSIZE: usize, const CAPACITY: usize> PageTableIterator<BLOCKSIZE, CAPACITY> {
    pub fn new(entries: Vec<Option<PageTableEntry>>) -> Self {
        Self {
            current: 0,
            entries,
        }
    }
}

impl<const BLOCKSIZE: usize, const CAPACITY: usize> Iterator
    for PageTableIterator<BLOCKSIZE, CAPACITY>
{
    type Item = PageTableEntry;

    fn next(&mut self) -> Option<Self::Item> {
        let entry = self.entries.get(self.current)?.clone();
        entry
    }
}
