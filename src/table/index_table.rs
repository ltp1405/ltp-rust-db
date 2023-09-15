use file_system::btree_index::btree::{BTree, KeyExistedError, RowAddress};

pub struct Index(Vec<u8>, RowAddress);

pub struct Table<'a, const BLOCKSIZE: usize, const CAPACITY: usize, const MEMORY_CAPACITY: usize> {
    btree: BTree<'a, BLOCKSIZE, CAPACITY, MEMORY_CAPACITY>,
}

impl<'a, const BLOCKSIZE: usize, const CAPACITY: usize, const MEMORY_CAPACITY: usize>
    Table<'a, BLOCKSIZE, CAPACITY, MEMORY_CAPACITY>
{
    pub fn new(btree: BTree<'a, BLOCKSIZE, CAPACITY, MEMORY_CAPACITY>) -> Self {
        Self { btree }
    }

    pub fn insert(&mut self, index: Index) -> Result<(), KeyExistedError> {
        let key = &index.0;
        let address = index.1;
        self.btree.insert(key, address)?;
        Ok(())
    }

    pub fn save(&self) {}
}
