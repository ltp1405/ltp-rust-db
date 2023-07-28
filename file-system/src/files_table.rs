use std::mem::size_of;

use disk::Disk;

use crate::{
    buffer_manager::BufferManager,
    free_space_manager::{bitmap::Bitmap, FreeSpaceManager},
    unordered_file::{Cell, File},
};

pub struct FilesTable<
    'a,
    const BLOCKSIZE: usize,
    const CAPACITY: usize,
    const MEMORY_CAPACITY: usize,
> {
    file: File<'a, BLOCKSIZE, CAPACITY, MEMORY_CAPACITY>,
    disk_manager: FreeSpaceManager<BLOCKSIZE, CAPACITY>,
}

impl<'a, const BLOCKSIZE: usize, const CAPACITY: usize, const MEMORY_CAPACITY: usize>
    FilesTable<'a, BLOCKSIZE, CAPACITY, MEMORY_CAPACITY>
{
    pub fn init(
        buffer_manager: &'a BufferManager<'a, BLOCKSIZE, CAPACITY, MEMORY_CAPACITY>,
        disk_manager: &'a FreeSpaceManager<BLOCKSIZE, CAPACITY>,
    ) -> Self {
        let file = File::init(disk_manager, buffer_manager);
        let files_table_pos = Bitmap::<BLOCKSIZE, CAPACITY>::size() / BLOCKSIZE
            + if Bitmap::<BLOCKSIZE, CAPACITY>::size() % BLOCKSIZE == 0 {
                0
            } else {
                1
            };
        // assert_eq!(files_table_pos, file.head_page_number as usize);
        Self {
            file,
            disk_manager: disk_manager.clone(),
        }
    }

    pub fn open(
        buffer_manager: &'a BufferManager<'a, BLOCKSIZE, CAPACITY, MEMORY_CAPACITY>,
        disk_manager: &'a FreeSpaceManager<BLOCKSIZE, CAPACITY>,
        block_number: u32,
    ) -> Self {
        let file = File::open(buffer_manager, disk_manager, block_number);
        Self {
            file,
            disk_manager: disk_manager.clone(),
        }
    }

    pub fn add_file(&self, name: &str, block_number: u32) {
        let mut buf = name.as_bytes().to_vec();
        buf.extend_from_slice(block_number.to_be_bytes().as_ref());
        self.file.insert(Cell::new(buf))
    }

    pub fn search_file(&'a self, name: &str) -> Option<u32> {
        let buf = name.as_bytes().to_vec();
        let cursor = self.file.cursor();
        for cell in cursor {
            if cell.payload_len() - size_of::<u32>() == buf.len() {
                let cell_buf = &cell[0..buf.len()];
                if cell_buf == &buf {
                    let mut block_number = [0; 4];
                    block_number.copy_from_slice(&cell[buf.len()..]);
                    return Some(u32::from_be_bytes(block_number));
                }
            }
        }
        None
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::free_space_manager::FreeSpaceManager;
    use disk::Disk;

    #[test]
    fn test_files_table() {
        const BLOCKSIZE: usize = 512;
        const CAPACITY: usize = 512 * 128;
        const MEMORY_CAPACITY: usize = 512 * 32;
        let memory = [0; MEMORY_CAPACITY];
        let disk = Disk::<BLOCKSIZE, CAPACITY>::create("test_files_table").unwrap();
        let disk_manager = FreeSpaceManager::init(&disk);
        let buffer_manager = BufferManager::init(&memory, &disk);
        let file = File::init(&disk_manager, &buffer_manager);
        let mut files_table = FilesTable::<BLOCKSIZE, CAPACITY, MEMORY_CAPACITY>::init(
            &buffer_manager,
            &disk_manager,
        );
        files_table.add_file("test", 1);
        files_table.add_file("test2", 2);
        files_table.add_file("test3", 3);
        assert_eq!(files_table.search_file("test"), Some(1));
        assert_eq!(files_table.search_file("test2"), Some(2));
        assert_eq!(files_table.search_file("test3"), Some(3));
        assert_eq!(files_table.search_file("test4"), None);
    }
}
