use std::mem::size_of;

use crate::{
    buffer_manager::BufferManager,
    disk_manager::{bitmap::Bitmap, DiskManager},
    unordered_file::{Cell, File},
};

pub struct FilesTable<
    'a,
    const BLOCKSIZE: usize,
    const CAPACITY: usize,
    const MEMORY_CAPACITY: usize,
> {
    file: File<'a, BLOCKSIZE, CAPACITY, MEMORY_CAPACITY>,
    disk_manager: DiskManager<BLOCKSIZE, CAPACITY>,
}

impl<'a, const BLOCKSIZE: usize, const CAPACITY: usize, const MEMORY_CAPACITY: usize>
    FilesTable<'a, BLOCKSIZE, CAPACITY, MEMORY_CAPACITY>
{
    pub fn init(
        buffer_manager: &'a BufferManager<'a, BLOCKSIZE, CAPACITY, MEMORY_CAPACITY>,
        disk_manager: &'a DiskManager<BLOCKSIZE, CAPACITY>,
    ) -> Self {
        let file = File::init(disk_manager, buffer_manager);
        Self {
            file,
            disk_manager: disk_manager.clone(),
        }
    }

    pub fn open(
        buffer_manager: &'a BufferManager<'a, BLOCKSIZE, CAPACITY, MEMORY_CAPACITY>,
        disk_manager: &'a DiskManager<BLOCKSIZE, CAPACITY>,
        pos: u32,
    ) -> Self {
        let file = File::open(buffer_manager, disk_manager, pos as u32);
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

    pub fn save(&self) {
        self.file.save();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::disk_manager::DiskManager;
    use disk::Disk;

    #[test]
    fn test_file_table_with_normal_file() {
        const BLOCKSIZE: usize = 512;
        const CAPACITY: usize = 512 * 128;
        const MEMORY_CAPACITY: usize = 512 * 32;
        let disk = Disk::<BLOCKSIZE, CAPACITY>::create("test_file_table2").unwrap();
        let disk_manager = DiskManager::init(&disk);

        {
            let memory = [0; MEMORY_CAPACITY];
            let buffer_manager = BufferManager::init(&memory, &disk);
            let files_table = FilesTable::<BLOCKSIZE, CAPACITY, MEMORY_CAPACITY>::init(
                &buffer_manager,
                &disk_manager,
            );
            let file = File::init(&disk_manager, &buffer_manager);
            file.insert(Cell::new("test".as_bytes().to_vec()));
            file.insert(Cell::new("test".as_bytes().to_vec()));
            file.insert(Cell::new("test".as_bytes().to_vec()));
            files_table.add_file("test", 1);
            files_table.add_file("test2", 2);
            files_table.add_file("test3", 3);
            assert_eq!(files_table.search_file("test"), Some(1));
            assert_eq!(files_table.search_file("test2"), Some(2));
            assert_eq!(files_table.search_file("test3"), Some(3));
            assert_eq!(files_table.search_file("test4"), None);
            files_table.save();
        }
        {
            let memory = [0; MEMORY_CAPACITY];
            let buffer_manager = BufferManager::init(&memory, &disk);
            let files_table = FilesTable::<BLOCKSIZE, CAPACITY, MEMORY_CAPACITY>::open(
                &buffer_manager,
                &disk_manager,
                1,
            );
            assert_eq!(files_table.search_file("test"), Some(1));
            assert_eq!(files_table.search_file("test2"), Some(2));
            assert_eq!(files_table.search_file("test3"), Some(3));
            assert_eq!(files_table.search_file("test4"), None);
        }
    }

    #[test]
    fn test_files_table() {
        const BLOCKSIZE: usize = 512;
        const CAPACITY: usize = 512 * 128;
        const MEMORY_CAPACITY: usize = 512 * 32;
        let disk = Disk::<BLOCKSIZE, CAPACITY>::create("test_files_table").unwrap();
        let disk_manager = DiskManager::init(&disk);

        {
            let memory = [0; MEMORY_CAPACITY];
            let buffer_manager = BufferManager::init(&memory, &disk);
            let files_table = FilesTable::<BLOCKSIZE, CAPACITY, MEMORY_CAPACITY>::init(
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
            files_table.save();
        }
        {
            let memory = [0; MEMORY_CAPACITY];
            let buffer_manager = BufferManager::init(&memory, &disk);
            let files_table = FilesTable::<BLOCKSIZE, CAPACITY, MEMORY_CAPACITY>::open(
                &buffer_manager,
                &disk_manager,
                1,
            );
            assert_eq!(files_table.search_file("test"), Some(1));
            assert_eq!(files_table.search_file("test2"), Some(2));
            assert_eq!(files_table.search_file("test3"), Some(3));
            assert_eq!(files_table.search_file("test4"), None);
        }
    }
}
