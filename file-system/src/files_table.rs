use std::mem::size_of;

use crate::unordered_file::File;
use buffer_manager::BufferManager;
use disk_manager::DiskManager;

/// This table is used to store the file name and the block number of the file.
pub struct FilesTable<
    'a,
    const BLOCKSIZE: usize,
    const CAPACITY: usize,
    const MEMORY_CAPACITY: usize,
> {
    file: File<'a, BLOCKSIZE, CAPACITY, MEMORY_CAPACITY>,
}

impl<'a, const BLOCKSIZE: usize, const CAPACITY: usize, const MEMORY_CAPACITY: usize>
    FilesTable<'a, BLOCKSIZE, CAPACITY, MEMORY_CAPACITY>
{
    pub fn init(
        buffer_manager: &'a BufferManager<'a, BLOCKSIZE, CAPACITY, MEMORY_CAPACITY>,
        disk_manager: &'a DiskManager<BLOCKSIZE, CAPACITY>,
    ) -> Self {
        let file = File::init(disk_manager, buffer_manager);
        Self { file }
    }

    pub fn open(
        buffer_manager: &'a BufferManager<'a, BLOCKSIZE, CAPACITY, MEMORY_CAPACITY>,
        disk_manager: &'a DiskManager<BLOCKSIZE, CAPACITY>,
        pos: u32,
    ) -> Self {
        let file = File::open(buffer_manager, disk_manager, pos as u32);
        Self { file }
    }

    pub fn add_file(&self, name: &str, block_number: u32) {
        let mut buf = name.as_bytes().to_vec();
        buf.extend_from_slice(block_number.to_be_bytes().as_ref());
        self.file.insert(&buf);
    }

    pub fn search_file(&'a self, name: &str) -> Option<u32> {
        let search_name = name.as_bytes().to_vec();
        let cursor = self.file.cursor();
        for cell in cursor {
            if cell.len() - size_of::<u32>() == search_name.len() {
                let cell_name = &cell[0..search_name.len()];
                if cell_name == &search_name {
                    return Some(u32::from_be_bytes(cell[name.len()..].try_into().unwrap()));
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
    use disk::Disk;
    use disk_manager::DiskManager;

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
            file.insert("test".as_bytes());
            file.insert("test".as_bytes());
            file.insert("test".as_bytes());
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
