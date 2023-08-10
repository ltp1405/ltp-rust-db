use buffer_manager::BufferManager;
use disk_manager::DiskManager;
use files_table::FilesTable;
use unordered_file::File;

mod btree_index;
pub mod buffer_manager;
pub mod disk_manager;
pub mod files_table;
pub mod unordered_file;
mod frame_allocator;

pub struct FileSystem<
    'a,
    const BLOCKSIZE: usize,
    const CAPACITY: usize,
    const MEMORY_CAPACITY: usize,
> {
    files_table: FilesTable<'a, BLOCKSIZE, CAPACITY, MEMORY_CAPACITY>,
    buffer_manager: &'a BufferManager<'a, BLOCKSIZE, CAPACITY, MEMORY_CAPACITY>,
    disk_manager: &'a DiskManager<BLOCKSIZE, CAPACITY>,
}

#[derive(Debug)]
pub enum FileSystemError {
    FileNotFound,
    DiskFull,
}

impl<'a, const BLOCKSIZE: usize, const CAPACITY: usize, const MEMORY_CAPACITY: usize>
    FileSystem<'a, BLOCKSIZE, CAPACITY, MEMORY_CAPACITY>
{
    pub fn init(
        buffer_manager: &'a BufferManager<'a, BLOCKSIZE, CAPACITY, MEMORY_CAPACITY>,
        disk_manager: &'a DiskManager<BLOCKSIZE, CAPACITY>,
    ) -> std::io::Result<Self> {
        let files_table = FilesTable::init(&buffer_manager, &disk_manager);
        Ok(Self {
            files_table,
            buffer_manager,
            disk_manager,
        })
    }

    pub fn open(
        buffer_manager: &'a BufferManager<'a, BLOCKSIZE, CAPACITY, MEMORY_CAPACITY>,
        disk_manager: &'a DiskManager<BLOCKSIZE, CAPACITY>,
    ) -> std::io::Result<Self> {
        let files_table = FilesTable::open(&buffer_manager, &disk_manager, 1);
        Ok(Self {
            files_table,
            buffer_manager,
            disk_manager,
        })
    }

    pub fn create_file(
        &'a self,
        name: &str,
    ) -> Result<File<BLOCKSIZE, CAPACITY, MEMORY_CAPACITY>, FileSystemError> {
        let file = File::init(&self.disk_manager, &self.buffer_manager);
        println!("file head page number: {}", file.head_page_number);
        self.files_table.add_file(name, file.head_page_number);
        self.save_files_table();
        Ok(file)
    }

    pub fn open_file(
        &'a self,
        name: &str,
    ) -> Result<File<BLOCKSIZE, CAPACITY, MEMORY_CAPACITY>, FileSystemError> {
        let num = self
            .files_table
            .search_file(name)
            .ok_or(FileSystemError::FileNotFound)?;
        let file = File::open(&self.buffer_manager, &self.disk_manager, num);
        Ok(file)
    }

    pub fn save_files_table(&self) {
        self.files_table.save()
    }
}

#[cfg(test)]
mod tests {
    use crate::{buffer_manager::BufferManager, disk_manager::DiskManager, FileSystem};

    #[test]
    fn create_open_file() {
        use disk::Disk;

        const BLOCKSIZE: usize = 512;
        const CAPACITY: usize = BLOCKSIZE * 512;
        const MEMORY_CAPACITY: usize = BLOCKSIZE * 32;
        let disk = Disk::create("create_open_file").unwrap();
        let disk_manager = DiskManager::init(&disk);

        {
            let memory = [0; MEMORY_CAPACITY];
            let buffer_manager = BufferManager::init(&memory, &disk);
            let file_system = FileSystem::<BLOCKSIZE, CAPACITY, MEMORY_CAPACITY>::init(
                &buffer_manager,
                &disk_manager,
            )
            .unwrap();
            let _file1 = file_system.create_file("file1").unwrap();
            file_system.save_files_table()
        }
        {
            let memory = [0; MEMORY_CAPACITY];
            let buffer_manager = BufferManager::init(&memory, &disk);
            let file_system = FileSystem::<BLOCKSIZE, CAPACITY, MEMORY_CAPACITY>::open(
                &buffer_manager,
                &disk_manager,
            )
            .unwrap();
            let _file1 = file_system.open_file("file1").unwrap();
        }
    }
}
