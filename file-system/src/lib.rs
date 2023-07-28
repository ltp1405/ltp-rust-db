use buffer_manager::BufferManager;
use files_table::FilesTable;
use free_space_manager::{bitmap::Bitmap, FreeSpaceManager};
use unordered_file::File;

pub mod buffer_manager;
pub mod files_table;
pub mod free_space_manager;
pub mod unordered_file;

pub struct FileSystem<
    'a,
    const BLOCKSIZE: usize,
    const CAPACITY: usize,
    const MEMORY_CAPACITY: usize,
> {
    files_table: FilesTable<'a, BLOCKSIZE, CAPACITY, MEMORY_CAPACITY>,
    buffer_manager: &'a BufferManager<'a, BLOCKSIZE, CAPACITY, MEMORY_CAPACITY>,
    disk_manager: &'a FreeSpaceManager<BLOCKSIZE, CAPACITY>,
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
        disk_manager: &'a FreeSpaceManager<BLOCKSIZE, CAPACITY>,
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
        disk_manager: &'a FreeSpaceManager<BLOCKSIZE, CAPACITY>,
    ) -> std::io::Result<Self> {
        let files_table_pos = Bitmap::<BLOCKSIZE, CAPACITY>::size() / BLOCKSIZE
            + if Bitmap::<BLOCKSIZE, CAPACITY>::size() % BLOCKSIZE == 0 {
                0
            } else {
                1
            };
        let files_table = FilesTable::open(&buffer_manager, &disk_manager, files_table_pos as u32);
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
        self.files_table.add_file(name, file.head_page_number);
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
}
