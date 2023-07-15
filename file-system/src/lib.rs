use disk::Disk;
use files_table::FilesTable;
use free_space_manager::{bitmap::Bitmap, FreeSpaceManager};
use unordered_file::File;

mod files_table;
pub mod free_space_manager;
pub mod unordered_file;

pub struct FileSystem<const BLOCKSIZE: usize, const CAPACITY: usize> {
    files_table: FilesTable<BLOCKSIZE, CAPACITY>,
    disk: Disk<BLOCKSIZE, CAPACITY>,
    disk_manager: FreeSpaceManager<BLOCKSIZE, CAPACITY>,
}

#[derive(Debug)]
pub enum FileSystemError {
    FileNotFound,
    DiskFull,
}

impl<const BLOCKSIZE: usize, const CAPACITY: usize> FileSystem<BLOCKSIZE, CAPACITY> {
    pub fn create(name: &str) -> std::io::Result<Self> {
        let disk = Disk::create(name)?;
        let disk_manager = FreeSpaceManager::init(&disk);

        let files_table = FilesTable::init(&disk, &disk_manager);
        Ok(Self {
            files_table,
            disk,
            disk_manager,
        })
    }

    pub fn open(name: &str) -> std::io::Result<Self> {
        let disk = Disk::connect(name)?;
        let disk_manager = FreeSpaceManager::open(&disk);
        let files_table_pos = Bitmap::<BLOCKSIZE, CAPACITY>::size() / BLOCKSIZE
            + if Bitmap::<BLOCKSIZE, CAPACITY>::size() % BLOCKSIZE == 0 {
                0
            } else {
                1
            };
        let files_table = FilesTable::open(&disk, &disk_manager, files_table_pos as u32);
        Ok(Self {
            files_table,
            disk,
            disk_manager,
        })
    }

    pub fn create_file(
        &mut self,
        name: &str,
    ) -> Result<File<BLOCKSIZE, CAPACITY>, FileSystemError> {
        let file = File::init(&self.disk, &self.disk_manager);
        self.files_table.add_file(name, file.first_page_num);
        Ok(file)
    }

    pub fn open_file(&self, name: &str) -> Result<File<BLOCKSIZE, CAPACITY>, FileSystemError> {
        let num = self
            .files_table
            .search_file(name)
            .ok_or(FileSystemError::FileNotFound)?;
        let file = File::open(&self.disk, &self.disk_manager, num);
        Ok(file)
    }
}
