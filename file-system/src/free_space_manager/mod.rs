pub mod bitmap;
use std::sync::{Arc, Mutex};

use disk::Disk;

use self::bitmap::{read_bitmap_from_disk, write_bitmap_to_disk, Bitmap};

pub type DiskAddress = u32;

/// This struct is responsible for managing the free space on the disk.
/// It is implemented as a bitmap, where each bit represents a block on the disk.
#[derive(Debug, Clone)]
pub struct FreeSpaceManager<const BLOCKSIZE: usize, const CAPACITY: usize> {
    bitmap: Arc<Mutex<Bitmap<BLOCKSIZE, CAPACITY>>>,
    disk: Disk<BLOCKSIZE, CAPACITY>,
}

#[derive(Debug)]
pub enum FreeSpaceManagerError {
    DiskFull,
    DiskError,
}

impl<const BLOCKSIZE: usize, const CAPACITY: usize> FreeSpaceManager<BLOCKSIZE, CAPACITY> {
    pub fn init(disk: &Disk<BLOCKSIZE, CAPACITY>) -> FreeSpaceManager<BLOCKSIZE, CAPACITY> {
        let mut bitmap: Bitmap<BLOCKSIZE, CAPACITY> = Bitmap::new();
        let bitmap = Arc::new(Mutex::new(bitmap));

        FreeSpaceManager {
            disk: disk.clone(),
            bitmap,
        }
    }

    pub fn open(disk: &Disk<BLOCKSIZE, CAPACITY>) -> FreeSpaceManager<BLOCKSIZE, CAPACITY> {
        let bitmap = read_bitmap_from_disk(&disk);
        let bitmap = Arc::new(Mutex::new(bitmap));
        FreeSpaceManager {
            disk: disk.clone(),
            bitmap,
        }
    }

    pub fn allocate(&self) -> Result<DiskAddress, FreeSpaceManagerError> {
        match self.bitmap.lock().unwrap().allocate() {
            Some(b) => Ok(b as u32),
            None => Err(FreeSpaceManagerError::DiskFull),
        }
    }

    pub fn deallocate(&self, block: DiskAddress) -> Result<(), FreeSpaceManagerError> {
        Ok(self.bitmap.lock().unwrap().deallocate(block as usize))
    }
}

impl<const BLOCKSIZE: usize, const CAPACITY: usize> Drop for FreeSpaceManager<BLOCKSIZE, CAPACITY> {
    fn drop(&mut self) {
        write_bitmap_to_disk(&self.disk, &self.bitmap.lock().unwrap());
    }
}
