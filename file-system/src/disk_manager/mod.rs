pub mod bitmap;
use std::sync::{Arc, Mutex};

use disk::Disk;

use self::bitmap::{read_bitmap_from_disk, write_bitmap_to_disk, Bitmap};

pub type DiskAddress = u32;

/// This struct is responsible for managing the free space on the disk.
/// It is implemented as a bitmap, where each bit represents a block on the disk.
#[derive(Debug, Clone)]
pub struct DiskManager<const BLOCKSIZE: usize, const CAPACITY: usize> {
    bitmap: Arc<Mutex<Bitmap<BLOCKSIZE, CAPACITY>>>,
    disk: Disk<BLOCKSIZE, CAPACITY>,
}

#[derive(Debug)]
pub enum DiskManagerError {
    DiskFull,
    DiskError,
}

impl<const BLOCKSIZE: usize, const CAPACITY: usize> DiskManager<BLOCKSIZE, CAPACITY> {
    pub fn init(disk: &Disk<BLOCKSIZE, CAPACITY>) -> DiskManager<BLOCKSIZE, CAPACITY> {
        let bitmap: Bitmap<BLOCKSIZE, CAPACITY> = Bitmap::new();
        let bitmap = Arc::new(Mutex::new(bitmap));

        DiskManager {
            disk: disk.clone(),
            bitmap,
        }
    }

    pub fn open(disk: &Disk<BLOCKSIZE, CAPACITY>) -> DiskManager<BLOCKSIZE, CAPACITY> {
        let bitmap = read_bitmap_from_disk(&disk);
        let bitmap = Arc::new(Mutex::new(bitmap));
        DiskManager {
            disk: disk.clone(),
            bitmap,
        }
    }

    pub fn allocate(&self) -> Result<DiskAddress, DiskManagerError> {
        match self.bitmap.lock().unwrap().allocate() {
            Some(b) => Ok(b as u32),
            None => Err(DiskManagerError::DiskFull),
        }
    }

    pub fn deallocate(&self, block: DiskAddress) -> Result<(), DiskManagerError> {
        Ok(self.bitmap.lock().unwrap().deallocate(block as usize))
    }
}

impl<const BLOCKSIZE: usize, const CAPACITY: usize> Drop for DiskManager<BLOCKSIZE, CAPACITY> {
    fn drop(&mut self) {
        write_bitmap_to_disk(&self.disk, &self.bitmap.lock().unwrap());
    }
}
