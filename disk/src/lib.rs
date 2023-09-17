use std::{
    fs::{remove_file, File},
    io::{Read, Seek, SeekFrom, Write},
    mem::size_of,
    sync::{Arc, Mutex},
};

use log::info;

#[derive(Debug, PartialEq)]
pub enum DiskError {
    IncorrectBlockSize,
    OverCapacity,
}

const HEADER_SIZE: usize = size_of::<u32>() * 2;

#[derive(Debug, Clone)]
pub struct Disk<const BLOCKSIZE: usize, const CAPACITY: usize> {
    file_name: String,
    file: Arc<Mutex<File>>,
}

pub fn make_name(name: &str) -> String {
    let name = name.replace("-", "_");
    let mut disk_name = String::from("DISK_IMAGE_");
    disk_name.push_str(&name);
    disk_name
}

fn write_header(file: &mut File, block_size: u32, capacity: u32) -> Result<(), std::io::Error> {
    let block_size = block_size.to_be_bytes();
    file.seek(SeekFrom::Start(0))?;
    file.write(&block_size)?;
    let capacity = capacity.to_be_bytes();
    file.seek(SeekFrom::Start(size_of::<u32>() as u64))?;
    file.write(&capacity)?;
    Ok(())
}

fn read_header(file: &mut File) -> Result<(u32, u32), std::io::Error> {
    let mut block_size: [u8; size_of::<u32>()] = [0; size_of::<u32>()];
    let mut capacity = [0; size_of::<u32>()];
    file.seek(SeekFrom::Start(0))?;
    file.read_exact(&mut block_size)?;
    let block_size = u32::from_be_bytes(block_size);

    file.seek(SeekFrom::Start(size_of::<u32>() as u64))?;
    file.read(&mut capacity)?;

    let capacity = u32::from_be_bytes(capacity);

    Ok((block_size, capacity))
}

impl<const BLOCKSIZE: usize, const CAPACITY: usize> Disk<BLOCKSIZE, CAPACITY> {
    pub fn create(name: &str) -> Result<Self, std::io::Error> {
        assert_eq!(
            CAPACITY % BLOCKSIZE,
            0,
            "Capacity must be a multiply of BlockSize"
        );
        let mut file = File::options()
            .truncate(true)
            .write(true)
            .read(true)
            .create(true)
            .open(make_name(name))?;
        file.set_len(CAPACITY as u64)?;
        write_header(&mut file, BLOCKSIZE as u32, CAPACITY as u32)?;
        Ok(Self {
            file_name: String::from(name),
            file: Arc::new(Mutex::new(file)),
        })
    }

    pub fn connect(name: &str) -> Result<Self, std::io::Error> {
        assert_eq!(
            CAPACITY % BLOCKSIZE,
            0,
            "Capacity must be a multiply of BlockSize"
        );
        let mut file = File::options()
            .write(true)
            .read(true)
            .open(make_name(name))?;
        let (block_size, capacity) = read_header(&mut file)?;
        assert_eq!(BLOCKSIZE, block_size as usize, "Incorrect disk block size");
        assert_eq!(CAPACITY, capacity as usize, "Incorrect disk capacity");
        Ok(Self {
            file_name: String::from(name),
            file: Arc::new(Mutex::new(file)),
        })
    }

    pub fn read_block(&self, block_number: usize) -> Result<Box<[u8; BLOCKSIZE]>, DiskError> {
        let mut file = self.file.lock().unwrap();
        info!("Start reading block[{}]", block_number);
        if block_number >= CAPACITY / BLOCKSIZE {
            return Err(DiskError::OverCapacity);
        }
        file.seek(SeekFrom::Start(
            HEADER_SIZE as u64 + (block_number * BLOCKSIZE) as u64,
        ))
        .unwrap();
        let mut buf = Box::new([0; BLOCKSIZE]);
        file.read(&mut *buf).unwrap();
        info!("Done reading block[{}]", block_number);
        Ok(buf)
    }

    pub fn write_block(&self, block_number: usize, block: &[u8]) -> Result<(), DiskError> {
        let mut file = self.file.lock().unwrap();
        info!("Start writing block[{}]", block_number);
        if block.len() != BLOCKSIZE {
            return Err(DiskError::IncorrectBlockSize);
        } else if block_number >= CAPACITY / BLOCKSIZE {
            return Err(DiskError::OverCapacity);
        }
        file.seek(SeekFrom::Start(
            HEADER_SIZE as u64 + (block_number * BLOCKSIZE) as u64,
        ))
        .unwrap();
        file.write(block).unwrap();
        info!("Done writing block[{}]", block_number);
        Ok(())
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use std::fs::remove_file;

    #[test]
    fn test_create() {
        let disk = Disk::<512, 1024>::create("test_create").unwrap();
        remove_file(make_name("test_create")).unwrap();
    }

    #[test]
    fn test_connect() {
        let disk = Disk::<512, 1024>::create("test_connect").unwrap();
        let disk = Disk::<512, 1024>::connect("test_connect").unwrap();
        remove_file(make_name("test_connect")).unwrap();
    }

    #[test]
    fn test_read_write() {
        let disk = Disk::<512, 1024>::create("test_read_write").unwrap();
        let mut block = Box::new([0; 512]);
        block[0] = 1;
        disk.write_block(0, &*block).unwrap();
        let block = disk.read_block(0).unwrap();
        assert_eq!(block[0], 1);
        remove_file(make_name("test_read_write")).unwrap();
    }

    #[test]
    fn test_read_write_over_capacity() {
        let disk = Disk::<512, 1024>::create("test_read_write_over_capacity").unwrap();
        let mut block = Box::new([0; 512]);
        block[0] = 1;
        assert_eq!(disk.write_block(2, &*block), Err(DiskError::OverCapacity));
        assert_eq!(disk.read_block(2), Err(DiskError::OverCapacity));
        remove_file(make_name("test_read_write_over_capacity")).unwrap();
    }

    #[test]
    fn test_read_write_incorrect_block_size() {
        let disk = Disk::<512, 1024>::create("test_read_write_incorrect_block_size").unwrap();
        let mut block = Box::new([0; 256]);
        block[0] = 1;
        assert_eq!(
            disk.write_block(0, &*block),
            Err(DiskError::IncorrectBlockSize)
        );
        remove_file(make_name("test_read_write_incorrect_block_size")).unwrap();
    }

    #[test]
    fn test_read_write_incorrect_block_size2() {
        let disk = Disk::<512, 1024>::create("test_read_write_incorrect_block_size2").unwrap();
        let mut block = Box::new([0; 1024]);
        block[0] = 1;
        assert_eq!(
            disk.write_block(0, &*block),
            Err(DiskError::IncorrectBlockSize)
        );
        remove_file(make_name("test_read_write_incorrect_block_size2")).unwrap();
    }

    #[test]
    #[should_panic]
    fn test_invalid_header() {
        let _ = Disk::<512, 1024>::create("test_invalid_header").unwrap();
        let mut file = File::options()
            .write(true)
            .read(true)
            .open(make_name("test_invalid_header"))
            .unwrap();
        file.seek(SeekFrom::Start(0)).unwrap();
        file.write(&[0; 8]).unwrap();
        Disk::<512, 1024>::connect("test_invalid_header").unwrap_err();
        remove_file(make_name("test_invalid_header")).unwrap();
    }
}
