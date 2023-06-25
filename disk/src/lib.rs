use std::{
    fs::File,
    io::{Read, Seek, SeekFrom, Write},
    mem::size_of,
};

#[derive(Debug)]
enum DiskError {
    IncorrectBlockSize,
    OverCapacity,
}

struct Disk<const BlockSize: usize, const Capacity: usize> {
    file: File,
}

fn make_name(name: &str) -> String {
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
    pub fn init(name: &str) -> Result<Self, std::io::Error> {
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
        Ok(Self { file })
    }

    pub fn open(name: &str) -> Result<Self, std::io::Error> {
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
        Ok(Self { file })
    }

    fn read_block(&mut self, block_number: usize) -> Result<Box<[u8; BLOCKSIZE]>, DiskError> {
        if block_number > CAPACITY / BLOCKSIZE {
            return Err(DiskError::OverCapacity);
        }
        self.file
            .seek(SeekFrom::Start(8 + (block_number * BLOCKSIZE) as u64))
            .unwrap();
        let mut buf = Box::new([0; BLOCKSIZE]);
        self.file.read(&mut *buf).unwrap();
        Ok(buf)
    }

    fn write_block(&mut self, block_number: usize, block: &[u8]) -> Result<(), DiskError> {
        if block.len() != BLOCKSIZE {
            return Err(DiskError::IncorrectBlockSize);
        } else if block_number > CAPACITY / BLOCKSIZE {
            return Err(DiskError::OverCapacity);
        }
        self.file
            .seek(SeekFrom::Start(8 + (block_number * BLOCKSIZE) as u64))
            .unwrap();
        self.file.write(block).unwrap();
        Ok(())
    }
}

#[cfg(test)]
mod test {
    use std::io::{Seek, SeekFrom};

    use crate::Disk;

    #[test]
    fn init() {
        let mut disk: Disk<512, 65536> = Disk::init("disk1").unwrap();
        let len = disk.file.seek(SeekFrom::End(0)).unwrap();
        assert_eq!(len, 65536);
        drop(disk);

        let _disk2: Disk<512, 65536> = Disk::open("disk1").unwrap();
    }

    #[test]
    fn read_write() {
        let mut disk: Disk<512, 65536> = Disk::init("disk2").unwrap();

        let data: [u8; 512] = [0xff; 512];
        disk.write_block(0, &data).unwrap();

        let mut disk2: Disk<512, 65536> = Disk::open("disk2").unwrap();
        assert_eq!(*disk2.read_block(0).unwrap(), data);
    }
}
