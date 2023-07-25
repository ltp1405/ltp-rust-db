use std::{
    fs::File,
    io::{Read, Seek, SeekFrom, Write},
    mem::size_of,
    sync::{Arc, Mutex},
};

#[derive(Debug)]
pub enum MemoryError {
    OverCapacity,
}

#[derive(Clone)]
pub struct PhysicalMemory<const CAPACITY: usize> {
    file: Arc<Mutex<File>>,
    buffer: Arc<Mutex<[u8; CAPACITY]>>,
}

pub fn make_name(name: &str) -> String {
    let name = name.replace("-", "_");
    let mut disk_name = String::from("MEMORY_FILE_");
    disk_name.push_str(&name);
    disk_name
}

fn write_header(file: &mut File, capacity: u32) -> Result<(), std::io::Error> {
    file.seek(SeekFrom::Start(0))?;
    let capacity = capacity.to_be_bytes();
    file.seek(SeekFrom::Start(size_of::<u32>() as u64))?;
    file.write(&capacity)?;
    Ok(())
}

fn read_header(file: &mut File) -> Result<u32, std::io::Error> {
    let mut page_size: [u8; size_of::<u32>()] = [0; size_of::<u32>()];
    let mut capacity = [0; size_of::<u32>()];
    file.seek(SeekFrom::Start(0))?;
    file.read_exact(&mut page_size)?;
    let page_size = u32::from_be_bytes(page_size);

    file.seek(SeekFrom::Start(size_of::<u32>() as u64))?;
    file.read(&mut capacity)?;

    let capacity = u32::from_be_bytes(capacity);

    Ok(capacity)
}

impl<const CAPACITY: usize> PhysicalMemory<CAPACITY> {
    pub fn create(name: &str) -> Result<Self, std::io::Error> {
        let c = (CAPACITY as f64).log2();
        assert!(
            ((c - c.round()).abs() < 1e-20),
            "Capacity {} is not a multiply of 2",
            CAPACITY
        );
        let mut file = File::options()
            .truncate(true)
            .write(true)
            .read(true)
            .create(true)
            .open(make_name(name))?;
        file.set_len(CAPACITY as u64)?;
        write_header(&mut file, CAPACITY as u32)?;
        let file = Arc::new(Mutex::new(file));
        Ok(Self {
            file,
            buffer: Arc::new(Mutex::new([0; CAPACITY])),
        })
    }

    pub fn connect(name: &str) -> Result<Self, std::io::Error> {
        let c = (CAPACITY as f64).log2();
        assert!(
            ((c - c.round()).abs() < 1e-20),
            "Capacity {} is not a multiply of 2",
            CAPACITY
        );
        let mut file = File::options()
            .write(true)
            .read(true)
            .open(make_name(name))?;
        let capacity = read_header(&mut file)?;
        assert_eq!(CAPACITY, capacity as usize, "Incorrect disk capacity");
        let file = Arc::new(Mutex::new(file));
        Ok(Self {
            file,
            buffer: Arc::new(Mutex::new([0; CAPACITY])),
        })
    }

    pub fn check_address(&self, address: u64) -> Result<(), MemoryError> {
        if address as usize >= CAPACITY {
            return Err(MemoryError::OverCapacity);
        }
        Ok(())
    }

    pub fn read_u8(&self, address: u64) -> Result<u8, MemoryError> {
        self.check_address(address)?;
        let buffer = self.buffer.lock().unwrap();
        Ok(buffer[address as usize])
    }

    pub fn write_u8(&self, address: u64, byte: u8) -> Result<(), MemoryError> {
        self.check_address(address)?;
        let mut buffer = self.buffer.lock().unwrap();
        buffer[address as usize] = byte;
        Ok(())
    }

    pub fn read_u16(&self, address: u64) -> Result<u16, MemoryError> {
        self.check_address(address)?;
        let buffer = self.buffer.lock().unwrap();
        let mut bytes = [0; size_of::<u16>()];
        bytes.copy_from_slice(&buffer[address as usize..(address + 2) as usize]);
        Ok(u16::from_be_bytes(bytes))
    }

    pub fn write_u16(&self, address: u64, word: u16) -> Result<(), MemoryError> {
        self.check_address(address)?;
        let mut buffer = self.buffer.lock().unwrap();
        let bytes = word.to_be_bytes();
        buffer[address as usize..(address + 2) as usize].copy_from_slice(&bytes);
        Ok(())
    }

    pub fn read_u32(&self, address: u64) -> Result<u32, MemoryError> {
        self.check_address(address)?;
        let buffer = self.buffer.lock().unwrap();
        let mut bytes = [0; size_of::<u32>()];
        bytes.copy_from_slice(&buffer[address as usize..(address + 4) as usize]);
        Ok(u32::from_be_bytes(bytes))
    }

    pub fn write_u32(&self, address: u64, word: u32) -> Result<(), MemoryError> {
        self.check_address(address)?;
        let mut buffer = self.buffer.lock().unwrap();
        let bytes = word.to_be_bytes();
        buffer[address as usize..(address + 4) as usize].copy_from_slice(&bytes);
        Ok(())
    }
}

impl<const CAPACITY: usize> Drop for PhysicalMemory<CAPACITY> {
    fn drop(&mut self) {
        let mut file = self.file.lock().unwrap();
        file.seek(SeekFrom::Start(size_of::<u32>() as u64)).unwrap();
        let buffer = self.buffer.lock().unwrap();
        file.write(buffer.as_slice()).unwrap();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs::remove_file;

    #[test]
    fn test_create() {
        let name = "test_create";
        let _ = remove_file(make_name(name));
        let _ = PhysicalMemory::<1024>::create(name).unwrap();
        let _ = remove_file(make_name(name));
    }

    #[test]
    fn test_connect() {
        let name = "test_connect";
        let _ = remove_file(make_name(name));
        let _ = PhysicalMemory::<1024>::create(name).unwrap();
        let _ = PhysicalMemory::<1024>::connect(name).unwrap();
        let _ = remove_file(make_name(name));
    }

    #[test]
    fn test_read_write() {
        let name = "test_read_write";
        let _ = remove_file(make_name(name));
        let mem = PhysicalMemory::<1024>::create(name).unwrap();
        mem.write_u8(0, 0x12).unwrap();
        assert_eq!(mem.read_u8(0).unwrap(), 0x12);
        let _ = remove_file(make_name(name));
    }

    #[test]
    fn test_write_a_lot_of_data() {
        let name = "test_write_a_lot_of_data";
        let _ = remove_file(make_name(name));
        let mem = PhysicalMemory::<1024>::create(name).unwrap();
        for i in 0..1024 {
            mem.write_u8(i, i as u8).unwrap();
        }
        for i in 0..1024 {
            assert_eq!(mem.read_u8(i).unwrap(), i as u8);
        }
        let _ = remove_file(make_name(name));
    }

    #[test]
    fn test_write_invalid_address() {
        let name = "test_write_invalid_address";
        let _ = remove_file(make_name(name));
        let mem = PhysicalMemory::<1024>::create(name).unwrap();
        assert!(mem.write_u8(1024, 0x12).is_err());
        let _ = remove_file(make_name(name));
    }
}
