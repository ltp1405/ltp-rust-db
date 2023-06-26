use std::sync::{Arc, Mutex};

use disk::Disk;
use ltp_rust_db_page::pager::Pager;

use super::record::Record;

pub struct FilePage {
    pub pager: Arc<Mutex<Pager>>,
    pub page_num: usize,
}

/// A `File` which only contain records from one `Table`
pub struct File {
    pub pages: Vec<FilePage>,
}

impl File {
    pub fn open(disk: Disk<4096, 65536>) -> Self {
        todo!()
    }

    pub fn create(disk: Disk<4096, 65536>) -> Self {
        todo!()
    }

    pub fn insert(&mut self, record: Record) {}
}

#[cfg(test)]
mod tests {
    use memory::{make_name, PhysicalMemory};

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
        let mut mem = PhysicalMemory::<1024>::create(name).unwrap();
        mem.write(0, 0x12).unwrap();
        assert_eq!(mem.read(0).unwrap(), 0x12);
        let _ = remove_file(make_name(name));
    }

    #[test]
    fn test_over_capacity() {
        let name = "test_over_capacity";
        let _ = remove_file(make_name(name));
        let mut mem = PhysicalMemory::<1024>::create(name).unwrap();
        assert!(mem.write(1024, 0x12).is_err());
        assert!(mem.read(1024).is_err());
        let _ = remove_file(make_name(name));
    }
}
