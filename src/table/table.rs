use super::record::Record;

use file_system::{
    free_space_manager::FreeSpaceManager,
    unordered_file::{Cursor, File},
};

pub struct Table<const BLOCKSIZE: usize, const CAPACITY: usize> {
    file: File<BLOCKSIZE, CAPACITY>,
}

impl<const BLOCKSIZE: usize, const CAPACITY: usize> Table<BLOCKSIZE, CAPACITY> {
    pub fn init(
        disk: &disk::Disk<BLOCKSIZE, CAPACITY>,
        disk_manager: &FreeSpaceManager<BLOCKSIZE, CAPACITY>,
    ) -> Self {
        let file = File::init(disk, disk_manager);
        Self { file }
    }

    pub fn insert(&mut self, record: Record) {
        let cell = record.to_cell();
        self.file.insert(cell);
    }

    pub fn cursor(&self) -> Cursor<BLOCKSIZE, CAPACITY> {
        self.file.cursor()
    }
}

#[cfg(test)]
mod tests {
    use std::fs::remove_file;

    use disk::Disk;
    use file_system::free_space_manager::FreeSpaceManager;

    use crate::table::{
        record::{Field, Record},
        schema::{DataType, Schema},
    };

    use super::Table;

    #[test]
    fn basic() {
        let disk = disk::Disk::<4096, 819200>::create("table_basic").unwrap();
        let disk_manager = FreeSpaceManager::init(&disk);
        let mut table = Table::init(&disk, &disk_manager);

        let schema = Schema {
            schema: vec![
                DataType::Char(10),
                DataType::Bool,
                DataType::UInt,
                DataType::VarChar(255),
            ],
        };
        let record = Record {
            schema: &schema,
            data: vec![
                Field::Char(Some(b"Hello".to_vec())),
                Field::Bool(Some(true)),
                Field::UInt(Some(42)),
                Field::VarChar(Some(b"World".to_vec())),
            ],
        };
        let bytes = record.clone().to_bytes();
        let record2 = Record::from_bytes(&schema, bytes);
        assert_eq!(record, record2);

        table.insert(record.clone());

        let mut cursor = table.cursor();
        let r = cursor.read();
        let record2 = Record::from_bytes(&schema, r.unwrap().buf);
        assert_eq!(record, record2);
    }

    #[test]
    fn simple_insert() {
        let disk = disk::Disk::<4096, 819200>::create("table_simple_insert").unwrap();
        let disk_manager = FreeSpaceManager::init(&disk);
        let mut table = Table::init(&disk, &disk_manager);

        let schema = Schema {
            schema: vec![
                DataType::Char(10),
                DataType::Bool,
                DataType::UInt,
                DataType::VarChar(255),
            ],
        };
        let record = Record {
            schema: &schema,
            data: vec![
                Field::Char(Some(b"Hello".to_vec())),
                Field::Bool(Some(true)),
                Field::UInt(Some(42)),
                Field::VarChar(Some(b"World".to_vec())),
            ],
        };

        for _ in 0..10 {
            table.insert(record.clone());
        }

        for r in table.cursor() {
            let record2 = Record::from_bytes(&schema, r.buf);
            assert_eq!(record, record2);
        }
    }

    #[test]
    fn big_record_insert() {
        const CAPACITY: usize = 512 * 4096;
        let disk = Disk::<4096, CAPACITY>::create("table_big_record_insert").unwrap();
        let disk_manager = FreeSpaceManager::init(&disk);
        let mut table = Table::init(&disk, &disk_manager);

        let schema = Schema {
            schema: vec![
                DataType::Int,
                DataType::Int,
                DataType::Int,
                DataType::Char(10),
                DataType::Char(10),
                DataType::Char(10),
                DataType::Bool,
                DataType::Bool,
                DataType::Float,
                DataType::UInt,
                DataType::VarChar(255),
                DataType::VarChar(255),
            ],
        };
        let record = Record {
            schema: &schema,
            data: vec![
                Field::Int(Some(1)),
                Field::Int(Some(2)),
                Field::Int(Some(3)),
                Field::Char(Some(b"Hello".to_vec())),
                Field::Char(Some(b"Hello".to_vec())),
                Field::Char(Some(b"Hello".to_vec())),
                Field::Bool(Some(true)),
                Field::Bool(Some(true)),
                Field::Float(Some(1.0)),
                Field::UInt(Some(42)),
                Field::VarChar(Some(b"World".to_vec())),
                Field::VarChar(Some(b"World".to_vec())),
            ],
        };

        for _ in 0..1000 {
            table.insert(record.clone());
        }

        for cell in table.cursor() {
            let record2 = Record::from_bytes(&schema, cell.buf);
            assert_eq!(record, record2);
        }
    }

    #[test]
    fn a_lot_of_insert() {
        const CAPACITY: usize = 512 * 4096;
        let disk = Disk::<512, CAPACITY>::create("test_table_a_lot_of_insert").unwrap();
        let disk_manager = FreeSpaceManager::init(&disk);
        let mut table = Table::init(&disk, &disk_manager);

        let schema = Schema {
            schema: vec![
                DataType::Char(10),
                DataType::Bool,
                DataType::UInt,
                DataType::VarChar(255),
            ],
        };
        let record = Record {
            schema: &schema,
            data: vec![
                Field::Char(Some(b"Hello".to_vec())),
                Field::Bool(Some(true)),
                Field::UInt(Some(42)),
                Field::VarChar(Some(b"World".to_vec())),
            ],
        };

        for _ in 0..10000 {
            table.insert(record.clone());
        }

        for r in table.cursor() {
            let record2 = Record::from_bytes(&schema, r.buf);
            assert_eq!(record, record2);
        }
    }
}
