use super::{
    record::{InvalidSchema, Record},
    schema::Schema,
};

use file_system::unordered_file::{Cursor, File};

pub struct Table<'a, const BLOCKSIZE: usize, const CAPACITY: usize, const MEMORY_CAPACITY: usize> {
    file: File<'a, BLOCKSIZE, CAPACITY, MEMORY_CAPACITY>,
    schema: &'a Schema,
}

impl<'a, const BLOCKSIZE: usize, const CAPACITY: usize, const MEMORY_CAPACITY: usize>
    Table<'a, BLOCKSIZE, CAPACITY, MEMORY_CAPACITY>
{
    pub fn new(file: File<'a, BLOCKSIZE, CAPACITY, MEMORY_CAPACITY>, schema: &'a Schema) -> Self {
        Self { file, schema }
    }

    pub fn insert(&mut self, record: Record) -> Result<(), InvalidSchema> {
        let cell = &record.to_bytes(self.schema)?;
        self.file.insert(cell);
        Ok(())
    }

    pub fn cursor(&'a self) -> Cursor<'a, BLOCKSIZE, CAPACITY, MEMORY_CAPACITY> {
        self.file.cursor()
    }

    pub fn save(&self) {
        self.file.save()
    }
}

#[cfg(test)]
mod tests {
    use buffer_manager::BufferManager;
    use disk::Disk;
    use disk_manager::DiskManager;
    use file_system::FileSystem;

    use crate::table::{
        record::{Field, Record},
        schema::{DataType, Schema},
    };

    use super::Table;

    #[test]
    fn basic() {
        let disk = disk::Disk::<4096, 819200>::create("table_basic").unwrap();
        const MEMORY_CAPACITY: usize = 4096 * 32;
        let memory = [0; MEMORY_CAPACITY];
        let buffer_manager: BufferManager<4096, 819200, MEMORY_CAPACITY> =
            BufferManager::init(&memory, &disk);
        let disk_manager = DiskManager::init(&disk);
        let file_system = FileSystem::init(&buffer_manager, &disk_manager).unwrap();
        let schema = Schema {
            schema: vec![
                (String::new(), DataType::Char(10)),
                (String::new(), DataType::Bool),
                (String::new(), DataType::UInt),
                (String::new(), DataType::VarChar(255)),
            ],
        };
        let mut table = Table::new(file_system.create_file("test1").unwrap(), &schema);

        let record = Record {
            data: vec![
                Field::Char(Some(b"Hello".to_vec())),
                Field::Bool(Some(true)),
                Field::UInt(Some(42)),
                Field::VarChar(Some(b"World".to_vec())),
            ],
        };
        let bytes = record.clone().to_bytes(&schema).unwrap();
        let record2 = Record::from_bytes(bytes, &schema).unwrap();
        assert_eq!(record, record2);

        table.insert(record.clone()).unwrap();

        let cursor = table.cursor();
        let r = cursor.read();
        let record2 = Record::from_bytes(r.unwrap(), &schema).unwrap();
        assert_eq!(record, record2);
    }

    #[test]
    fn simple_insert() {
        let disk = disk::Disk::<4096, 819200>::create("table_simple_insert").unwrap();
        const MEMORY_CAPACITY: usize = 4096 * 32;
        let memory = [0; MEMORY_CAPACITY];
        let buffer_manager: BufferManager<4096, 819200, MEMORY_CAPACITY> =
            BufferManager::init(&memory, &disk);
        let disk_manager = DiskManager::init(&disk);
        let file_system = FileSystem::init(&buffer_manager, &disk_manager).unwrap();
        let schema = Schema {
            schema: vec![
                (String::new(), DataType::Char(10)),
                (String::new(), DataType::Bool),
                (String::new(), DataType::UInt),
                (String::new(), DataType::VarChar(255)),
            ],
        };
        let mut table = Table::new(file_system.create_file("test1").unwrap(), &schema);

        let record = Record {
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
            let record2 = Record::from_bytes(r, &schema).unwrap();
            assert_eq!(record, record2);
        }
    }

    #[test]
    fn big_record_insert() {
        const CAPACITY: usize = 512 * 4096;
        let disk = Disk::<4096, CAPACITY>::create("table_big_record_insert").unwrap();
        const MEMORY_CAPACITY: usize = 4096 * 32;
        let memory = [0; MEMORY_CAPACITY];
        let buffer_manager: BufferManager<4096, CAPACITY, MEMORY_CAPACITY> =
            BufferManager::init(&memory, &disk);
        let disk_manager = DiskManager::init(&disk);
        let file_system = FileSystem::init(&buffer_manager, &disk_manager).unwrap();
        let schema = Schema {
            schema: vec![
                (String::new(), DataType::Int),
                (String::new(), DataType::Int),
                (String::new(), DataType::Int),
                (String::new(), DataType::Char(10)),
                (String::new(), DataType::Char(10)),
                (String::new(), DataType::Char(10)),
                (String::new(), DataType::Bool),
                (String::new(), DataType::Bool),
                (String::new(), DataType::Float),
                (String::new(), DataType::UInt),
                (String::new(), DataType::VarChar(255)),
                (String::new(), DataType::VarChar(255)),
            ],
        };
        let record = Record {
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

        let mut table = Table::new(file_system.create_file("test1").unwrap(), &schema);

        for _ in 0..1000 {
            table.insert(record.clone());
        }

        for cell in table.cursor() {
            let record2 = Record::from_bytes(cell, &schema).unwrap();
            assert_eq!(record, record2);
        }
    }

    #[test]
    fn a_lot_of_insert() {
        const CAPACITY: usize = 512 * 4096;
        let disk = Disk::<512, CAPACITY>::create("test_table_a_lot_of_insert").unwrap();
        const MEMORY_CAPACITY: usize = 4096 * 32;
        let memory = [0; MEMORY_CAPACITY];
        let buffer_manager: BufferManager<512, CAPACITY, MEMORY_CAPACITY> =
            BufferManager::init(&memory, &disk);
        let disk_manager = DiskManager::init(&disk);
        let file_system = FileSystem::init(&buffer_manager, &disk_manager).unwrap();
        let schema = Schema {
            schema: vec![
                (String::new(), DataType::Char(10)),
                (String::new(), DataType::Bool),
                (String::new(), DataType::UInt),
                (String::new(), DataType::VarChar(255)),
            ],
        };
        let mut table = Table::new(file_system.create_file("test1").unwrap(), &schema);

        let record = Record {
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
            let record2 = Record::from_bytes(r, &schema).unwrap();
            assert_eq!(record, record2);
        }
    }
}
