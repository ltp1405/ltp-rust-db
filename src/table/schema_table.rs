use file_system::unordered_file::{Cursor, File};

use super::schema::Schema;

pub struct SchemaTable<
    'a,
    const BLOCKSIZE: usize,
    const CAPACITY: usize,
    const MEMORY_CAPACITY: usize,
> {
    file: File<'a, BLOCKSIZE, CAPACITY, MEMORY_CAPACITY>,
}

impl<'a, const BLOCKSIZE: usize, const CAPACITY: usize, const MEMORY_CAPACITY: usize>
    SchemaTable<'a, BLOCKSIZE, CAPACITY, MEMORY_CAPACITY>
{
    pub fn new(file: File<'a, BLOCKSIZE, CAPACITY, MEMORY_CAPACITY>) -> Self {
        Self { file }
    }

    fn cursor(&'a self) -> Cursor<'a, BLOCKSIZE, CAPACITY, MEMORY_CAPACITY> {
        self.file.cursor()
    }

    pub fn save_schema(&self, schema: Schema) {
        for s in schema.serialize() {
            self.file.insert(&s)
        }
    }

    pub fn load_schema(&self) -> Schema {
        let v = self.file.cursor().map(|s| s.to_vec()).collect();
        Schema::deserialize(v).expect("Invalid file format.")
    }

    pub fn save(&self) {
        self.file.save()
    }
}

#[cfg(test)]
mod tests {
    use disk::Disk;
    use file_system::{buffer_manager::BufferManager, disk_manager::DiskManager, FileSystem};

    use crate::table::{
        schema::{DataType, Schema},
        schema_table::SchemaTable,
    };

    #[test]
    fn basic() {
        let disk = disk::Disk::<4096, 819200>::create("schema_basic").unwrap();
        const MEMORY_CAPACITY: usize = 4096 * 32;
        let memory = [0; MEMORY_CAPACITY];
        let buffer_manager: BufferManager<4096, 819200, MEMORY_CAPACITY> =
            BufferManager::init(&memory, &disk);
        let disk_manager = DiskManager::init(&disk);
        let file_system = FileSystem::init(&buffer_manager, &disk_manager).unwrap();
        let table = SchemaTable::new(file_system.create_file("test1").unwrap());

        let schema = Schema {
            schema: vec![
                (String::from("124513o51h3ointl"), DataType::Char(10)),
                (String::from("woeithesnkrjfr"), DataType::Bool),
                (String::from("swl2lk32lkne"), DataType::UInt),
                (String::from("skjdga"), DataType::VarChar(255)),
            ],
        };

        table.save_schema(schema.clone());

        let schema2 = table.load_schema();

        assert_eq!(schema, schema2);
    }
}
