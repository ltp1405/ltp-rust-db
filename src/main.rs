use disk::Disk;
use file_system::{buffer_manager::BufferManager, disk_manager::DiskManager, FileSystem};
use my_database::table::{
    record::{Field, Record},
    schema::{DataType, Schema},
    schema_table::SchemaTable,
    table::Table,
};
const BLOCKSIZE: usize = 512;
const CAPACITY: usize = 512 * 4096;
const MEMORY_CAPACITY: usize = 4096 * 32;

fn main() {
    let disk = Disk::<BLOCKSIZE, CAPACITY>::create("main").unwrap();

    {
        let memory = [0; MEMORY_CAPACITY];
        let buffer_manager: BufferManager<BLOCKSIZE, CAPACITY, MEMORY_CAPACITY> =
            BufferManager::init(&memory, &disk);
        let disk_manager = DiskManager::init(&disk);
        let file_system = FileSystem::init(&buffer_manager, &disk_manager).unwrap();
        println!("---- System initialized ----");

        let mut table = Table::new(file_system.create_file("myfile1").unwrap());
        let schema_table = SchemaTable::new(file_system.create_file("schema1").unwrap());

        let schema = Schema {
            schema: vec![
                (String::from("firstname"), DataType::Char(20)),
                (String::from("lastname"), DataType::Char(20)),
                (String::from("male"), DataType::Bool),
                (String::from("age"), DataType::UInt),
                (String::from("details"), DataType::VarChar(255)),
            ],
        };

        let record = Record {
            schema: &schema,
            data: vec![
                Field::Char(Some(b"Firstname".to_vec())),
                Field::Char(Some(b"Lastname".to_vec())),
                Field::Bool(Some(true)),
                Field::UInt(Some(21)),
                Field::VarChar(Some(b"This is some one.".to_vec())),
            ],
        };

        table.insert(record);
        println!(
            "Record: \n{}",
            Record::from_bytes(&schema, table.cursor().next().unwrap())
        );

        table.save();
        schema_table.save_schema(schema.clone());
        schema_table.save();
        println!("---- System closed ----");
    }
    {
        let memory = [0; MEMORY_CAPACITY];
        let buffer_manager: BufferManager<BLOCKSIZE, CAPACITY, MEMORY_CAPACITY> =
            BufferManager::init(&memory, &disk);
        let disk_manager = DiskManager::open(&disk);
        let file_system = FileSystem::open(&buffer_manager, &disk_manager).unwrap();
        let table = Table::new(file_system.open_file("myfile1").unwrap());
        println!("---- System reopened ----");
        let schema_table = SchemaTable::new(file_system.open_file("schema1").unwrap());

        let schema = schema_table.load_schema();
        println!(
            "Record: \n{}",
            Record::from_bytes(&schema, table.cursor().next().unwrap())
        );
        println!("---- System closed ----");
    }
}
