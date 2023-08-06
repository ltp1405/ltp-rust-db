use disk::Disk;
use file_system::{
    buffer_manager::BufferManager, disk_manager::DiskManager, unordered_file::Cell,
    FileSystem,
};

#[test]
fn general() {
    const BLOCKSIZE: usize = 512;
    const CAPACITY: usize = BLOCKSIZE * 512;
    const MEMORY_CAPACITY: usize = 512 * 32;
    let disk = Disk::create("test.db").unwrap();
    let disk_manager = DiskManager::init(&disk);

    {
        let memory = [0; MEMORY_CAPACITY];
        let buffer_manager = BufferManager::init(&memory, &disk);
        let file_system = FileSystem::<BLOCKSIZE, CAPACITY, MEMORY_CAPACITY>::init(
            &buffer_manager,
            &disk_manager,
        )
        .unwrap();
        let file1 = file_system.create_file("file1").unwrap();

        let cells = vec![
            Cell::new([0x1; 17].to_vec()),
            Cell::new([0x1; 17].to_vec()),
            Cell::new([0x2; 51].to_vec()),
            Cell::new([0x1; 17].to_vec()),
            Cell::new([0x2; 51].to_vec()),
            Cell::new([0x1; 17].to_vec()),
            // Cell::new([0x2; 51].to_vec()),
            // Cell::new([0x6; 117].to_vec()),
            // Cell::new([0x4; 246].to_vec()),
            // Cell::new([0xe; 123].to_vec()),
            // Cell::new([0x5; 410].to_vec()),
            // Cell::new([0x3; 100].to_vec()),
            // Cell::new([0x4; 204].to_vec()),
            // Cell::new([0xe; 123].to_vec()),
            // Cell::new([0x5; 400].to_vec()),
            // Cell::new([0x3; 105].to_vec()),
            // Cell::new([0x4; 200].to_vec()),
            // Cell::new([0xe; 123].to_vec()),
            // Cell::new([0x5; 400].to_vec()),
            // Cell::new([0x2; 51].to_vec()),
            // Cell::new([0x3; 106].to_vec()),
            // Cell::new([0x4; 200].to_vec()),
            // Cell::new([0xe; 123].to_vec()),
            // Cell::new([0x5; 400].to_vec()),
        ];

        let mut cells1 = Vec::new();
        let mut cells2 = Vec::new();

        let file2 = file_system.create_file("file2").unwrap();

        for cell in cells.clone().into_iter().filter(|_| rand::random()) {
            cells1.push(cell.clone());
            file1.insert(cell);
        }

        for cell in cells.clone().into_iter().filter(|_| rand::random()) {
            cells2.push(cell.clone());
            file2.insert(cell);
        }

        for cell in cells.clone().into_iter().filter(|_| rand::random()) {
            cells1.push(cell.clone());
            file1.insert(cell);
        }

        for cell in cells.clone().into_iter().filter(|_| rand::random()) {
            cells2.push(cell.clone());
            file2.insert(cell);
        }

        for (i, cell) in file1.cursor().enumerate() {
            assert_eq!(cell, cells1[i]);
        }

        for (i, cell) in file2.cursor().enumerate() {
            assert_eq!(cell, cells2[i]);
        }
        file_system.save_files_table();
    }
    {
        let memory = [0; MEMORY_CAPACITY];
        let buffer_manager = BufferManager::init(&memory, &disk);
        let file_system = FileSystem::<BLOCKSIZE, CAPACITY, MEMORY_CAPACITY>::init(
            &buffer_manager,
            &disk_manager,
        )
        .unwrap();
        let file1 = file_system.open_file("file1").unwrap();
        let file2 = file_system.open_file("file2").unwrap();

        let mut cells1 = Vec::new();
        let mut cells2 = Vec::new();

        for cell in file1.cursor() {
            cells1.push(cell);
        }

        for cell in file2.cursor() {
            cells2.push(cell);
        }

        assert_eq!(cells1, cells2);
    }
}
