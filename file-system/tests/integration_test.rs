use disk::Disk;
use file_system::{
    buffer_manager::BufferManager, disk_manager::DiskManager, unordered_file::Cell, FileSystem,
};

#[test]
fn no_replacement() {
    const BLOCKSIZE: usize = 512;
    const CAPACITY: usize = BLOCKSIZE * 64;
    const MEMORY_CAPACITY: usize = BLOCKSIZE * 32;
    let disk = Disk::create("no_replacement").unwrap();
    let disk_manager = DiskManager::init(&disk);
    let cells = vec![
        Cell::new([0x1; 17].to_vec()),
        Cell::new([0x1; 17].to_vec()),
        Cell::new([0x2; 51].to_vec()),
        Cell::new([0x1; 17].to_vec()),
        Cell::new([0x2; 51].to_vec()),
        Cell::new([0x1; 17].to_vec()),
        Cell::new([0x2; 51].to_vec()),
        Cell::new([0x6; 117].to_vec()),
        Cell::new([0x4; 246].to_vec()),
        Cell::new([0xe; 123].to_vec()),
        Cell::new([0x5; 410].to_vec()),
        Cell::new([0x3; 100].to_vec()),
        Cell::new([0x4; 204].to_vec()),
        Cell::new([0xe; 123].to_vec()),
        Cell::new([0x5; 400].to_vec()),
        Cell::new([0x3; 105].to_vec()),
        Cell::new([0x4; 200].to_vec()),
        Cell::new([0xe; 123].to_vec()),
        Cell::new([0x5; 400].to_vec()),
        Cell::new([0x2; 51].to_vec()),
        Cell::new([0x3; 106].to_vec()),
        Cell::new([0x4; 200].to_vec()),
        Cell::new([0xe; 123].to_vec()),
        Cell::new([0x5; 400].to_vec()),
    ];

    {
        let memory = [0; MEMORY_CAPACITY];
        let buffer_manager = BufferManager::init(&memory, &disk);
        let file_system = FileSystem::<BLOCKSIZE, CAPACITY, MEMORY_CAPACITY>::init(
            &buffer_manager,
            &disk_manager,
        )
        .unwrap();
        let file1 = file_system.create_file("file1").unwrap();
        let file2 = file_system.create_file("file2").unwrap();
        for cell in cells.clone().iter() {
            file1.insert(cell.clone());
        }
        for cell in cells.iter() {
            file2.insert(cell.clone());
        }

        file_system.save_files_table();
        file1.save();
        file2.save();
    }
    {
        let memory = [0; MEMORY_CAPACITY];
        let buffer_manager = BufferManager::init(&memory, &disk);
        let file_system = FileSystem::<BLOCKSIZE, CAPACITY, MEMORY_CAPACITY>::open(
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

        assert_eq!(cells1, cells);
        assert_eq!(cells2, cells);
    }
}

#[test]
fn need_replacement() {
    if let Ok(_) = env_logger::try_init() {
        println!("Logger initialized");
    }
    const BLOCKSIZE: usize = 512;
    const CAPACITY: usize = BLOCKSIZE * 512 * 4;
    const MEMORY_CAPACITY: usize = 512 * 32;
    let disk = Disk::create("need_replacement").unwrap();
    let disk_manager = DiskManager::init(&disk);
    let mut cells = vec![];
    let chunk = vec![
        Cell::new([0x1; 17].to_vec()),
        Cell::new([0x1; 17].to_vec()),
        Cell::new([0x2; 51].to_vec()),
        Cell::new([0x1; 17].to_vec()),
        Cell::new([0x2; 51].to_vec()),
        Cell::new([0x1; 17].to_vec()),
        Cell::new([0x2; 51].to_vec()),
        Cell::new([0x6; 117].to_vec()),
        Cell::new([0x4; 246].to_vec()),
        Cell::new([0xe; 123].to_vec()),
        Cell::new([0x5; 410].to_vec()),
        Cell::new([0x3; 100].to_vec()),
        Cell::new([0x4; 204].to_vec()),
        Cell::new([0xe; 123].to_vec()),
        Cell::new([0x5; 400].to_vec()),
        Cell::new([0x3; 105].to_vec()),
        Cell::new([0x4; 200].to_vec()),
        Cell::new([0xe; 123].to_vec()),
        Cell::new([0x5; 400].to_vec()),
        Cell::new([0x2; 51].to_vec()),
        Cell::new([0x3; 106].to_vec()),
        Cell::new([0x4; 200].to_vec()),
        Cell::new([0xe; 123].to_vec()),
        Cell::new([0x5; 400].to_vec()),
    ];
    for _ in 0..100 {
        cells.extend(chunk.clone());
    }

    {
        let memory = [0; MEMORY_CAPACITY];
        let buffer_manager = BufferManager::init(&memory, &disk);
        let file_system = FileSystem::<BLOCKSIZE, CAPACITY, MEMORY_CAPACITY>::init(
            &buffer_manager,
            &disk_manager,
        )
        .unwrap();
        let file1 = file_system.create_file("file1").unwrap();
        let file2 = file_system.create_file("file2").unwrap();
        for cell in cells.clone().iter() {
            file1.insert(cell.clone());
        }
        for cell in cells.iter() {
            file2.insert(cell.clone());
        }

        file_system.save_files_table();
        file1.save();
        file2.save();
    }
    {
        let memory = [0; MEMORY_CAPACITY];
        let buffer_manager = BufferManager::init(&memory, &disk);
        let file_system = FileSystem::<BLOCKSIZE, CAPACITY, MEMORY_CAPACITY>::open(
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

        for (i, cell) in cells1.iter().enumerate() {
            assert_eq!(cell, &cells[i]);
        }
        for (i, cell) in cells2.iter().enumerate() {
            assert_eq!(cell, &cells[i]);
        }
    }
}
