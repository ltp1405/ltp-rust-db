use file_system::{unordered_file::Cell, FileSystem};

#[test]
fn general() {
    const CAPACITY: usize = 512 * 400;
    let mut file_system = FileSystem::<512, CAPACITY>::create("test.db").unwrap();
    let mut file1 = file_system.create_file("file1").unwrap();
    let mut file2 = file_system.create_file("file2").unwrap();

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

    let mut cells1 = Vec::new();
    let mut cells2 = Vec::new();

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
}
