use crate::{
    btree_index::btree::{
        node::{node_header::NodeType, Slot},
        RowAddress,
    },
    buffer_manager::BufferManager,
    disk_manager::DiskManager,
};

fn init<'a, const BLOCKSIZE: usize, const DISK_CAPACITY: usize, const MEMORY_CAPACITY: usize>(
    file_name: &str,
    memory: &'a [u8; MEMORY_CAPACITY],
) -> (
    BufferManager<'a, BLOCKSIZE, DISK_CAPACITY, MEMORY_CAPACITY>,
    DiskManager<BLOCKSIZE, DISK_CAPACITY>,
) {
    let disk = disk::Disk::<BLOCKSIZE, DISK_CAPACITY>::create(file_name).unwrap();
    let buffer_manager: BufferManager<'_, BLOCKSIZE, DISK_CAPACITY, MEMORY_CAPACITY> =
        BufferManager::init(memory, &disk);
    let disk_manager = DiskManager::init(&disk);
    (buffer_manager, disk_manager)
}

use super::{node_header::NodePointer, InsertResult, Node};

fn tree_contains_holes<
    const BLOCKSIZE: usize,
    const CAPACITY: usize,
    const MEMORY_CAPACITY: usize,
>(
    root: &Node<'_, BLOCKSIZE, CAPACITY, MEMORY_CAPACITY>,
) -> bool {
    let buffer_manager = root.buffer_manager;
    let disk_manager = root.disk_manager;
    let mut queue = vec![root.page_number];
    while let Some(node) = queue.pop() {
        let node = Node::from(&buffer_manager, &disk_manager, node);
        match node.node_type() {
            NodeType::Leaf => {
                let holes = node.find_holes();
                if !holes.is_empty() {
                    return true;
                }
            }
            NodeType::Interior => {
                let holes = node.find_holes();
                if !holes.is_empty() {
                    return true;
                }
                for child in node.children() {
                    queue.push(child.page_number);
                }
            }
        }
    }
    false
}

fn create_sample_tree<
    'a,
    const BLOCKSIZE: usize,
    const CAPACITY: usize,
    const MEMORY_CAPACITY: usize,
>(
    disk_manager: &DiskManager<BLOCKSIZE, CAPACITY>,
    buffer_manager: &BufferManager<'a, BLOCKSIZE, CAPACITY, MEMORY_CAPACITY>,
) -> NodePointer {
    let node = Node::new(NodeType::Leaf, &buffer_manager, &disk_manager);
    let node = match node.node_insert(&['t' as u8; 100], RowAddress::new(3333, 8888)) {
        InsertResult::Normal(node) => node,
        _ => unreachable!(),
    };
    let node = match node.node_insert(&['y' as u8; 101], RowAddress::new(1, 22)) {
        InsertResult::Normal(node) => node,
        _ => unreachable!(),
    };
    let node = match node.node_insert(&['r' as u8; 102], RowAddress::new(4, 22)) {
        InsertResult::Normal(node) => node,
        _ => unreachable!(),
    };
    let node = match node.node_insert(&['q' as u8; 103], RowAddress::new(1, 22)) {
        InsertResult::Normal(node) => node,
        _ => unreachable!(),
    };
    let node = match node.node_insert(&['b' as u8; 104], RowAddress::new(53, 22)) {
        InsertResult::Splitted(mid_key, left, right) => {
            let new_node = Node::new(NodeType::Interior, &buffer_manager, &disk_manager);
            new_node.set_right_child(right.page_number);
            match new_node.interior_insert(&mid_key, left.page_number, None) {
                InsertResult::Normal(node) => node,
                _ => unreachable!(),
            }
        }
        _ => unreachable!(),
    };
    node.page_number
}

#[test]
fn shifting_cell() {
    const BLOCK_SIZE: usize = 4096;
    const DISK_CAPACITY: usize = 4096 * 32;
    const MEMORY_CAPACITY: usize = 4096 * 16;

    let memory = [0; MEMORY_CAPACITY];
    let (buffer_manager, disk_manager) =
        init::<BLOCK_SIZE, DISK_CAPACITY, MEMORY_CAPACITY>("shifting_cell", &memory);

    let node = Node::new(NodeType::Leaf, &buffer_manager, &disk_manager);
    let mut node = match node.node_insert(&[1, 2, 3], RowAddress::new(1, 2)) {
        InsertResult::Normal(node) => node,
        _ => unreachable!(),
    };
    let (ptr, size) = node.cell_pointer_and_size(0);
    unsafe {
        node.shift_cell(0, -100);
    }
    assert_eq!(node.cell_pointer_and_size(0), (ptr - 100, size));
    unsafe {
        node.shift_cell(0, 100);
    }
    assert_eq!(node.cell_pointer_and_size(0), (ptr, size));
    assert_eq!(node.row_address_of_cell(0), RowAddress::new(1, 2));
}

#[test]
fn cleaning_holes() {
    const BLOCK_SIZE: usize = 4096;
    const DISK_CAPACITY: usize = 4096 * 32;
    const MEMORY_CAPACITY: usize = 4096 * 16;

    let memory = [0; MEMORY_CAPACITY];
    let (buffer_manager, disk_manager) =
        init::<BLOCK_SIZE, DISK_CAPACITY, MEMORY_CAPACITY>("cleaning_holes", &memory);

    let node = Node::new(NodeType::Leaf, &buffer_manager, &disk_manager);
    let mut node = match node.node_insert(&[1, 2, 3], RowAddress::new(1, 2)) {
        InsertResult::Normal(node) => node,
        _ => unreachable!(),
    };
    let mut node = match node.node_insert(&[4, 5, 6], RowAddress::new(2, 1)) {
        InsertResult::Normal(node) => node,
        _ => unreachable!(),
    };
    unsafe {
        node.shift_cell(1, -100);
        node.set_cell_content_start(node.cell_content_start() - 100);
    }
    let mut node = match node.node_insert(&[5, 5, 6], RowAddress::new(2, 1)) {
        InsertResult::Normal(node) => node,
        _ => unreachable!(),
    };
    let bounds = node.cell_bounds();
    assert_eq!(bounds.len(), 3);

    node.clean_holes();
    assert_eq!(node.key_of_cell(0), &[1, 2, 3]);
    assert_eq!(node.key_of_cell(1), &[4, 5, 6]);
    assert_eq!(node.key_of_cell(2), &[5, 5, 6]);
    assert_eq!(node.row_address_of_cell(0), RowAddress::new(1, 2));
    assert_eq!(node.row_address_of_cell(1), RowAddress::new(2, 1));
    assert_eq!(node.row_address_of_cell(2), RowAddress::new(2, 1));
}

#[test]
fn basic_header() {
    const BLOCK_SIZE: usize = 4096;
    const DISK_CAPACITY: usize = 4096 * 32;
    const MEMORY_CAPACITY: usize = 4096 * 16;

    let memory = [0; MEMORY_CAPACITY];
    let disk = disk::Disk::<BLOCK_SIZE, DISK_CAPACITY>::create("basic_header").unwrap();
    let buffer_manager: BufferManager<'_, BLOCK_SIZE, DISK_CAPACITY, MEMORY_CAPACITY> =
        BufferManager::init(&memory, &disk);
    let disk_manager = DiskManager::init(&disk);

    let mut node = Node::new(NodeType::Leaf, &buffer_manager, &disk_manager);
    assert_eq!(node.node_type(), NodeType::Leaf);
    assert_eq!(node.num_cells(), 0);
    assert_eq!(node.free_size(), 4085);
    assert_eq!(node.cell_content_start(), 4096);
    node.set_num_cells(14);
    assert_eq!(node.num_cells(), 14);
    node.set_cell_content_start(4096 - 14 * 2);
    assert_eq!(node.cell_content_start(), 4096 - 14 * 2);

    node.set_cell_pointer_and_size(0, 4096 - 14 * 2, 2);
    assert_eq!(node.cell_pointer_and_size(0), (4096 - 14 * 2, 2));
    node.set_cell_pointer_and_size(1, 4096 - 14 * 2 - 2, 2);
    assert_eq!(node.cell_pointer_and_size(1), (4096 - 14 * 2 - 2, 2));
}

#[test]
fn insert_cell_pointer() {
    const BLOCK_SIZE: usize = 4096;
    const DISK_CAPACITY: usize = 4096 * 32;
    const MEMORY_CAPACITY: usize = 4096 * 16;

    let memory = [0; MEMORY_CAPACITY];
    let disk = disk::Disk::<BLOCK_SIZE, DISK_CAPACITY>::create("insert_cell_pointer").unwrap();
    let buffer_manager: BufferManager<'_, BLOCK_SIZE, DISK_CAPACITY, MEMORY_CAPACITY> =
        BufferManager::init(&memory, &disk);
    let disk_manager = DiskManager::init(&disk);

    let mut node = Node::new(NodeType::Leaf, &buffer_manager, &disk_manager);
    node.insert_cell_pointer(0, 12, 14);
    assert_eq!(node.cell_pointer_and_size(0), (12, 14));
    assert_eq!(node.num_cells(), 1);

    node.insert_cell_pointer(1, 15, 16);
    assert_eq!(node.cell_pointer_and_size(1), (15, 16));
    assert_eq!(node.num_cells(), 2);

    node.insert_cell_pointer(0, 1521, 14);
    assert_eq!(node.cell_pointer_and_size(0), (1521, 14));
    assert_eq!(node.num_cells(), 3);

    node.insert_cell_pointer(2, 643, 400);
    assert_eq!(node.cell_pointer_and_size(2), (643, 400));
    assert_eq!(node.num_cells(), 4);
}

#[test]
fn insert_and_search_in_interior_node() {
    const BLOCK_SIZE: usize = 4096;
    const DISK_CAPACITY: usize = 4096 * 32;
    const MEMORY_CAPACITY: usize = 4096 * 16;

    let memory = [0; MEMORY_CAPACITY];
    let disk =
        disk::Disk::<BLOCK_SIZE, DISK_CAPACITY>::create("insert_and_search_in_interior_node")
            .unwrap();
    let buffer_manager: BufferManager<'_, BLOCK_SIZE, DISK_CAPACITY, MEMORY_CAPACITY> =
        BufferManager::init(&memory, &disk);
    let disk_manager = DiskManager::init(&disk);

    let keys: Vec<i32> = vec![5, 56, 43, 67, 47, 2, 34, 2345, 235];
    let node = Node::new(NodeType::Interior, &buffer_manager, &disk_manager);
    assert_eq!(node.search(&keys[0].to_be_bytes()), Slot::Hole(0));
    let node = match node.interior_insert(&keys[0].to_be_bytes(), 12, None) {
        InsertResult::Normal(node) => node,
        _ => unreachable!(),
    };
    assert_eq!(node.search(&keys[0].to_be_bytes()), Slot::Cell(0));
    assert_eq!(node.search(&keys[1].to_be_bytes()), Slot::Hole(1));
    let node = match node.interior_insert(&keys[1].to_be_bytes(), 12, None) {
        InsertResult::Normal(node) => node,
        _ => unreachable!(),
    };
    assert_eq!(node.search(&keys[1].to_be_bytes()), Slot::Cell(1));
    assert_eq!(node.search(&keys[2].to_be_bytes()), Slot::Hole(1));
    let node = match node.interior_insert(&keys[2].to_be_bytes(), 12, None) {
        InsertResult::Normal(node) => node,
        _ => unreachable!(),
    };
    assert_eq!(node.search(&keys[2].to_be_bytes()), Slot::Cell(1));
    assert_eq!(node.search(&keys[3].to_be_bytes()), Slot::Hole(3));
    let node = match node.interior_insert(&keys[3].to_be_bytes(), 12, None) {
        InsertResult::Normal(node) => node,
        _ => unreachable!(),
    };
    assert_eq!(node.search(&keys[3].to_be_bytes()), Slot::Cell(3));
    assert_eq!(node.search(&keys[4].to_be_bytes()), Slot::Hole(2));
    let node = match node.interior_insert(&keys[4].to_be_bytes(), 12, None) {
        InsertResult::Normal(node) => node,
        _ => unreachable!(),
    };
    assert_eq!(node.search(&keys[4].to_be_bytes()), Slot::Cell(2));
    assert_eq!(node.search(&keys[5].to_be_bytes()), Slot::Hole(0));
    let node = match node.interior_insert(&keys[5].to_be_bytes(), 12, None) {
        InsertResult::Normal(node) => node,
        _ => unreachable!(),
    };
    assert_eq!(node.search(&keys[5].to_be_bytes()), Slot::Cell(0));

    assert_eq!(node.search(&keys[0].to_be_bytes()), Slot::Cell(1));
    assert_eq!(node.search(&keys[1].to_be_bytes()), Slot::Cell(4));
    assert_eq!(node.search(&keys[2].to_be_bytes()), Slot::Cell(2));
    assert_eq!(node.search(&keys[3].to_be_bytes()), Slot::Cell(5));
    assert_eq!(node.search(&keys[4].to_be_bytes()), Slot::Cell(3));
    assert_eq!(node.search(&keys[5].to_be_bytes()), Slot::Cell(0));
}

#[test]
fn insert_and_search_in_leaf_node() {
    const BLOCK_SIZE: usize = 4096;
    const DISK_CAPACITY: usize = 4096 * 32;
    const MEMORY_CAPACITY: usize = 4096 * 16;

    let memory = [0; MEMORY_CAPACITY];
    let disk =
        disk::Disk::<BLOCK_SIZE, DISK_CAPACITY>::create("insert_and_search_in_leaf_node").unwrap();
    let buffer_manager: BufferManager<'_, BLOCK_SIZE, DISK_CAPACITY, MEMORY_CAPACITY> =
        BufferManager::init(&memory, &disk);
    let disk_manager = DiskManager::init(&disk);

    let keys: Vec<i32> = vec![5, 56, 43, 67, 47, 2, 34, 2345, 235];
    let node = Node::new(NodeType::Leaf, &buffer_manager, &disk_manager);
    assert_eq!(node.search(&keys[0].to_be_bytes()), Slot::Hole(0));
    let node = match node.leaf_insert(&keys[0].to_be_bytes(), RowAddress::new(1, 2), None) {
        InsertResult::Normal(node) => node,
        _ => unreachable!(),
    };
    assert_eq!(node.search(&keys[0].to_be_bytes()), Slot::Cell(0));
    assert_eq!(node.search(&keys[1].to_be_bytes()), Slot::Hole(1));
    let node = match node.leaf_insert(&keys[1].to_be_bytes(), RowAddress::new(3, 4), None) {
        InsertResult::Normal(node) => node,
        _ => unreachable!(),
    };
    assert_eq!(node.search(&keys[1].to_be_bytes()), Slot::Cell(1));
    assert_eq!(node.search(&keys[2].to_be_bytes()), Slot::Hole(1));
    let node = match node.leaf_insert(&keys[2].to_be_bytes(), RowAddress::new(5, 6), None) {
        InsertResult::Normal(node) => node,
        _ => unreachable!(),
    };
    assert_eq!(node.search(&keys[2].to_be_bytes()), Slot::Cell(1));
    assert_eq!(node.search(&keys[3].to_be_bytes()), Slot::Hole(3));
    let node = match node.leaf_insert(&keys[3].to_be_bytes(), RowAddress::new(1, 2), None) {
        InsertResult::Normal(node) => node,
        _ => unreachable!(),
    };
    assert_eq!(node.search(&keys[3].to_be_bytes()), Slot::Cell(3));
    assert_eq!(node.search(&keys[4].to_be_bytes()), Slot::Hole(2));
    let node = match node.leaf_insert(&keys[4].to_be_bytes(), RowAddress::new(1, 2), None) {
        InsertResult::Normal(node) => node,
        _ => unreachable!(),
    };
    assert_eq!(node.search(&keys[4].to_be_bytes()), Slot::Cell(2));
    assert_eq!(node.search(&keys[5].to_be_bytes()), Slot::Hole(0));
    let node = match node.leaf_insert(&keys[5].to_be_bytes(), RowAddress::new(12, 423), None) {
        InsertResult::Normal(node) => node,
        _ => unreachable!(),
    };
    assert_eq!(node.search(&keys[5].to_be_bytes()), Slot::Cell(0));

    println!("{:#?}", node);
    assert_eq!(node.search(&keys[0].to_be_bytes()), Slot::Cell(1));
    assert_eq!(node.search(&keys[1].to_be_bytes()), Slot::Cell(4));
    assert_eq!(node.search(&keys[2].to_be_bytes()), Slot::Cell(2));
    assert_eq!(node.search(&keys[3].to_be_bytes()), Slot::Cell(5));
    assert_eq!(node.search(&keys[4].to_be_bytes()), Slot::Cell(3));
    assert_eq!(node.search(&keys[5].to_be_bytes()), Slot::Cell(0));
}

#[test]
fn basic_interior_insert() {
    const BLOCK_SIZE: usize = 4096;
    const DISK_CAPACITY: usize = 4096 * 32;
    const MEMORY_CAPACITY: usize = 4096 * 16;

    let memory = [0; MEMORY_CAPACITY];
    let disk = disk::Disk::<BLOCK_SIZE, DISK_CAPACITY>::create("basic_interior_insert").unwrap();
    let buffer_manager: BufferManager<'_, BLOCK_SIZE, DISK_CAPACITY, MEMORY_CAPACITY> =
        BufferManager::init(&memory, &disk);
    let disk_manager = DiskManager::init(&disk);

    let node = Node::new(NodeType::Interior, &buffer_manager, &disk_manager);
    let node =
        if let InsertResult::Normal(node) = node.interior_insert(&[1, 2, 3, 4, 5, 6], 112, None) {
            node
        } else {
            unreachable!()
        };
    assert_eq!(node.num_cells(), 1);
    assert_eq!(node.key_of_cell(0), &[1, 2, 3, 4, 5, 6]);
    assert_eq!(node.child_pointer_of_cell(0), 112);

    let node =
        if let InsertResult::Normal(node) = node.interior_insert(&[3, 4, 5, 6, 7, 8], 12, None) {
            node
        } else {
            unreachable!()
        };
    assert_eq!(node.num_cells(), 2);
    assert_eq!(node.key_of_cell(0), &[1, 2, 3, 4, 5, 6]);
    assert_eq!(node.key_of_cell(1), &[3, 4, 5, 6, 7, 8]);
    assert_eq!(node.child_pointer_of_cell(0), 112);
    assert_eq!(node.child_pointer_of_cell(1), 12);
}

#[test]
fn basic_leaf_insert() {
    const BLOCK_SIZE: usize = 4096;
    const DISK_CAPACITY: usize = 4096 * 32;
    const MEMORY_CAPACITY: usize = 4096 * 16;

    let memory = [0; MEMORY_CAPACITY];
    let disk = disk::Disk::<BLOCK_SIZE, DISK_CAPACITY>::create("basic_leaf_insert").unwrap();
    let buffer_manager: BufferManager<'_, BLOCK_SIZE, DISK_CAPACITY, MEMORY_CAPACITY> =
        BufferManager::init(&memory, &disk);
    let disk_manager = DiskManager::init(&disk);

    let node = Node::new(NodeType::Leaf, &buffer_manager, &disk_manager);
    let node = if let InsertResult::Normal(node) =
        node.leaf_insert(&[1, 2, 3, 4, 5, 6], RowAddress::new(3333, 8888), None)
    {
        node
    } else {
        unreachable!()
    };
    assert_eq!(node.num_cells(), 1);
    assert_eq!(node.key_of_cell(0), &[1, 2, 3, 4, 5, 6]);
    assert_eq!(node.row_address_of_cell(0), RowAddress::new(3333, 8888));

    let node = if let InsertResult::Normal(node) =
        node.leaf_insert(&[3, 4, 5, 6, 7, 8], RowAddress::new(1234, 5678), None)
    {
        node
    } else {
        unreachable!()
    };
    assert_eq!(node.num_cells(), 2);
    assert_eq!(node.key_of_cell(0), &[1, 2, 3, 4, 5, 6]);
    assert_eq!(node.key_of_cell(1), &[3, 4, 5, 6, 7, 8]);
    assert_eq!(node.row_address_of_cell(0), RowAddress::new(3333, 8888));
    assert_eq!(node.row_address_of_cell(1), RowAddress::new(1234, 5678));
}

// #[test]
// fn find_holes() {
//     const BLOCK_SIZE: usize = 4096;
//     const DISK_CAPACITY: usize = 4096 * 32;
//     const MEMORY_CAPACITY: usize = 4096 * 16;

//     let memory = [0; MEMORY_CAPACITY];
//     let disk = disk::Disk::<BLOCK_SIZE, DISK_CAPACITY>::create("find_holes").unwrap();
//     let buffer_manager: BufferManager<'_, BLOCK_SIZE, DISK_CAPACITY, MEMORY_CAPACITY> =
//         BufferManager::init(&memory, &disk);
//     let disk_manager = DiskManager::init(&disk);

//     let mut node = Node::new(&buffer_manager, &disk_manager);
//     node.set_node_type(NodeType::Leaf);
//     let data = vec![
//         (12, [1, 2, 3]),
//         (222, [1, 2, 3]),
//         (11, [1, 2, 3]),
//         (88, [1, 2, 3]),
//         (8, [1, 2, 3]),
//     ];
//     for datum in &data[0..4] {
//         node = match node.node_insert(datum.0, &datum.1) {
//             InsertResult::Normal(node) => node,
//             _ => unreachable!(),
//         };
//     }
//     match node.node_insert(data[4].0, &data[4].1) {
//         InsertResult::Splitted(key, left, right) => {
//             println!("{:#?}", left.find_holes());
//         }
//         _ => unreachable!(),
//     };
//     panic!()
// }

// #[test]
// fn split_logic() {
//     const BLOCK_SIZE: usize = 4096;
//     const DISK_CAPACITY: usize = 4096 * 32;
//     const MEMORY_CAPACITY: usize = 4096 * 16;

//     let memory = [0; MEMORY_CAPACITY];
//     let disk = disk::Disk::<BLOCK_SIZE, DISK_CAPACITY>::create("find_holes").unwrap();
//     let buffer_manager: BufferManager<'_, BLOCK_SIZE, DISK_CAPACITY, MEMORY_CAPACITY> =
//         BufferManager::init(&memory, &disk);
//     let disk_manager = DiskManager::init(&disk);

//     let mut node = Node::new(&buffer_manager, &disk_manager);
//     node.set_node_type(NodeType::Leaf);
//     let data = vec![
//         (12, [1, 2, 3]),
//         (222, [1, 2, 3]),
//         (11, [1, 2, 3]),
//         (88, [1, 2, 3]),
//         (8, [1, 2, 3]),
//         (1, [1, 2, 3]),
//         (111, [1, 2, 3]),
//         (333, [1, 2, 3]),
//         (7777, [1, 2, 3]),
//         (23, [1, 2, 3]),
//         (56, [1, 2, 3]),
//         (12521, [1, 2, 3]),
//     ];
//     for datum in &data[0..4] {
//         node = match node.node_insert(datum.0, &datum.1) {
//             InsertResult::Normal(node) => node,
//             _ => unreachable!(),
//         };
//     }
//     node = match node.node_insert(data[4].0, &data[4].1) {
//         InsertResult::Splitted(key, left, right) => {
//             let mut node = Node::new(&buffer_manager, &disk_manager);
//             node.set_node_type(NodeType::Interior);
//             node.set_right_child(right.page_number);
//             let node = match node.interior_insert(key, left.page_number) {
//                 InsertResult::Normal(node) => node,
//                 _ => unreachable!(),
//             };
//             node
//         }
//         _ => unreachable!(),
//     };
//     for datum in &data[5..] {
//         node = match node.node_insert(datum.0, &datum.1) {
//             InsertResult::Normal(node) => {
//                 println!("{:#?}", node);
//                 node
//             }
//             _ => unreachable!(),
//         };
//     }
//     panic!();
// }

#[test]
fn leaf_insert_split() {
    const BLOCK_SIZE: usize = 512;
    const DISK_CAPACITY: usize = 512 * 32;
    const MEMORY_CAPACITY: usize = 512 * 16;

    let memory = [0; MEMORY_CAPACITY];
    let disk = disk::Disk::<BLOCK_SIZE, DISK_CAPACITY>::create("leaf_insert_split").unwrap();
    let buffer_manager: BufferManager<'_, BLOCK_SIZE, DISK_CAPACITY, MEMORY_CAPACITY> =
        BufferManager::init(&memory, &disk);
    let disk_manager = DiskManager::init(&disk);

    let node = Node::new(NodeType::Leaf, &buffer_manager, &disk_manager);
    let node = match node.leaf_insert(&[1; 100], RowAddress::new(3333, 8888), None) {
        InsertResult::Normal(node) => node,
        _ => unreachable!(),
    };
    let node = match node.leaf_insert(&[2; 101], RowAddress::new(1, 22), None) {
        InsertResult::Normal(node) => node,
        _ => unreachable!(),
    };
    let node = match node.leaf_insert(&[3; 102], RowAddress::new(4, 22), None) {
        InsertResult::Normal(node) => node,
        _ => unreachable!(),
    };
    let node = match node.leaf_insert(&[4; 103], RowAddress::new(1, 22), None) {
        InsertResult::Normal(node) => node,
        _ => unreachable!(),
    };
    match node.leaf_insert(&[5; 104], RowAddress::new(53, 22), None) {
        InsertResult::Splitted(key, left, right) => {
            assert_eq!(left.node_type(), NodeType::Leaf);
            assert_eq!(left.num_cells(), 2);
            assert_eq!(&left.key_of_cell(0), &[1; 100]);
            assert_eq!(left.row_address_of_cell(0), RowAddress::new(3333, 8888));
            assert_eq!(&left.key_of_cell(1), &[2; 101]);
            assert_eq!(left.row_address_of_cell(1), RowAddress::new(1, 22));

            assert_eq!(right.num_cells(), 3);
            assert_eq!(&right.key_of_cell(0), &[3; 102]);
            assert_eq!(&right.key_of_cell(1), &[4; 103]);
            assert_eq!(&right.key_of_cell(2), &[5; 104]);

            assert_eq!(key, &[3; 102]);
        }
        _ => unreachable!(),
    };
}

#[test]
fn interior_insert_split() {
    const BLOCK_SIZE: usize = 512;
    const DISK_CAPACITY: usize = 512 * 32;
    const MEMORY_CAPACITY: usize = 512 * 16;

    let memory = [0; MEMORY_CAPACITY];
    let disk = disk::Disk::<BLOCK_SIZE, DISK_CAPACITY>::create("interior_insert_split").unwrap();
    let buffer_manager: BufferManager<'_, BLOCK_SIZE, DISK_CAPACITY, MEMORY_CAPACITY> =
        BufferManager::init(&memory, &disk);
    let disk_manager = DiskManager::init(&disk);

    let node = Node::new(NodeType::Interior, &buffer_manager, &disk_manager);
    let node = match node.interior_insert(&[1; 100], 2, None) {
        InsertResult::Normal(node) => node,
        _ => unreachable!(),
    };
    let node = match node.interior_insert(&[2; 101], 3, None) {
        InsertResult::Normal(node) => node,
        _ => unreachable!(),
    };
    let node = match node.interior_insert(&[3; 102], 4, None) {
        InsertResult::Normal(node) => node,
        _ => unreachable!(),
    };
    let node = match node.interior_insert(&[4; 103], 5, None) {
        InsertResult::Normal(node) => node,
        _ => unreachable!(),
    };
    // match node.interior_insert(&[5; 104], 6, None) {
    //     InsertResult::Splitted(key, left, right) => {
    //         assert_eq!(left.node_type(), NodeType::Interior);
    //         assert_eq!(left.num_cells(), 1);
    //         assert_eq!(&left.key_of_cell(0), &[1; 100]);
    //         assert_eq!(left.child_pointer_of_cell(0), 2);
    //         assert_eq!(&left.key_of_cell(1), &[2; 101]);
    //         assert_eq!(left.child_pointer_of_cell(1), 3);

    //         assert_eq!(right.num_cells(), 3);
    //         assert_eq!(&right.key_of_cell(0), &[3; 102]);
    //         assert_eq!(right.child_pointer_of_cell(0), 4);
    //         assert_eq!(&right.key_of_cell(1), &[4; 103]);
    //         assert_eq!(right.child_pointer_of_cell(1), 5);
    //         assert_eq!(&right.key_of_cell(2), &[5; 104]);
    //         assert_eq!(right.child_pointer_of_cell(2), 6);

    //         assert_eq!(key, &[3; 102]);
    //     }
    //     _ => unreachable!(),
    // };
}

fn handle_normal_insert<
    'a,
    const BLOCK_SIZE: usize,
    const DISK_CAPACITY: usize,
    const MEMORY_CAPACITY: usize,
>(
    rs: InsertResult<'a, BLOCK_SIZE, DISK_CAPACITY, MEMORY_CAPACITY>,
) -> Node<'a, BLOCK_SIZE, DISK_CAPACITY, MEMORY_CAPACITY> {
    match rs {
        InsertResult::Normal(node) => node,
        _ => unreachable!(),
    }
}

fn handle_split_insert<
    'a,
    const BLOCK_SIZE: usize,
    const DISK_CAPACITY: usize,
    const MEMORY_CAPACITY: usize,
>(
    rs: InsertResult<'a, BLOCK_SIZE, DISK_CAPACITY, MEMORY_CAPACITY>,
) -> Node<'a, BLOCK_SIZE, DISK_CAPACITY, MEMORY_CAPACITY> {
    match rs {
        InsertResult::Splitted(key, left, right) => {
            let buffer_manager = left.buffer_manager;
            let disk_manager = left.disk_manager;
            let new_root = Node::new(NodeType::Interior, &buffer_manager, &disk_manager);
            let new_root = match new_root.interior_insert(&key, left.page_number, None) {
                InsertResult::Normal(node) => node,
                _ => unreachable!(),
            };
            new_root.set_right_child(right.page_number);
            new_root
        }
        _ => unreachable!(),
    }
}

#[test]
fn node_insert_split() {
    env_logger::try_init().unwrap_or(());
    const BLOCK_SIZE: usize = 512;
    const DISK_CAPACITY: usize = 512 * 32;
    const MEMORY_CAPACITY: usize = 512 * 16;

    let memory = [0; MEMORY_CAPACITY];
    let disk = disk::Disk::<BLOCK_SIZE, DISK_CAPACITY>::create("node_insert_split").unwrap();
    let buffer_manager: BufferManager<'_, BLOCK_SIZE, DISK_CAPACITY, MEMORY_CAPACITY> =
        BufferManager::init(&memory, &disk);
    let disk_manager = DiskManager::init(&disk);

    let node_ptr = create_sample_tree(&disk_manager, &buffer_manager);
    let root = Node::from(&buffer_manager, &disk_manager, node_ptr);
    assert_eq!(root.num_cells(), 1);
    let root = handle_normal_insert(root.node_insert(&['a' as u8; 11], RowAddress::new(111, 222)));
    let root = handle_normal_insert(root.node_insert(&['b' as u8; 99], RowAddress::new(112, 23)));
    let root = handle_normal_insert(root.node_insert(&['c' as u8; 98], RowAddress::new(111, 222)));
    let root = handle_normal_insert(root.node_insert(&['w' as u8; 97], RowAddress::new(111, 222)));
    let root = handle_normal_insert(root.node_insert(&['d' as u8; 97], RowAddress::new(111, 222)));
    let root = handle_normal_insert(root.node_insert(&['v' as u8; 95], RowAddress::new(111, 222)));
    let root = handle_normal_insert(root.node_insert(&['d' as u8; 110], RowAddress::new(111, 222)));
    let root = handle_normal_insert(root.node_insert(&['k' as u8; 111], RowAddress::new(111, 222)));
    let root = handle_split_insert(root.node_insert(&['z' as u8; 112], RowAddress::new(111, 222)));
    let root = handle_normal_insert(root.node_insert(&['f' as u8; 108], RowAddress::new(111, 222)));
    let root = handle_normal_insert(root.node_insert(&['g' as u8; 108], RowAddress::new(111, 222)));
    let root = handle_normal_insert(root.node_insert(&['g' as u8; 109], RowAddress::new(111, 222)));
    let root = handle_normal_insert(root.node_insert(&['g' as u8; 106], RowAddress::new(111, 222)));
    let root = handle_normal_insert(root.node_insert(&['g' as u8; 109], RowAddress::new(111, 222)));
    println!("{:#?}", root);
    // let root = match root.node_insert(&[1; 120], RowAddress::new(111, 222)) {
    //     InsertResult::Splitted(mid, left, right) => {
    //         let new_root = Node::new(NodeType::Interior, &buffer_manager, &disk_manager);
    //         let new_root = match new_root.interior_insert(&mid, left.page_number, None) {
    //             InsertResult::Normal(node) => node,
    //             _ => unreachable!(),
    //         };
    //         new_root.set_right_child(right.page_number);
    //         new_root
    //     }
    //     InsertResult::Normal(node) => node,
    //     _ => unreachable!(),
    // };
    // println!("{:#?}", root);
}
