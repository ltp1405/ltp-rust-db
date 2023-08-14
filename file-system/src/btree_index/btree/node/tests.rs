use crate::{
    btree_index::btree::{
        node::{node_header::NodeType, Slot},
        RowAddress,
    },
    buffer_manager::BufferManager,
    disk_manager::DiskManager,
};

use super::{InsertResult, Node};

fn create_leaf_node_samples<
    'a,
    const BLOCKSIZE: usize,
    const CAPACITY: usize,
    const MEMORY_CAPACITY: usize,
>(
    disk_manager: &DiskManager<BLOCKSIZE, CAPACITY>,
    buffer_manager: &BufferManager<'a, BLOCKSIZE, CAPACITY, MEMORY_CAPACITY>,
    sample_size: usize,
) -> Vec<u32> {
    let mut children = Vec::new();
    for i in 0..sample_size {
        let mut node = Node::new(NodeType::Leaf, buffer_manager, disk_manager);
        let node = node.leaf_insert(&i.to_be_bytes(), RowAddress::new(0, 0), None);
        match node {
            InsertResult::Normal(node) => {
                println!("{:#?}", node);
                children.push(node.page_number);
            }
            _ => unreachable!(),
        }
    }
    children
}

mod set_and_get {
    use crate::{
        btree_index::btree::node::{node_header::NodeType, Node},
        buffer_manager::BufferManager,
        disk_manager::DiskManager,
    };

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

    println!("{:#?}", node);
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

mod insert_without_splitting {
    use crate::{
        btree_index::btree::{
            node::{node_header::NodeType, InsertResult, Node},
            RowAddress,
        },
        buffer_manager::BufferManager,
        disk_manager::DiskManager,
    };

    #[test]
    fn basic_interior_insert() {
        const BLOCK_SIZE: usize = 4096;
        const DISK_CAPACITY: usize = 4096 * 32;
        const MEMORY_CAPACITY: usize = 4096 * 16;

        let memory = [0; MEMORY_CAPACITY];
        let disk =
            disk::Disk::<BLOCK_SIZE, DISK_CAPACITY>::create("basic_interior_insert").unwrap();
        let buffer_manager: BufferManager<'_, BLOCK_SIZE, DISK_CAPACITY, MEMORY_CAPACITY> =
            BufferManager::init(&memory, &disk);
        let disk_manager = DiskManager::init(&disk);

        let node = Node::new(NodeType::Interior, &buffer_manager, &disk_manager);
        let node = if let InsertResult::Normal(node) =
            node.interior_insert(&[1, 2, 3, 4, 5, 6], 112, None)
        {
            node
        } else {
            unreachable!()
        };
        assert_eq!(node.num_cells(), 1);
        assert_eq!(node.key_of_cell(0), &[1, 2, 3, 4, 5, 6]);
        assert_eq!(node.child_pointer_of_cell(0), 112);

        let node = if let InsertResult::Normal(node) =
            node.interior_insert(&[3, 4, 5, 6, 7, 8], 12, None)
        {
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
    // fn leaf_insert_cell() {
    //     const BLOCK_SIZE: usize = 4096;
    //     const DISK_CAPACITY: usize = 4096 * 32;
    //     const MEMORY_CAPACITY: usize = 4096 * 16;

    //     let memory = [0; MEMORY_CAPACITY];
    //     let disk = disk::Disk::<BLOCK_SIZE, DISK_CAPACITY>::create("leaf_insert_cell").unwrap();
    //     let buffer_manager: BufferManager<'_, BLOCK_SIZE, DISK_CAPACITY, MEMORY_CAPACITY> =
    //         BufferManager::init(&memory, &disk);
    //     let disk_manager = DiskManager::init(&disk);

    //     let mut node = Node::new(&buffer_manager, &disk_manager);
    //     let node =
    //         if let InsertResult::Normal(node) = node.leaf_insert(22, &[1, 2, 3, 4, 5, 6], None) {
    //             node
    //         } else {
    //             unreachable!()
    //         };
    //     let node = if let InsertResult::Normal(node) = node.leaf_insert(12, &[1, 2, 3], None) {
    //         node
    //     } else {
    //         unreachable!()
    //     };
    //     let node = if let InsertResult::Normal(node) = node.leaf_insert(124, &[1, 2, 5, 6], None) {
    //         node
    //     } else {
    //         unreachable!()
    //     };
    //     let cell = node.cell_at(0);
    //     assert_eq!(cell.kept_payload(), &[1, 2, 3],);
    //     let cell = node.cell_at(1);
    //     assert_eq!(cell.kept_payload(), &[1, 2, 3, 4, 5, 6]);
    //     for i in 0..node.num_cells() - 1 {
    //         let lo = node.cell_at(i).key();
    //         let hi = node.cell_at(i + 1).key();
    //         assert!(lo < hi, "Key should be sorted: {} > {}", lo, hi);
    //     }
    // }

    // #[test]
    // fn interior_insert() {
    //     const BLOCK_SIZE: usize = 4096;
    //     const DISK_CAPACITY: usize = 4096 * 32;
    //     const MEMORY_CAPACITY: usize = 4096 * 16;

    //     let memory = [0; MEMORY_CAPACITY];
    //     let disk = disk::Disk::<BLOCK_SIZE, DISK_CAPACITY>::create("interior_insert").unwrap();
    //     let buffer_manager: BufferManager<'_, BLOCK_SIZE, DISK_CAPACITY, MEMORY_CAPACITY> =
    //         BufferManager::init(&memory, &disk);
    //     let disk_manager = DiskManager::init(&disk);

    //     let node = Node::new(&buffer_manager, &disk_manager);
    //     node.set_right_child(4);
    //     assert_eq!(node.right_child(), 4);
    //     let node = match node.interior_insert(12, 42) {
    //         InsertResult::Normal(node) => node,
    //         _ => unreachable!(),
    //     };
    //     let node = match node.interior_insert(42, 143) {
    //         InsertResult::Normal(node) => node,
    //         _ => unreachable!(),
    //     };
    //     assert_eq!(node.right_child(), 4);
    // }
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

// #[test]
// fn leaf_insert_split() {
//     const BLOCK_SIZE: usize = 4096;
//     const DISK_CAPACITY: usize = 4096 * 32;
//     const MEMORY_CAPACITY: usize = 4096 * 16;

//     let memory = [0; MEMORY_CAPACITY];
//     let disk = disk::Disk::<BLOCK_SIZE, DISK_CAPACITY>::create("leaf_insert_split").unwrap();
//     let buffer_manager: BufferManager<'_, BLOCK_SIZE, DISK_CAPACITY, MEMORY_CAPACITY> =
//         BufferManager::init(&memory, &disk);
//     let disk_manager = DiskManager::init(&disk);

//     let node = Node::new(&buffer_manager, &disk_manager);
//     let node = match node.leaf_insert(533, &[2; 50], None) {
//         InsertResult::Normal(node) => node,
//         _ => unreachable!(),
//     };
//     let node = match node.leaf_insert(22, &[1; 12], None) {
//         InsertResult::Normal(node) => node,
//         _ => unreachable!(),
//     };
//     let node = match node.leaf_insert(12, &[9; 3], None) {
//         InsertResult::Normal(node) => node,
//         _ => unreachable!(),
//     };
//     let node = match node.leaf_insert(124, &[1, 2, 5, 6], None) {
//         InsertResult::Normal(node) => node,
//         _ => unreachable!(),
//     };
//     let node = match node.leaf_insert(5, &[4; 15], None) {
//         InsertResult::Splitted(k, l, r) => {
//             let mut node = Node::new(&buffer_manager, &disk_manager);
//             node.set_node_type(NodeType::Interior);
//             node.set_right_child(r.page_number);

//             let page = node.page();
//             let node = match node.interior_insert(k, l.page_number) {
//                 InsertResult::Normal(node) => node,
//                 _ => unreachable!(),
//             };
//             println!("{:?}", node);
//             panic!();
//             node
//         }
//         _ => unreachable!(),
//     };
// }
