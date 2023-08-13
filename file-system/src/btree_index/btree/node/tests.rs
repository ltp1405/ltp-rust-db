use crate::{
    btree_index::btree::node::node_header::NodeType, buffer_manager::BufferManager,
    disk_manager::DiskManager,
};

use super::Node;

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
        let node = node.leaf_insert(i as u32, &[1, 2, 3], None);
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

mod insert_without_splitting {
    use crate::{
        btree_index::btree::node::{
            node_header::NodeType, tests::create_leaf_node_samples, InsertResult, Node,
        },
        buffer_manager::BufferManager,
        disk_manager::DiskManager,
    };

    #[test]
    fn single_interior_insert() {
        const BLOCK_SIZE: usize = 4096;
        const DISK_CAPACITY: usize = 4096 * 32;
        const MEMORY_CAPACITY: usize = 4096 * 16;

        let memory = [0; MEMORY_CAPACITY];
        let disk =
            disk::Disk::<BLOCK_SIZE, DISK_CAPACITY>::create("single_interior_insert").unwrap();
        let buffer_manager: BufferManager<'_, BLOCK_SIZE, DISK_CAPACITY, MEMORY_CAPACITY> =
            BufferManager::init(&memory, &disk);
        let disk_manager = DiskManager::init(&disk);

        let children = create_leaf_node_samples(&disk_manager, &buffer_manager, 5);
        let mut node = Node::new(NodeType::Interior, &buffer_manager, &disk_manager);
        let node = if let InsertResult::Normal(node) = node.interior_insert(22, children[0]) {
            node
        } else {
            unreachable!()
        };
        let node = if let InsertResult::Normal(node) = node.interior_insert(12, children[1]) {
            node
        } else {
            unreachable!()
        };
        let node = if let InsertResult::Normal(node) = node.interior_insert(300, children[2]) {
            node
        } else {
            unreachable!()
        };
        let node = if let InsertResult::Normal(node) = node.interior_insert(1242, children[2]) {
            node
        } else {
            unreachable!()
        };
        println!("{:#?}", node);
        let node = match node.interior_insert(200, children[4] as u32) {
            InsertResult::Splitted(key, left, right) => {
                println!("{:#?}", key);
                println!("{:#?}", left);
                println!("{:#?}", right);
                left
            }
            _ => unreachable!("Bad result"),
        };
        let cell = node.cell_at(0);
        assert_eq!(cell.child_pointer(), 12);
        for i in 0..node.num_cells() - 1 {
            let lo = node.cell_at(i).key();
            let hi = node.cell_at(i + 1).key();
            // assert!(lo < hi, "Key should be sorted: {} > {}", lo, hi);
        }
    }

    // #[test]
    // fn basic_leaf_insert() {
    //     const BLOCK_SIZE: usize = 4096;
    //     const DISK_CAPACITY: usize = 4096 * 32;
    //     const MEMORY_CAPACITY: usize = 4096 * 16;

    //     let memory = [0; MEMORY_CAPACITY];
    //     let disk = disk::Disk::<BLOCK_SIZE, DISK_CAPACITY>::create("basic_leaf_insert").unwrap();
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
    //     let cell = node.cell_at(node.cell_pointer(0));
    //     assert_eq!(cell.kept_payload(), &[1, 2, 3, 4, 5, 6],);
    // }

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
