mod node;

use std::sync::{Arc, Mutex};

#[derive(Debug, PartialEq)]
pub struct RowAddress {
    page_number: u32,
    offset: u32,
}

impl RowAddress {
    pub fn new(page_number: u32, offset: u32) -> Self {
        Self {
            page_number,
            offset,
        }
    }

    pub fn page_number(&self) -> u32 {
        self.page_number
    }

    pub fn offset(&self) -> u32 {
        self.offset
    }
}

// pub struct BTree {
//     root: Node,
// }

// #[derive(Debug)]
// pub struct KeyExistedError;

// impl BTree {
//     pub fn init(pager: Arc<Mutex<Pager>>) -> Self {
//         let new_page = pager.lock().unwrap().get_free_page().unwrap();
//         let mut root = Node::new(new_page, pager);
//         root.set_node_type(NodeType::Leaf);
//         Self { root }
//     }

//     pub fn insert(&mut self, key: u32, payload: &[u8]) -> Result<(), KeyExistedError> {
//         let root_clone = Node::new(self.root.page_num as usize, self.root.pager.clone());
//         self.root = {
//             let result = root_clone.node_insert(key, payload);
//             match result {
//                 InsertResult::Normal(node) => node,
//                 InsertResult::Splitted(key, left, right) => {
//                     let new_page = left.pager.lock().unwrap().get_free_page().unwrap();
//                     let pager = left.pager.clone();
//                     let mut node = Node::new(new_page as usize, pager.clone());
//                     node.set_node_type(NodeType::Interior);
//                     node.set_right_child(right.page_num);
//                     let node = match node.interior_insert(key, left.page_num) {
//                         InsertResult::Normal(node) => node,
//                         _ => unreachable!(),
//                     };
//                     node
//                 }
//                 InsertResult::KeyExisted(_key) => return Err(KeyExistedError),
//             }
//         };
//         Ok(())
//     }
// }

// #[test]
// fn basic_insert() {
//     use rand::Rng;

//     let mut rng = rand::thread_rng();
//     let pager = Arc::new(Mutex::new(Pager::init("btree1")));
//     let mut btree = BTree::init(pager);
//     for i in 0..100 {
//         let key: u32 = rng.gen();
//         if let Err(_) = btree.insert(key, &[1, 2, 3]) {
//             continue;
//         }
//     }
//     println!("{:#?}", btree.root);
// }

// #[test]
// fn find_holes() {
//     let pager = Arc::new(Mutex::new(Pager::init("btree2")));
//     let mut btree = BTree::init(pager);
//     for i in 0..3 {
//         if let Err(_) = btree.insert(i, &[1, 2, 3]) {
//             continue;
//         }
//     }
//     println!("{:#?}", btree.root.find_holes());
//     panic!()
// }
