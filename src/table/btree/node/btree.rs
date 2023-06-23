#![allow(dead_code)]

use std::{
    fmt::Display,
    mem::{swap, take},
};

trait BtreeNode {
    fn should_split(&self) -> bool;
    fn insert<K: PartialOrd, V>(&mut self, key: &K, val: &V);
}

type InteriorVal<K, V> = (K, Box<SimpleNode<K, V>>);

#[derive(Debug, PartialEq, Default)]
pub enum SimpleNode<K, V> {
    Interior {
        vals: Vec<InteriorVal<K, V>>,
        left_child: Option<Box<SimpleNode<K, V>>>,
    },
    Leaf {
        vals: Vec<(K, V)>,
    },
    #[default]
    None,
}

#[derive(Debug)]
enum InsertResult<K, V> {
    KeyExisted(Box<SimpleNode<K, V>>),
    Normal(Box<SimpleNode<K, V>>),
    Splitted(K, Box<SimpleNode<K, V>>, Box<SimpleNode<K, V>>),
}

#[derive(Debug, PartialEq)]
pub enum Slot {
    /// Represent a slot which is not occupied by a key yet
    Hole(usize),
    /// Represent a slot which is occupied by a key
    Cell(usize),
}

fn split_in_half<T>(vals: Vec<T>) -> (Vec<T>, Vec<T>) {
    let mid = vals.len() / 2;
    let mut l = Vec::new();
    let mut r = Vec::new();
    for (i, item) in vals.into_iter().enumerate() {
        if i < mid {
            l.push(item);
        } else {
            r.push(item);
        }
    }
    (l, r)
}

fn split_in_half_with_mid<T>(vals: Vec<T>) -> (Vec<T>, T, Vec<T>) {
    let mid = vals.len() / 2;
    let mut l = Vec::new();
    let mut r = Vec::new();
    let mut mid_val = None;
    for (i, item) in vals.into_iter().enumerate() {
        if i < mid {
            l.push(item);
        } else if i == mid {
            mid_val = Some(item);
        } else {
            r.push(item);
        }
    }
    (l, mid_val.unwrap(), r)
}

impl<K: PartialOrd + Ord + Default + Clone, V> SimpleNode<K, V> {
    fn node_insert(mut node: Box<SimpleNode<K, V>>, key: K, value: V) -> InsertResult<K, V> {
        let slot = node.search(&key);
        let node: Box<SimpleNode<K, V>> = match *node {
            SimpleNode::Leaf { ref mut vals } => {
                let hole = match slot {
                    Slot::Hole(hole) => hole,
                    Slot::Cell(_) => return InsertResult::KeyExisted(node),
                };
                vals.insert(hole, (key, value));
                node
            }
            SimpleNode::Interior {
                mut vals,
                mut left_child,
            } => {
                let hole = match slot {
                    Slot::Hole(hole) => hole,
                    Slot::Cell(hole) => hole,
                };
                match vals.get_mut(hole as usize) {
                    Some(val) => {
                        let val = take(val);
                        let val_key = val.0;
                        let (split, val) = match SimpleNode::node_insert(val.1, key, value) {
                            InsertResult::Normal(node) => (None, node),
                            InsertResult::Splitted(k, l, r) => (Some((k, l)), r),
                            InsertResult::KeyExisted(node) => {
                                return InsertResult::KeyExisted(node)
                            }
                        };
                        swap(&mut (val_key, val), &mut vals[hole]);
                        if let Some(v) = split {
                            vals.insert(hole, v);
                        }
                    }
                    None => {
                        let val = left_child.take().unwrap();
                        let (split, val) = match SimpleNode::node_insert(val, key, value) {
                            InsertResult::Normal(node) => (None, node),
                            InsertResult::Splitted(k, l, r) => (Some((k, l)), r),
                            InsertResult::KeyExisted(node) => {
                                return InsertResult::KeyExisted(node)
                            }
                        };
                        let _ = left_child.insert(val);
                        if let Some(v) = split {
                            vals.push(v);
                        }
                    }
                }
                Box::new(SimpleNode::Interior { vals, left_child })
            }
            SimpleNode::None => unreachable!(),
        };
        if node.overflow() {
            let (k, l, r) = SimpleNode::node_split(node);
            return InsertResult::Splitted(k, l, r);
        }
        InsertResult::Normal(node)
    }

    pub fn node_split(
        node: Box<SimpleNode<K, V>>,
    ) -> (K, Box<SimpleNode<K, V>>, Box<SimpleNode<K, V>>) {
        match *node {
            SimpleNode::None => unreachable!(),
            SimpleNode::Leaf { vals } => {
                let (l, r) = split_in_half(vals);
                let mid_key = r.first().unwrap().0.clone();
                let l = Box::new(SimpleNode::Leaf { vals: l });
                let r = Box::new(SimpleNode::Leaf { vals: r });
                (mid_key, l, r)
            }
            SimpleNode::Interior { vals, left_child } => {
                let (l, mid, r) = split_in_half_with_mid(vals);
                let mid_key = mid.0;
                let l = Box::new(SimpleNode::Interior {
                    vals: l,
                    left_child: Some(mid.1),
                });
                let r = Box::new(SimpleNode::Interior {
                    vals: r,
                    left_child: Some(left_child.unwrap()),
                });
                (mid_key, l, r)
            }
        }
    }

    pub fn get(&self, key: &K) -> Option<&V> {
        match self {
            Self::Interior { vals, left_child } => {
                let first = &vals.first().unwrap().0;
                if key < first {
                    return vals.first().unwrap().1.get(key);
                }
                let last = &vals.last().unwrap().0;
                if key >= last {
                    return left_child.as_ref().unwrap().get(key);
                }
                for i in 0..vals.len() - 1 {
                    let lo = &vals[i].0;
                    let hi = &vals[i + 1].0;
                    if key >= lo && key < hi {
                        return vals[i + 1].1.get(key);
                    }
                }
                None
            }
            Self::Leaf { vals } => match vals.binary_search_by_key(&key, |(k, _)| &k) {
                Ok(idx) => Some(&vals[idx].1),
                Err(_) => None,
            },
            _ => unreachable!(),
        }
    }

    fn get_mut(&mut self, key: &K) -> Option<&mut V> {
        match self {
            Self::Interior { vals, left_child } => {
                let first = &vals.first().unwrap().0;
                if &key < &first {
                    return vals.first_mut().unwrap().1.get_mut(key);
                }
                let last = &vals.last().unwrap().0;
                if &key >= &last {
                    return left_child.as_mut().unwrap().get_mut(key);
                }
                for i in 0..vals.len() {
                    let lo = &vals[i].0;
                    let hi = &vals[i + 1].0;
                    if &key >= &lo && &key < &hi {
                        return vals[0].1.get_mut(key);
                    }
                }
                None
            }
            Self::Leaf { vals } => match vals.binary_search_by_key(&key, |(k, _)| &k) {
                Ok(idx) => Some(&mut vals[idx].1),
                Err(_) => None,
            },
            _ => unreachable!(),
        }
    }

    fn overflow(&self) -> bool {
        let len = match self {
            Self::Interior {
                vals,
                left_child: _,
            } => vals.len() + 1,
            Self::Leaf { vals } => vals.len(),
            _ => unreachable!(),
        };
        len > 10
    }

    fn keys(&self) -> Vec<&K> {
        match self {
            SimpleNode::None => unreachable!(),
            Self::Leaf { vals } => vals.iter().map(|(k, _)| k).collect(),
            Self::Interior {
                vals,
                left_child: _,
            } => vals.iter().map(|(e, _)| e).collect(),
        }
    }

    fn search(&self, search_key: &K) -> Slot {
        let num_cells = self.keys().len();
        if num_cells == 0 {
            return Slot::Hole(0);
        }
        let mut hi = num_cells;
        let mut lo = 0;
        loop {
            let mid = (lo + hi) / 2;
            let mid_key = self.keys()[mid].clone();
            if search_key < &mid_key {
                if mid == 0 {
                    return Slot::Hole(0);
                } else if search_key > &self.keys()[mid - 1] {
                    return Slot::Hole(mid);
                }
                hi = mid;
            } else if search_key > &mid_key {
                if mid == num_cells - 1 {
                    return Slot::Hole(num_cells);
                }
                lo = mid;
            } else {
                return Slot::Cell(mid);
            }
        }
    }
}

#[derive(Debug)]
pub struct Btree<K, V> {
    root: Box<SimpleNode<K, V>>,
}

impl<K: Default + Ord + Clone, V> Btree<K, V> {
    pub fn new() -> Self {
        Self {
            root: Box::new(SimpleNode::Leaf { vals: vec![] }),
        }
    }

    pub fn insert(&mut self, key: K, val: V) {
        let root = take(&mut self.root);
        self.root = match SimpleNode::node_insert(root, key, val) {
            InsertResult::Normal(node) => node,
            InsertResult::Splitted(k, l, r) => Box::new(SimpleNode::Interior {
                vals: vec![(k, l)],
                left_child: Some(r),
            }),
            InsertResult::KeyExisted(_) => unreachable!(),
        }
    }

    pub fn get(&self, key: &K) -> Option<&V> {
        self.root.get(key)
    }

    pub fn get_mut(&mut self, key: &K) -> Option<&mut V> {
        self.root.get_mut(key)
    }
}
