use std::{collections::HashMap, hash::Hash};

use super::Page;

// TODO: implement this
pub struct PageLayout<K: PartialEq + Eq + Hash> {
    fields: HashMap<K, LayoutField>,
}

pub struct LayoutField(usize, usize);

pub struct LayoutFieldBuilder {}

impl<K: PartialEq + Eq + Hash> PageLayout<K> {
    pub fn new() -> Self {
        PageLayout {
            fields: HashMap::new(),
        }
    }

    pub fn with_field(mut self, key: K, field_info: LayoutField) -> Self {
        self.fields.insert(key, field_info);
        self
    }
}

struct PageWithLayout {
    raw_page: Page,
    layout: PageLayout<u8>,
}
