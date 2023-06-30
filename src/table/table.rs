use std::sync::{Arc, Mutex};

use ltp_rust_db_page::pager::Pager;

use super::{
    record::Record, unordered_file::File, cursor::Cursor,
};

pub struct Table {
    file: File,
}

impl Table {
    pub fn init(filename: &str) -> Self {
        let pager = Arc::new(Mutex::new(Pager::init(filename)));
        let file = File::init(pager);
        Self { file }
    }

    pub fn insert(&mut self, record: Record) {
        let cell = record.to_cell();
        self.file.insert(cell);
    }

    pub fn cursor(&self) -> impl Cursor {
        self.file.cursor()
    }
}

#[cfg(test)]
mod tests {
    use std::fs::remove_file;

    use crate::table::{
        record::{Field, Record},
        schema::{DataType, Schema}, cursor::Cursor,
    };

    use super::Table;

    #[test]
    fn basic() {
        let mut table = Table::init("table_basic");

        let schema = Schema {
            schema: vec![
                DataType::Char(10),
                DataType::Bool,
                DataType::UInt,
                DataType::VarChar(255),
            ],
        };
        let record = Record {
            schema: &schema,
            data: vec![
                Field::Char(Some(b"Hello".to_vec())),
                Field::Bool(Some(true)),
                Field::UInt(Some(42)),
                Field::VarChar(Some(b"World".to_vec())),
            ],
        };
        let bytes = record.clone().to_bytes();
        let record2 = Record::from_bytes(&schema, bytes);
        assert_eq!(record, record2);

        table.insert(record.clone());

        let mut cursor = table.cursor();
        let r = cursor.read();
        let record2 = Record::from_bytes(&schema, r.buf);
        assert_eq!(record, record2);

        remove_file("table_basic").unwrap();
    }
}
