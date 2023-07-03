use std::sync::{Arc, Mutex};

use ltp_rust_db_page::pager::Pager;

use super::{
    record::Record,
    unordered_file::{cell::Cell, Cursor, File},
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
        let cell = Cell::new(record.to_bytes());
        self.file.insert(cell);
    }

    pub fn cursor(&self) -> Cursor {
        self.file.cursor()
    }
}

#[cfg(test)]
mod tests {
    use std::fs::remove_file;

    use crate::table::{
        record::{Field, Record},
        schema::{DataType, Schema},
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
            schema: schema.clone(),
            data: vec![
                Field::Char(Some(b"Hello".to_vec())),
                Field::Bool(Some(true)),
                Field::UInt(Some(42)),
                Field::VarChar(Some(b"World".to_vec())),
            ],
        };
        let bytes = record.clone().to_bytes();
        let record2 = Record::from_bytes(schema.clone(), bytes);
        assert_eq!(record, record2);

        table.insert(record.clone());

        let mut cursor = table.cursor();
        let r = cursor.read();
        let record2 = Record::from_bytes(schema, r.unwrap().buf);
        assert_eq!(record, record2);

        remove_file("table_basic").unwrap();
    }

    #[test]
    fn simple_insert() {
        let mut table = Table::init("table_simple_insert");

        let schema = Schema {
            schema: vec![
                DataType::Char(10),
                DataType::Bool,
                DataType::UInt,
                DataType::VarChar(255),
            ],
        };
        let record = Record {
            schema: schema.clone(),
            data: vec![
                Field::Char(Some(b"Hello".to_vec())),
                Field::Bool(Some(true)),
                Field::UInt(Some(42)),
                Field::VarChar(Some(b"World".to_vec())),
            ],
        };

        for _ in 0..10 {
            table.insert(record.clone());
        }

        for r in table.cursor() {
            let record2 = Record::from_bytes(schema.clone(), r.buf);
            assert_eq!(record, record2);
        }

        remove_file("table_simple_insert").unwrap();
    }

    #[test]
    fn big_record_insert() {
        let mut table = Table::init("table_big_record_insert");

        let schema = Schema {
            schema: vec![
                DataType::Int,
                DataType::Int,
                DataType::Int,
                DataType::Char(10),
                DataType::Char(10),
                DataType::Char(10),
                DataType::Bool,
                DataType::Bool,
                DataType::Float,
                DataType::UInt,
                DataType::VarChar(255),
                DataType::VarChar(255),
            ],
        };
        let record = Record {
            schema: schema.clone(),
            data: vec![
                Field::Int(Some(1)),
                Field::Int(Some(2)),
                Field::Int(Some(3)),
                Field::Char(Some(b"Hello".to_vec())),
                Field::Char(Some(b"Hello".to_vec())),
                Field::Char(Some(b"Hello".to_vec())),
                Field::Bool(Some(true)),
                Field::Bool(Some(true)),
                Field::Float(Some(1.0)),
                Field::UInt(Some(42)),
                Field::VarChar(Some(b"World".to_vec())),
                Field::VarChar(Some(b"World".to_vec())),
            ],
        };

        for _ in 0..1000 {
            table.insert(record.clone());
        }

        for cell in table.cursor() {
            let record2 = Record::from_bytes(schema.clone(), cell.buf);
            assert_eq!(record, record2);
        }

        remove_file("table_big_record_insert").unwrap();
    }

    #[test]
    fn a_lot_of_insert() {
        let mut table = Table::init("table_a_lot_of_insert");

        let schema = Schema {
            schema: vec![
                DataType::Char(10),
                DataType::Bool,
                DataType::UInt,
                DataType::VarChar(255),
            ],
        };
        let record = Record {
            schema: schema.clone(),
            data: vec![
                Field::Char(Some(b"Hello".to_vec())),
                Field::Bool(Some(true)),
                Field::UInt(Some(42)),
                Field::VarChar(Some(b"World".to_vec())),
            ],
        };

        for _ in 0..100000 {
            table.insert(record.clone());
        }

        for r in table.cursor() {
            let record2 = Record::from_bytes(schema.clone(), r.buf);
            assert_eq!(record, record2);
        }

        remove_file("table_a_lot_of_insert").unwrap();
    }
}
