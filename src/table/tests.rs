use std::fs::File;

use super::{Cursor, Row, Table};

#[test]
fn simple_insert() {
    File::create("mydb.db").unwrap();
    let mut table = Table::init();
    let mut cursor = Cursor::table_end(&mut table);
    let test_row = Row::new(1, "ltp1405", "ltp@gmail.com");
    cursor.write(test_row.clone());
    let rs = cursor.read().unwrap();
    assert_eq!(rs, &test_row);
}

#[test]
fn empty_table() {
    File::create("mydb.db").unwrap();
    let mut table = Table::init();
    let mut cursor = Cursor::table_end(&mut table);
    let rs = cursor.read();
    assert_eq!(rs, None);
}

fn many_insert() {
    File::create("mydb.db").unwrap();
    let mut table = Table::init();
    let mut cursor = Cursor::table_end(&mut table);
    let test_row = Row::new(1, "ltp1405", "ltp@gmail.com");
    for _ in 0..300 {
        cursor.write(test_row.clone());
        cursor.advance();
    }
    let mut cursor = Cursor::table_start(&mut table);
    for _ in 0..300 {
        assert_eq!(cursor.read(), Some(&test_row));
        cursor.advance();
    }
}

#[test]
#[should_panic]
fn over_advance() {
    File::create("mydb.db").unwrap();
    let mut table = Table::init();
    let mut cursor = Cursor::table_end(&mut table);
    cursor.advance();
}
