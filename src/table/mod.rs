use std::mem::size_of;

pub mod btree;
mod cursor;
mod row;
mod table;
mod tests;

pub use cursor::Cursor;
pub use row::Row;
pub use table::Table;

use crate::page::PAGE_SIZE;

const COLUMN_USERNAME_SIZE: usize = 32;
const COLUMN_EMAIL_SIZE: usize = 255;
pub const TABLE_MAX_PAGES: usize = 100;
const ROW_SIZE: usize = size_of_row();
const ROWS_PER_PAGE: usize = PAGE_SIZE / ROW_SIZE;

const fn size_of_row() -> usize {
    size_of::<Row>()
}
