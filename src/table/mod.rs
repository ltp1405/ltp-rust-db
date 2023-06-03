use std::mem::size_of;

mod pager;
mod row;
mod table;
mod tests;
mod cursor;
mod page;
pub mod btree;

pub use row::Row;
pub use table::Table;
pub use cursor::Cursor;
pub use pager::Pager;

const COLUMN_USERNAME_SIZE: usize = 32;
const COLUMN_EMAIL_SIZE: usize = 255;
const PAGE_SIZE: usize = 4096;
const TABLE_MAX_PAGES: usize = 100;
const ROW_SIZE: usize = size_of_row();
const ROWS_PER_PAGE: usize = PAGE_SIZE / ROW_SIZE;

const fn size_of_row() -> usize {
    size_of::<Row>()
}
