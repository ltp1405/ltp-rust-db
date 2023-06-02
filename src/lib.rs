use std::mem::size_of;

pub mod meta_commands;
pub mod repl;
pub mod statements;
pub mod vm;

const COLUMN_USERNAME_SIZE: usize = 32;
const COLUMN_EMAIL_SIZE: usize = 255;
const PAGE_SIZE: usize = 4096;
const TABLE_MAX_PAGES: usize = 100;
const ROW_SIZE: usize = size_of_row();
const ROWS_PER_PAGE: usize = PAGE_SIZE / ROW_SIZE;

const fn size_of_row() -> usize {
    size_of::<Row>()
}

#[derive(Debug)]
pub struct Row {
    pub id: i32,
    pub username: [char; COLUMN_USERNAME_SIZE],
    pub email: [char; COLUMN_EMAIL_SIZE],
}

#[derive(Debug)]
struct Page {
    pub buffer: [u8; PAGE_SIZE],
}

pub struct Table {
    rows: usize,
    pages: Vec<Option<Box<Page>>>,
}

impl Table {
    pub fn init() -> Self {
        let mut pages = Vec::new();
        pages.push(None);
        Table { rows: 0, pages }
    }

    pub fn insert_row(&mut self, row: Row) {
        let page_num = self.rows / ROWS_PER_PAGE;
        let page = &mut self.pages[page_num];
        let page = page.get_or_insert(Box::new(Page {
            buffer: [0; PAGE_SIZE],
        }));

        let row_offset = self.rows % ROWS_PER_PAGE;
        let byte_offset = row_offset * ROW_SIZE;
        let page_ptr = page.buffer.as_mut_ptr();
        let row_slot_ptr = unsafe { page_ptr.add(byte_offset) } as *mut Row;
        unsafe {
            std::ptr::write(row_slot_ptr, row);
        }
        unsafe {println!("{:?}", *row_slot_ptr);}
        self.rows += 1;
    }

    pub fn select_row(&mut self) {
        for i in 0..self.rows {
            let page_num = i / ROWS_PER_PAGE;
            let page = &self.pages[page_num].as_ref().unwrap();
            let row_offset = i % ROWS_PER_PAGE;
            let byte_offset = row_offset * ROW_SIZE;
            let page_ptr = page.buffer.as_ptr();

            let row_ptr = unsafe { page_ptr.add(byte_offset) } as *mut Row;
            unsafe {
                println!("{:?}", *row_ptr);
            }
        }
    }
}
