use super::{Row, Table, ROWS_PER_PAGE, ROW_SIZE};

pub struct Cursor<'a> {
    table: &'a mut Table,
    row_num: usize,
    end_of_table: bool,
}

impl<'a> Cursor<'a> {
    pub fn table_start(table: &'a mut Table) -> Self {
        let end_of_table = if table.row_num == 0 { true } else { false };
        Cursor {
            table,
            row_num: 0,
            end_of_table,
        }
    }

    pub fn table_end(table: &'a mut Table) -> Self {
        let row_num = table.row_num;
        Cursor {
            table,
            row_num,
            end_of_table: true,
        }
    }

    pub fn advance(&mut self) {
        if self.end_of_table {
            panic!("End of table");
        }
        self.row_num += 1;
        if self.row_num == self.table.row_num {
            self.end_of_table = true;
        }
    }

    pub fn write(&mut self, row: Row) {
        let page_num = self.row_num / ROWS_PER_PAGE;
        let page = &mut self.table.pager.get_page_mut(page_num).unwrap();
        let row_offset = self.row_num % ROWS_PER_PAGE;
        let byte_offset = row_offset * ROW_SIZE;
        println!("{}", byte_offset);
        unsafe {
            page.write_val_at(byte_offset, row);
        }
        if self.end_of_table {
            self.end_of_table = false;
            self.table.row_num += 1;
        }
    }

    pub fn read(&mut self) -> Option<&Row> {
        if self.end_of_table {
            return None;
        }
        let page_num = self.row_num / ROWS_PER_PAGE;
        let page = &mut self.table.pager.get_page(page_num).unwrap();
        let row_offset = self.row_num % ROWS_PER_PAGE;
        let byte_offset = row_offset * ROW_SIZE;
        println!("{}", byte_offset);
        Some(unsafe { page.get_val_at::<Row>(byte_offset) })
    }
}
