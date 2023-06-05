use crate::page::Pager;

pub struct Table {
    pub row_num: usize,
    pub pager: Pager,
    pub root_page_num: usize,
}

impl Table {
    pub fn init() -> Self {
        let pager = Pager::init("mydb.db");
        Table {
            row_num: 0,
            pager,
            root_page_num: 0,
        }
    }
}

impl Drop for Table {
    fn drop(&mut self) {
        self.pager.flush();
    }
}
