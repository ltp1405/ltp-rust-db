use super::Pager;

pub struct Table {
    pub row_num: usize,
    pub pager: Pager,
}

impl Table {
    pub fn init() -> Self {
        let pager = Pager::init("mydb.db");
        Table { row_num: 0, pager }
    }
}

impl Drop for Table {
    fn drop(&mut self) {
        self.pager.flush();
    }
}
