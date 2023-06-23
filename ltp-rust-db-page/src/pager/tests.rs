use serial_test::serial;

static TEST_FILE: &str = "test_pager.db";
use std::fs::File;

use crate::{page::Page, page::PAGE_SIZE, pager::Pager};

use super::TABLE_MAX_PAGES;

// static INIT: Once = Once::new();

pub fn initialize() {
    File::create(TEST_FILE).unwrap();
    // INIT.call_once(|| if let Err(_) = std::fs::remove_file(TEST_FILE) {});
}

#[test]
#[serial]
fn init() {
    initialize();
    let mut pager = Pager::init(TEST_FILE);
    let empty_page = Page::init();
    let page = pager.get_page(0);
    assert_eq!(
        empty_page.read_buf_at(0, PAGE_SIZE),
        page.unwrap().read_buf_at(0, PAGE_SIZE)
    );
}

#[test]
#[serial]
fn modify_all_page() {
    (0..TABLE_MAX_PAGES).for_each(|i| {
        initialize();
        let mut pager = Pager::init(TEST_FILE);
        let page = pager.get_page_mut(i).unwrap();
        let mut buf = [0; PAGE_SIZE];
        for e in buf.iter_mut() {
            *e = 0x1;
        }
        page.write_buf_at(0, &buf);
        pager.flush();

        let page = pager.get_page(i).unwrap();
        for e in page.read_buf_at(0, PAGE_SIZE) {
            assert_eq!(e, &0x1);
        }
    });
}

#[test]
#[serial]
fn modify_a_page() {
    initialize();
    let mut pager = Pager::init(TEST_FILE);
    let page = pager.get_page_mut(0).unwrap();
    let mut buf = [0; PAGE_SIZE];
    for e in buf.iter_mut() {
        *e = 0x1;
    }
    page.write_buf_at(0, &buf);
    pager.flush();

    let page = pager.get_page(0).unwrap();
    for e in page.read_buf_at(0, PAGE_SIZE) {
        assert_eq!(e, &0x1);
    }
}

#[test]
#[should_panic]
#[serial]
fn get_off_limit_page() {
    initialize();
    let mut pager = Pager::init(TEST_FILE);
    pager.get_page(20000).unwrap();
}

#[test]
#[serial]
fn modify_save_load() {
    initialize();
    let mut pager = Pager::init(TEST_FILE);
    let page = pager.get_page_mut(0).unwrap();
    let mut buf = [0; PAGE_SIZE];
    for e in buf.iter_mut() {
        *e = 0x1;
    }
    page.write_buf_at(0, &buf);
    pager.flush();

    drop(pager);

    let mut pager = Pager::init(TEST_FILE);
    let page = pager.get_page(0).unwrap();
    let buf = [0x1; PAGE_SIZE];
    let page_buf = page.read_buf_at(0, PAGE_SIZE);
    assert_eq!(buf, page_buf);
}

#[test]
#[serial]
fn modify_save_load_all_pages() {
    initialize();
    let mut pager = Pager::init(TEST_FILE);
    for i in 0..TABLE_MAX_PAGES {
        let page = pager.get_page_mut(i).unwrap();
        let mut buf = [0; PAGE_SIZE];
        for e in buf.iter_mut() {
            *e = (i % (2 << 8)) as u8;
        }
        page.write_buf_at(0, &buf);
    }
    pager.flush();

    let mut pager = Pager::init(TEST_FILE);
    for i in 0..TABLE_MAX_PAGES {
        let page = pager.get_page(i).unwrap();
        for e in page.read_buf_at(0, PAGE_SIZE) {
            assert_eq!(*e, (i % (2 << 8)) as u8, "Error at page {}", i);
        }
    }
}
