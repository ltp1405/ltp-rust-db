use super::Pager;
use crate::table::{page::Page, TABLE_MAX_PAGES};

use serial_test::serial;

static TEST_FILE: &str = "test_pager.db";
use std::fs::File;

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
    assert_eq!(&empty_page, page.unwrap());
}

#[test]
#[serial]
fn modify_all_page() {
    for i in 0..TABLE_MAX_PAGES {
        initialize();
        let mut pager = Pager::init(TEST_FILE);
        let page = pager.get_page_mut(i).unwrap();
        for e in page.iter_mut() {
            *e = 0x1;
        }
        pager.flush();

        let page = pager.get_page(i).unwrap();
        for e in page.iter() {
            assert_eq!(e, &0x1);
        }
    }
}

#[test]
#[serial]
fn modify_a_page() {
    initialize();
    let mut pager = Pager::init(TEST_FILE);
    let page = pager.get_page_mut(0).unwrap();
    for e in page.iter_mut() {
        *e = 0x1;
    }
    pager.flush();

    let page = pager.get_page(0).unwrap();
    for e in page.iter() {
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
    for e in page.iter_mut() {
        *e = 0x1;
    }
    pager.flush();

    drop(pager);

    let mut pager = Pager::init(TEST_FILE);
    let page = pager.get_page(0).unwrap();
    for e in page.iter() {
        assert_eq!(e, &0x1);
    }
}

#[test]
#[serial]
fn modify_save_load_all_pages() {
    initialize();
    let mut pager = Pager::init(TEST_FILE);
    for i in 0..TABLE_MAX_PAGES {
        let page = pager.get_page_mut(i).unwrap();
        for e in page.iter_mut() {
            *e = i as u8;
        }
    }
    pager.flush();

    let mut pager = Pager::init(TEST_FILE);
    for i in 0..TABLE_MAX_PAGES {
        let page = pager.get_page(i).unwrap();
        for e in page.iter() {
            if *e != i as u8 {
                panic!("Error at page {}", i);
            }
        }
    }
}
