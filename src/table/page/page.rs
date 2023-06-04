use std::{
    mem::size_of,
    ops::{Deref, DerefMut},
};

use super::super::PAGE_SIZE;

#[derive(Debug, PartialEq, Eq)]
pub struct Page {
    buffer: Box<[u8; PAGE_SIZE]>,
}

impl Deref for Page {
    type Target = [u8; PAGE_SIZE];
    fn deref(&self) -> &Self::Target {
        &self.buffer
    }
}

impl DerefMut for Page {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.buffer
    }
}

impl Page {
    pub fn init() -> Self {
        Self {
            buffer: Box::new([0; PAGE_SIZE]),
        }
    }

    pub fn read_val_at<T>(&self, pos: usize) -> T {
        if pos + size_of::<T>() > PAGE_SIZE {
            panic!("Memory out of bound");
        }
        let buffer_ptr = self.as_ptr() as *const T;
        unsafe { buffer_ptr.add(pos).read_unaligned() }
    }

    pub fn get_val_at<T>(&self, pos: usize) -> &T {
        if pos + size_of::<T>() > PAGE_SIZE {
            panic!("Memory out of bound");
        }
        let buffer_ptr = self.as_ptr() as *const T;
        unsafe { buffer_ptr.add(pos).as_ref().unwrap() }
    }

    pub fn get_val_mut_at<T>(&self, pos: usize) -> &mut T {
        if pos + size_of::<T>() > PAGE_SIZE {
            panic!("Memory out of bound");
        }
        let buffer_ptr = self.as_ptr() as *mut T;
        unsafe { buffer_ptr.add(pos).as_mut().unwrap() }
    }


    pub fn write_val_at<T>(&mut self, pos: usize, val: T) {
        if pos + size_of::<T>() > PAGE_SIZE {
            panic!("Memory out of bound");
        }
        let buffer_ptr = self.as_ptr() as *mut T;
        unsafe { buffer_ptr.add(pos).write_unaligned(val) }
    }
}

#[cfg(test)]
mod page {
    use crate::table::PAGE_SIZE;

    use super::Page;

    #[derive(Debug, PartialEq, Clone)]
    struct TestStruct {
        x: u32,
        y: f32,
        z: i64,
    }

    #[test]
    fn init() {
        let page = Page::init();
        assert_eq!(*page.buffer, [0; PAGE_SIZE]);
    }

    #[test]
    fn simple_read_write() {
        let mut page = Page::init();
        page.write_val_at(0, 12);
        assert_eq!(page.read_val_at::<u32>(0), 12);
    }

    #[test]
    fn read_write() {
        let mut page = Page::init();
        page.write_val_at::<u32>(2, 12);
        assert_eq!(page.read_val_at::<u32>(2), 12);
    }

    #[test]
    fn read_write_struct() {
        let mut page = Page::init();
        let test_struct = TestStruct {
            x: 1231,
            y: 0.0,
            z: -12412,
        };
        page.write_val_at(1, test_struct.clone());
        assert_eq!(test_struct, page.read_val_at(1));
    }
}
