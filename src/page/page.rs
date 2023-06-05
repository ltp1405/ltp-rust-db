use std::{
    mem::size_of,
    ops::{Deref, DerefMut},
};

use super::PAGE_SIZE;

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

    pub unsafe fn read_val_at<T>(&self, pos: usize) -> T {
        if pos + size_of::<T>() > PAGE_SIZE {
            panic!("Memory out of bound");
        }
        let buffer_ptr = self.as_ptr().add(pos) as *const T;
        unsafe { buffer_ptr.read_unaligned() }
    }

    pub unsafe fn get_val_at<T>(&self, pos: usize) -> &T {
        if pos + size_of::<T>() > PAGE_SIZE {
            panic!("Memory out of bound");
        }
        let buffer_ptr = (self.as_ptr()).add(pos) as *const T;
        unsafe { buffer_ptr.as_ref().unwrap() }
    }

    pub unsafe fn get_val_mut_at<T>(&self, pos: usize) -> &mut T {
        if pos + size_of::<T>() > PAGE_SIZE {
            panic!("Memory out of bound");
        }
        let buffer_ptr = self.as_ptr().add(pos) as *mut T;
        unsafe { buffer_ptr.as_mut().unwrap() }
    }

    pub unsafe fn write_val_at<T>(&mut self, pos: usize, val: T) {
        if pos + size_of::<T>() > PAGE_SIZE {
            panic!("Memory out of bound");
        }
        let buffer_ptr = self.as_ptr().add(pos) as *mut T;
        unsafe { buffer_ptr.write_unaligned(val) }
    }
}

#[cfg(test)]
mod page {
    use crate::page::PAGE_SIZE;

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
        unsafe {
            page.write_val_at(0, 12);
        }
        let read_val = unsafe { page.read_val_at::<u32>(0) };
        assert_eq!(read_val, 12);
    }

    #[test]
    fn read_write() {
        let mut page = Page::init();
        unsafe {
            page.write_val_at::<u32>(2, 12);
        }
        let read_val = unsafe { page.read_val_at::<u32>(2) };
        assert_eq!(read_val, 12);
    }

    #[test]
    fn read_write_struct() {
        let mut page = Page::init();
        let test_struct = TestStruct {
            x: 1231,
            y: 0.0,
            z: -12412,
        };
        unsafe{page.write_val_at(1, test_struct.clone());}
        let read_val = unsafe { page.read_val_at::<TestStruct>(1) };
        assert_eq!(test_struct, read_val);
    }
}
