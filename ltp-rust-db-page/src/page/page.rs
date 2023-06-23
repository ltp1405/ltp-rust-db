use std::{
    mem::size_of,
    ops::{Deref, DerefMut},
    ptr::{slice_from_raw_parts, slice_from_raw_parts_mut},
    sync::{Arc, RwLock},
};

use super::PAGE_SIZE;

#[derive(Debug, Clone)]
pub struct Page {
    buffer: Arc<RwLock<Box<[u8; PAGE_SIZE]>>>,
}

// impl Deref for Page {
//     type Target = Arc<RwLock<[u8; PAGE_SIZE]>>;
//     fn deref(&self) -> &Self::Target {
//         &self.buffer
//     }
// }

// impl DerefMut for Page {
//     fn deref_mut(&mut self) -> &mut Self::Target {
//         &mut self.buffer
//     }
// }

impl Page {
    pub fn init() -> Self {
        let buf = Box::new([0; PAGE_SIZE]);
        Self {
            buffer: Arc::new(RwLock::new(buf)),
        }
    }

    pub unsafe fn read_val_at<T>(&self, pos: usize) -> T {
        if pos + size_of::<T>() > PAGE_SIZE {
            panic!("Memory out of bound");
        }
        let buffer = self.buffer.read().unwrap();
        let buffer_ptr = buffer.as_ptr().add(pos) as *const T;
        unsafe { buffer_ptr.read_unaligned() }
    }

    pub unsafe fn get_val_at<T>(&self, pos: usize) -> &T {
        if pos + size_of::<T>() > PAGE_SIZE {
            panic!("Memory out of bound");
        }
        let buffer = self.buffer.read().unwrap();
        let buffer_ptr = (buffer.as_ptr()).add(pos) as *const T;
        unsafe { buffer_ptr.as_ref().unwrap() }
    }

    pub unsafe fn get_val_mut_at<T>(&self, pos: usize) -> &mut T {
        if pos + size_of::<T>() > PAGE_SIZE {
            panic!("Memory out of bound");
        }
        let buffer = self.buffer.read().unwrap();
        let buffer_ptr = buffer.as_ptr().add(pos) as *mut T;
        unsafe { buffer_ptr.as_mut().unwrap() }
    }

    pub unsafe fn write_val_at<T>(&self, pos: usize, val: T) {
        if pos + size_of::<T>() > PAGE_SIZE {
            panic!("Memory out of bound");
        }
        let buffer = self.buffer.write().unwrap();
        let buffer_ptr = buffer.as_ptr().add(pos) as *mut T;
        unsafe { buffer_ptr.write_unaligned(val) }
    }

    pub fn read_buf_at(&self, pos: usize, len: usize) -> &[u8] {
        if pos + len > PAGE_SIZE {
            panic!("Memory out of bound");
        }
        let buffer = self.buffer.read().unwrap();
        let buffer_ptr = unsafe { buffer.as_ptr().add(pos) as *mut u8 };
        let s = slice_from_raw_parts(buffer_ptr, len);
        unsafe { s.as_ref().unwrap() }
    }

    pub fn get_buf_mut_at(&self, pos: usize, len: usize) -> &mut [u8] {
        if pos + len > PAGE_SIZE {
            panic!("Memory out of bound");
        }
        let buffer = self.buffer.write().unwrap();
        let buffer_ptr = unsafe { buffer.as_ptr().add(pos) as *mut u8 };
        let s = slice_from_raw_parts_mut(buffer_ptr, len);
        unsafe { s.as_mut().unwrap() }
    }

    pub fn write_buf_at(&self, pos: usize, buf: &[u8]) {
        if pos + buf.len() > PAGE_SIZE {
            panic!("Memory out of bound");
        }
        let buffer = self.buffer.write().unwrap();
        let buffer_ptr = unsafe { buffer.as_ptr().add(pos) as *mut u8 };
        let s = slice_from_raw_parts_mut(buffer_ptr, buf.len());
        unsafe { s.as_mut().unwrap().copy_from_slice(buf) }
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
    fn write_read_buf() {
        let mut page = Page::init();
        page.write_buf_at(0, &[1, 2, 3]);
        page.write_buf_at(0, &[1, 2, 3]);
        assert_eq!(page.read_buf_at(0, 3), &[1, 2, 3]);
        println!("{:?}", page);
    }

    #[test]
    fn init() {
        let page = Page::init();
        let buffer = page.buffer.read().unwrap();
        assert_eq!(buffer.as_ref(), &[0; PAGE_SIZE]);
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
        unsafe {
            page.write_val_at(1, test_struct.clone());
        }
        let read_val = unsafe { page.read_val_at::<TestStruct>(1) };
        assert_eq!(test_struct, read_val);
    }
}
