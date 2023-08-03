use std::{
    mem::size_of,
    ops::{Deref, DerefMut},
    ptr::{slice_from_raw_parts, slice_from_raw_parts_mut},
};

use super::PageTable;

pub type Memory<'a> = &'a [u8];

pub struct Page<'a, const PAGE_SIZE: usize> {
    page_number: u32,
    memory: Memory<'a>,
}

impl<'a, const PAGE_SIZE: usize> Page<'a, PAGE_SIZE> {
    pub fn init(page_number: u32, memory: Memory<'a>) -> Self {
        let page_number = page_number as usize;
        if page_number * PAGE_SIZE >= memory.len() {
            panic!("Memory out of bound");
        }
        Self {
            page_number: page_number as u32,
            memory,
        }
    }

    pub fn buffer(&self) -> &[u8] {
        let page_number = self.page_number as usize;
        &self.memory[page_number * PAGE_SIZE..(page_number + 1) * PAGE_SIZE]
    }

    pub fn buffer_mut(&self) -> &mut [u8] {
        let page_number = self.page_number as usize;
        let buffer_ptr = unsafe { self.memory.as_ptr().add(page_number * PAGE_SIZE) as *mut u8 };
        let s = slice_from_raw_parts_mut(buffer_ptr, PAGE_SIZE);
        unsafe { s.as_mut().unwrap() }
    }

    pub unsafe fn read_val_at<T>(&self, pos: usize) -> T {
        if pos + size_of::<T>() > PAGE_SIZE {
            panic!("Memory out of bound");
        }
        let buffer_ptr = self.buffer().as_ptr().add(pos) as *const T;
        unsafe { buffer_ptr.read_unaligned() }
    }

    pub unsafe fn get_val_at<T>(&self, pos: usize) -> &T {
        if pos + size_of::<T>() > PAGE_SIZE {
            panic!("Memory out of bound");
        }
        let buffer_ptr = (self.buffer().as_ptr()).add(pos) as *const T;
        unsafe { buffer_ptr.as_ref().unwrap() }
    }

    pub unsafe fn get_val_mut_at<T>(&self, pos: usize) -> &mut T {
        if pos + size_of::<T>() > PAGE_SIZE {
            panic!("Memory out of bound");
        }
        let buffer_ptr = self.buffer().as_ptr().add(pos) as *mut T;
        unsafe { buffer_ptr.as_mut().unwrap() }
    }

    pub unsafe fn write_val_at<T>(&self, pos: usize, val: T) {
        if pos + size_of::<T>() > PAGE_SIZE {
            panic!("Memory out of bound");
        }
        let buffer_ptr = self.buffer().as_ptr().add(pos) as *mut T;
        unsafe { buffer_ptr.write(val) }
    }

    pub fn read_buf_at(&self, pos: usize, len: usize) -> &[u8] {
        if pos + len > PAGE_SIZE {
            panic!("Memory out of bound");
        }
        let buffer_ptr = unsafe { self.buffer().as_ptr().add(pos) as *mut u8 };
        let s = slice_from_raw_parts(buffer_ptr, len);
        unsafe { s.as_ref().unwrap() }
    }

    pub fn get_buf_mut_at(&self, pos: usize, len: usize) -> &mut [u8] {
        if pos + len > PAGE_SIZE {
            panic!("Memory out of bound");
        }
        let buffer_ptr = unsafe { self.buffer().as_ptr().add(pos) as *mut u8 };
        let s = slice_from_raw_parts_mut(buffer_ptr, len);
        unsafe { s.as_mut().unwrap() }
    }

    pub fn write_buf_at(&self, pos: usize, buf: &[u8]) {
        if pos + buf.len() > PAGE_SIZE {
            panic!("Memory out of bound");
        }
        let buffer_ptr = unsafe { self.buffer().as_ptr().add(pos) as *mut u8 };
        let s = slice_from_raw_parts_mut(buffer_ptr, buf.len());
        unsafe { s.as_mut().unwrap().copy_from_slice(buf) }
    }
}

impl<const PAGE_SIZE: usize> Deref for Page<'_, PAGE_SIZE> {
    type Target = [u8];

    fn deref(&self) -> &Self::Target {
        self.buffer()
    }
}

impl<const PAGE_SIZE: usize> DerefMut for Page<'_, PAGE_SIZE> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        self.buffer_mut()
    }
}

impl<const PAGE_SIZE: usize> Drop for Page<'_, PAGE_SIZE> {
    fn drop(&mut self) {
        PageTable::read(&self.memory).drop_page(self.page_number)
    }
}

// #[cfg(test)]
// mod page {
//     use super::Page;

//     const PAGE_SIZE: usize = 4096;
//     const MEM_CAPACITY: usize = PAGE_SIZE * 16;

//     #[derive(Debug, PartialEq, Clone)]
//     struct TestStruct {
//         x: u32,
//         y: f32,
//         z: i64,
//     }

//     #[test]
//     fn write_read_buf() {
//         let mem = [0; MEM_CAPACITY];
//         let page: Page<PAGE_SIZE> = Page::init(0, &mem);
//         page.write_buf_at(0, &[1, 2, 3]);
//         page.write_buf_at(0, &[1, 2, 3]);
//         assert_eq!(page.read_buf_at(0, 3), &[1, 2, 3]);
//     }

//     #[test]
//     fn init() {
//         let mem = [0; MEM_CAPACITY];
//         let page: Page<PAGE_SIZE> = Page::init(0, &mem);
//         assert_eq!(page.buffer().as_ref(), &[0; 4096]);
//     }

//     #[test]
//     fn simple_read_write() {
//         let mem = [0; MEM_CAPACITY];
//         let page: Page<PAGE_SIZE> = Page::init(0, &mem);
//         unsafe {
//             page.write_val_at(0, 12);
//         }
//         let read_val = unsafe { page.read_val_at::<u32>(0) };
//         assert_eq!(read_val, 12);
//     }

//     #[test]
//     fn read_write() {
//         let mem = [0; MEM_CAPACITY];
//         let page: Page<PAGE_SIZE> = Page::init(0, &mem);
//         unsafe {
//             page.write_val_at::<u32>(2, 12);
//         }
//         let read_val = unsafe { page.read_val_at::<u32>(2) };
//         assert_eq!(read_val, 12);
//     }

//     #[test]
//     fn read_write_struct() {
//         let mem = [0; MEM_CAPACITY];
//         let page: Page<PAGE_SIZE> = Page::init(0, &mem);
//         let test_struct = TestStruct {
//             x: 1231,
//             y: 0.0,
//             z: -12412,
//         };
//         unsafe {
//             page.write_val_at(1, test_struct.clone());
//         }
//         let read_val = unsafe { page.read_val_at::<TestStruct>(1) };
//         assert_eq!(test_struct, read_val);
//     }
// }
