use std::{
    ops::{Deref, DerefMut},
    ptr::slice_from_raw_parts_mut,
};

use super::{Page, PageTable, Memory};

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
