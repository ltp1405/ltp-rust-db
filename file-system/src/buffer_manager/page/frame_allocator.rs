pub struct FrameAllocator<'a, const CAPACITY: usize, const PAGE_SIZE: usize> {
    memory: &'a [u8],
}

impl<'a, const CAPACITY: usize, const PAGE_SIZE: usize> FrameAllocator<'a, CAPACITY, PAGE_SIZE> {
    pub fn size() -> usize {
        CAPACITY / 8
    }

    pub fn init(memory: &'a [u8]) -> Self {
        let mut allocator = Self { memory };
        let blocks_needed = Self::size() / PAGE_SIZE
            + if Self::size() % PAGE_SIZE == 0 {
                0
            } else {
                1
            };
        for _ in 0..blocks_needed {
            unsafe {
                allocator.allocate_frame();
            }
        }
        allocator
    }

    pub unsafe fn allocate_frame(&mut self) -> Option<u32> {
        let bytes_required = Self::size();
        let bitmap = self.memory[0..bytes_required].as_ptr() as *mut u8;

        for i in 0..bytes_required {
            for j in 0..8 {
                let byte = bitmap.add(i).read();
                if byte & (1 << j) == 0 {
                    bitmap.add(i).write(byte | (1 << j));
                    return Some((i * 8 + j) as u32);
                }
            }
        }
        None
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_allocate_frame() {
        let mut memory = [0u8; 1024];
        let mut allocator = FrameAllocator::<1024, 4096>::init(&mut memory);
        let bytes_required = FrameAllocator::<1024, 4096>::size();
        let page_for_bitmap =
            bytes_required / 4096 + if bytes_required % 4096 == 0 { 0 } else { 1 };
        assert_eq!(
            unsafe { allocator.allocate_frame() },
            Some(page_for_bitmap as u32)
        );
        assert_eq!(
            unsafe { allocator.allocate_frame() },
            Some(page_for_bitmap as u32 + 1)
        );
    }
}
