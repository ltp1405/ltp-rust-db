pub struct FrameAllocator<const PAGE_SIZE: usize, const CAPACITY: usize> {
    bitmap: Vec<u8>,
}

impl<const PAGE_SIZE: usize, const CAPACITY: usize> FrameAllocator<PAGE_SIZE, CAPACITY> {
    pub fn size() -> usize {
        (CAPACITY / PAGE_SIZE) / 8
            + if (CAPACITY / PAGE_SIZE) % 8 == 0 {
                0
            } else {
                1
            }
    }

    pub fn init() -> Self {
        let allocator = Self {
            bitmap: vec![0; Self::size()],
        };
        allocator
    }

    pub unsafe fn allocate_frame(&mut self) -> Option<u32> {
        for i in 0..Self::size() {
            let byte = self.bitmap[i];
            for j in 0..8 {
                if byte & (1 << j) == 0 {
                    self.bitmap[i] |= 1 << j;
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
        let mut allocator = FrameAllocator::<1024, 4096>::init();
        assert_eq!(unsafe { allocator.allocate_frame() }, Some(0));
        assert_eq!(unsafe { allocator.allocate_frame() }, Some(1));
    }
}
