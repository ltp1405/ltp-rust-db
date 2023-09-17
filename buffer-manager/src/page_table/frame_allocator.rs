pub struct FrameAllocator<const CAPACITY: usize, const PAGE_SIZE: usize> {
    bitmap: Vec<u8>,
}

impl<const MEMORY_CAPACITY: usize, const FRAME_SIZE: usize>
    FrameAllocator<MEMORY_CAPACITY, FRAME_SIZE>
{
    /// How many bytes are required to store the bitmap
    pub fn bitmap_size() -> usize {
        (MEMORY_CAPACITY / FRAME_SIZE) / 8
            + if (MEMORY_CAPACITY / FRAME_SIZE) % 8 == 0 {
                0
            } else {
                1
            }
    }

    /// How many frames are occupied by the bitmap
    pub fn bitmap_frames_occupied() -> usize {
        Self::bitmap_size() / FRAME_SIZE
            + if Self::bitmap_size() % FRAME_SIZE == 0 {
                0
            } else {
                1
            }
    }

    pub fn init() -> Self {
        let mut bitmap = vec![0u8; Self::bitmap_size()];
        let mut allocator = Self { bitmap };
        allocator
    }

    pub unsafe fn allocate_frame(&mut self) -> Option<u32> {
        let bytes_required = Self::bitmap_size();

        for byte in 0..self.bitmap {
            for j in 0..8 {
                if byte & (1 << j) == 0 {
                    bitmap.add(i).write(byte | (1 << j));
                    return Some((i * 8 + j) as u32);
                }
            }
        }
        log::debug!("No free frames");
        None
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_frames_required() {
        let memory = [0u8; 1024];
        assert_eq!(FrameAllocator::<1024, 512>::bitmap_size(), 1);
        assert_eq!(FrameAllocator::<1024, 512>::bitmap_frames_occupied(), 1);

        let memory = [0u8; 4096];
        assert_eq!(FrameAllocator::<1024, 512>::bitmap_size(), 1);
        assert_eq!(FrameAllocator::<1024, 512>::bitmap_frames_occupied(), 1);
    }

    #[test]
    fn test_allocate_frame() {
        let mut memory = [0u8; 4096];
        let mut allocator = FrameAllocator::<4096, 512>::init();
        let bytes_required = FrameAllocator::<4096, 512>::bitmap_size();
        let page_for_bitmap = bytes_required / 512 + if bytes_required % 512 == 0 { 0 } else { 1 };
        assert_eq!(
            unsafe { allocator.allocate_frame() },
            Some(page_for_bitmap as u32)
        );
        assert_eq!(
            unsafe { allocator.allocate_frame() },
            Some(page_for_bitmap as u32 + 1)
        );
    }

    #[test]
    fn test_allocate_frame_full() {
        let mut memory = [0u8; 1024];
        let mut allocator = FrameAllocator::<1024, 512>::init();
        let bytes_required = FrameAllocator::<1024, 512>::bitmap_size();
        let page_for_bitmap =
            bytes_required / 4096 + if bytes_required % 4096 == 0 { 0 } else { 1 };
        for _ in 0..page_for_bitmap {
            unsafe { allocator.allocate_frame() };
        }
        assert_eq!(unsafe { allocator.allocate_frame() }, Some(2));
        assert_eq!(unsafe { allocator.allocate_frame() }, Some(3));
        assert_eq!(unsafe { allocator.allocate_frame() }, Some(4));
    }
}
