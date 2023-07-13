use disk::Disk;

#[derive(Debug)]
pub struct Bitmap<const BLOCKSIZE: usize, const CAPACITY: usize> {
    bitmap: Vec<u8>,
}

impl<const BLOCKSIZE: usize, const CAPACITY: usize> Bitmap<BLOCKSIZE, CAPACITY> {
    pub fn size() -> usize {
        CAPACITY / (8 * BLOCKSIZE)
    }

    pub fn new() -> Bitmap<BLOCKSIZE, CAPACITY> {
        let mut bitmap = Vec::new();
        let bitmap_len = CAPACITY / (8 * BLOCKSIZE);
        bitmap.resize(bitmap_len, 0);
        let mut bitmap = Bitmap { bitmap };
        let blocks_needed = Bitmap::<BLOCKSIZE, CAPACITY>::size() / BLOCKSIZE
            + if Bitmap::<BLOCKSIZE, CAPACITY>::size() % BLOCKSIZE == 0 {
                0
            } else {
                1
            };
        for _ in 0..blocks_needed {
            bitmap.allocate();
        }
        bitmap
    }

    pub fn allocate(&mut self) -> Option<usize> {
        for i in 0..self.bitmap.len() {
            for j in 0..8 {
                if self.bitmap[i] & (1 << j) == 0 {
                    self.bitmap[i] |= 1 << j;
                    return Some(i * 8 + j);
                }
            }
        }
        None
    }

    pub fn deallocate(&mut self, block: usize) {
        self.bitmap[block / 8] &= !(1 << (block % 8));
    }
}

pub fn read_bitmap_from_disk<const BLOCKSIZE: usize, const CAPACITY: usize>(
    disk: &Disk<BLOCKSIZE, CAPACITY>,
) -> Bitmap<BLOCKSIZE, CAPACITY> {
    let mut bitmap = Vec::new();
    let bitmap_len = CAPACITY / (8 * BLOCKSIZE);
    bitmap.resize(bitmap_len, 0);
    for i in 0..bitmap_len / BLOCKSIZE {
        let block = disk.read_block(i as usize).unwrap();
        bitmap[i * BLOCKSIZE..(i + 1) * BLOCKSIZE].copy_from_slice(block.as_ref());
    }
    let remainder = bitmap_len % BLOCKSIZE;
    if remainder != 0 {
        let block = disk.read_block(bitmap_len / BLOCKSIZE).unwrap();
        bitmap[bitmap_len - remainder..bitmap_len].copy_from_slice(&block.as_ref()[0..remainder]);
    }
    Bitmap { bitmap }
}

pub fn write_bitmap_to_disk<const BLOCKSIZE: usize, const CAPACITY: usize>(
    disk: &Disk<BLOCKSIZE, CAPACITY>,
    bitmap: &Bitmap<BLOCKSIZE, CAPACITY>,
) {
    for i in 0..bitmap.bitmap.len() / (8 * BLOCKSIZE) {
        println!("Writing block {}", i);
        let mut block = [0; BLOCKSIZE];
        println!("Copying from {} to {}", i * BLOCKSIZE, (i + 1) * BLOCKSIZE);
        block.copy_from_slice(&bitmap.bitmap[i * BLOCKSIZE..(i + 1) * BLOCKSIZE]);
        disk.write_block(i as usize, block.as_ref()).unwrap();
    }
    let remainder = bitmap.bitmap.len() % (8 * BLOCKSIZE);
    if remainder != 0 {
        let mut block = [0; BLOCKSIZE];
        block[0..remainder].copy_from_slice(&bitmap.bitmap[0..remainder]);
        disk.write_block(bitmap.bitmap.len() / (8 * BLOCKSIZE), block.as_ref())
            .unwrap();
    }
}

#[cfg(test)]
mod tests {
    use std::time::Instant;

    use disk::Disk;
    use rand::Rng;

    use crate::free_space_manager::bitmap::{read_bitmap_from_disk, write_bitmap_to_disk};

    use super::Bitmap;

    #[test]
    fn test_bitmap() {
        let disk = Disk::<512, 65536>::create("test_bitmap").unwrap();
        let mut bitmap = Bitmap::new();
        bitmap.bitmap[0] = 0b00000001;
        bitmap.bitmap[1] = 0b00000010;
        bitmap.bitmap[2] = 0b00000100;
        bitmap.bitmap[3] = 0b00001000;
        println!("{:?}", bitmap);
        write_bitmap_to_disk(&disk, &bitmap);
        let block = disk.read_block(0).unwrap();
        println!("{:?}", block);
        let bitmap = read_bitmap_from_disk(&disk);
        println!("{:?}", bitmap);
        assert_eq!(bitmap.bitmap[0], 0b00000001);
        assert_eq!(bitmap.bitmap[1], 0b00000010);
        assert_eq!(bitmap.bitmap[2], 0b00000100);
        assert_eq!(bitmap.bitmap[3], 0b00001000);
    }

    #[test]
    fn test_allocate_deallocate() {
        let start = Instant::now();
        let mut bitmap = Bitmap::<512, 65536>::new();
        let block = bitmap.allocate().unwrap();
        assert_eq!(block, 1);
        bitmap.deallocate(block);
        let block = bitmap.allocate().unwrap();
        assert_eq!(block, 1);
        bitmap.deallocate(block);

        for i in 0..65536 / 512 - 1 {
            let block = bitmap.allocate().unwrap();
            assert_eq!(block, i + 1);
        }
        assert!(bitmap.allocate().is_none());
        for i in 0..65536 / 512 - 1 {
            bitmap.deallocate(i + 1);
        }

        let mut rng = rand::thread_rng();
        let mut allocated_blocks = Vec::new();
        let first_limit = rng.gen_range(0..50);
        for _i in 0..first_limit {
            let block = bitmap.allocate().unwrap();
            allocated_blocks.push(block);
        }
        let second_limit = rng.gen_range(0..first_limit);
        for _ in 0..second_limit {
            let block = allocated_blocks.pop().unwrap();
            bitmap.deallocate(block);
        }
        let first_limit = rng.gen_range(0..100);
        for _ in 0..first_limit {
            let block = bitmap.allocate().unwrap();
            allocated_blocks.push(block);
        }
        let second_limit = rng.gen_range(0..first_limit);
        for _ in 0..second_limit {
            let block = allocated_blocks.pop().unwrap();
            bitmap.deallocate(block);
        }
        for block in allocated_blocks {
            bitmap.deallocate(block);
        }

        let end = Instant::now();
        println!("Time elapsed: {:?}", end - start);
    }
}
