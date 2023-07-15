/// The address of a cell on disk.
#[derive(Debug, Clone, Copy)]
pub struct CellAddress {
    pub block_number: usize,
    pub offset: usize,
}
