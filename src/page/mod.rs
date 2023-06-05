pub mod page;
pub mod page_with_layout;
mod pager;

pub const PAGE_SIZE: usize = 1 << 14;

pub use page::Page;
pub use pager::Pager;