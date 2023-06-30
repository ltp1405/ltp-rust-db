use super::cell::Cell;

pub trait Cursor {
    fn read(&mut self) -> Cell;
    fn next(&mut self);
}
