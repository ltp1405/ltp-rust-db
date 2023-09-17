use std::collections::HashMap;

#[derive(Debug, Clone, PartialEq, Eq)]
struct BufferPage {
    page_number: usize,
    next: Option<Box<BufferPage>>,
    pinned: bool,
    modified: bool,
}

struct BufferPool {
    size: usize,
    head: Option<Box<BufferPage>>,
    tail: Option<Box<BufferPage>>,
    page_table: HashMap<usize, Box<BufferPage>>,
}

impl BufferPool {
    fn new(size: usize) -> Self {
        Self {
            size,
            head: None,
            tail: None,
            page_table: HashMap::new(),
        }
    }

    fn add_page(&mut self, page_number: usize) {
        let page = Box::new(BufferPage {
            page_number,
            next: None,
            pinned: false,
            modified: false,
        });

        if self.head.is_none() {
            self.head = Some(page);
            self.tail = Some(page);
        } else {
            self.tail.as_mut().unwrap().next = Some(page);
            self.tail = Some(page);
        }

        self.page_table.insert(page_number, page);
    }

    fn get_page(&self, page_number: usize) -> Option<&Box<BufferPage>> {
        self.page_table.get(&page_number)
    }

    fn remove_page(&mut self, page: &Box<BufferPage>) {
        if self.head.as_ref() == Some(page) {
            self.head = self.head.as_ref().unwrap().next;
        } else {
            let mut prev = self.head.as_ref().unwrap();
            while prev.as_ref().unwrap().next != Some(page) {
                prev = prev.as_ref().unwrap().next;
            }
            prev.as_mut().unwrap().next = page.next.take();
        }

        if self.tail.as_ref() == Some(page) {
            self.tail = self.tail.as_ref().unwrap().next;
        }

        self.page_table.remove(&page.page_number);
    }

    fn flush_buffer_pool(&mut self) {
        for page in self.page_table.values() {
            if page.modified {
                // Write the page to disk.
            }
        }
    }
}
