use eyre::{eyre, Context, Result};
use positioned_io::{ReadAt, WriteAt};
use std::fs::{File, OpenOptions};

use crate::btree::Node;
use crate::table::Row;

pub const PAGE_SIZE: usize = 4096;
const TABLE_MAX_PAGES: usize = 100;

// a bit of a hack to get around issue of
// Option<Box<Node>> not implementing Copy
const PAGE_INIT: Option<Box<Node>> = None;

#[derive(Debug)]
pub struct Pager {
    file_descriptor: File,
    file_length: usize,
    pages: [Option<Box<Node>>; TABLE_MAX_PAGES],
    num_pages: usize,
}

impl Pager {
    pub fn new(filename: &str) -> Result<Self> {
        let file = OpenOptions::new()
            .create(true)
            .read(true)
            .write(true)
            .open(filename)
            .wrap_err("Could not open file.")?;
        let file_length = file.metadata()?.len() as usize;

        if file_length == 0 {
            // New database file. Initialize page 0 as leaf node.
        }
        if file_length % PAGE_SIZE != 0 {
            return Err(eyre!(
                "DB file is not a whole number of pages. Corrupt file."
            ));
        }

        return Ok(Self {
            file_descriptor: file,
            file_length: file_length,
            pages: [PAGE_INIT; TABLE_MAX_PAGES],
            num_pages: file_length / PAGE_SIZE,
        });
    }

    fn read_from_file(&self, page_num: usize) -> Result<Node> {
        // count number of pages, rounding up
        // in case of a partial page at the end
        // of the file
        let num_pages = self.file_length / PAGE_SIZE + (self.file_length % PAGE_SIZE != 0) as usize;

        if page_num < num_pages {
            let mut buf = vec![0; PAGE_SIZE];
            let bytes_read = self
                .file_descriptor
                .read_at((page_num * PAGE_SIZE) as u64, &mut buf);
            match bytes_read {
                Err(_) => Err(eyre!("Error reading page from file.")),
                Ok(_) => return Ok(Node::deserialize(&buf)),
            }
        } else {
            return Err(eyre!("Tried to access non-existent page."));
        }
    }

    pub fn get_page(&mut self, page_num: usize) -> Option<&Box<Node>> {
        if page_num >= TABLE_MAX_PAGES {
            return None;
        }

        if self.pages[page_num].is_none() {
            if page_num >= self.num_pages {
                // page does not exist yet; allocate
                // new one
                let mut node = Node::new(true);
                if self.num_pages == 0 {
                    match node {
                        Node::Internal(_) => (),
                        Node::Leaf(ref mut nd) => {
                            nd.is_root = true;
                        }
                    }
                }
                self.pages[page_num] = Some(Box::new(node));
                self.num_pages += 1;
            } else {
                // cache miss; allocate memory and load
                // from file
                let page = self
                    .read_from_file(page_num)
                    .expect("Error reading page from file");
                self.pages[page_num] = Some(Box::new(page));
            }
        }
        return self.pages[page_num].as_ref();
    }

    pub fn get_page_mut(&mut self, page_num: usize) -> Option<&mut Box<Node>> {
        if page_num >= TABLE_MAX_PAGES {
            return None;
        }

        if self.pages[page_num].is_none() {
            if page_num >= self.num_pages {
                // page does not exist yet; allocate
                // new one
                self.pages[page_num] = Some(Box::new(Node::new(true)));
            } else {
                // cache miss; allocate memory and load
                // from file
                let page = self
                    .read_from_file(page_num)
                    .expect("Error reading page from file");
                self.pages[page_num] = Some(Box::new(page));
            }
        }
        return self.pages[page_num].as_mut();
    }

    pub fn insert(&mut self, page_num: usize, cell_num: usize, key: u32, row: Row) -> Result<()> {
        let node = self.get_page_mut(page_num).unwrap();
        match node.as_mut() {
            Node::Internal(_) => (),
            Node::Leaf(node) => {
                node.insert(cell_num, key, row)?;
            }
        }
        return Ok(());
    }
}

impl Drop for Pager {
    fn drop(&mut self) {
        for (i, page) in self.pages.iter().enumerate() {
            if let Some(pg) = page {
                let bytes = pg.serialize();
                self.file_descriptor
                    .write_all_at((i * PAGE_SIZE) as u64, &bytes)
                    .expect("Error writing data to file.");
            }
        }
    }
}
