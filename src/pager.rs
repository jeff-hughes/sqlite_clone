use eyre::{eyre, Context, Result};
use lru::LruCache;
use positioned_io::ReadAt;
use std::fs::{File, OpenOptions};

use crate::btree::BtreePage;
use crate::DbOptions;

#[derive(Debug)]
pub struct Pager {
    file_descriptor: File,
    file_length: usize,
    cache: LruCache<usize, BtreePage>,
    num_pages: usize,
    page_size: usize,
    reserved_space: u8,
}

impl Pager {
    pub fn new(filename: &str, db_options: &DbOptions) -> Result<Self> {
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
        if file_length % db_options.page_size != 0 {
            return Err(eyre!(
                "DB file is not a whole number of pages. Corrupt file."
            ));
        }

        return Ok(Self {
            file_descriptor: file,
            file_length: file_length,
            cache: LruCache::new(10), // TODO: Change the max size later
            num_pages: file_length / db_options.page_size,
            page_size: db_options.page_size,
            reserved_space: db_options.reserved_space,
        });
    }

    fn read_from_file(&self, page_num: usize) -> Result<Vec<u8>> {
        if page_num < self.num_pages {
            let mut page = vec![0; self.page_size];
            let _ = self
                .file_descriptor
                .read_at((page_num * self.page_size) as u64, &mut page)?;
            return Ok(page);
        } else {
            return Err(eyre!("Tried to access non-existent page."));
        }
    }

    pub fn get_page(&mut self, page_num: usize) -> Result<&BtreePage> {
        let page_num = page_num - 1; // SQLite pages start at 1
        if page_num >= self.num_pages {
            return Err(eyre!("Trying to access page that does not exist."));
        }
        if self.cache.peek(&page_num).is_none() {
            // if page_num >= self.num_pages {
            //     // page does not exist yet; allocate
            //     // new one
            //     let page = Page::with_capacity(self.page_size);
            //     self.cache.put(page_num, page);
            //     self.num_pages += 1;
            // } else {
            // cache miss; allocate memory and load
            // from file
            let page = self.read_from_file(page_num)?;
            let parsed_page =
                BtreePage::parse(&page, page_num, self.page_size, self.reserved_space)?;
            self.cache.put(page_num, parsed_page);
            // }
        }
        return Ok(self.cache.get(&page_num).unwrap());
    }

    pub fn get_page_mut(&mut self, page_num: usize) -> Result<&mut BtreePage> {
        let page_num = page_num - 1; // SQLite pages start at 1
        if page_num >= self.num_pages {
            return Err(eyre!("Trying to access page that does not exist."));
        }
        if self.cache.peek(&page_num).is_none() {
            // if page_num >= self.num_pages {
            //     // page does not exist yet; allocate
            //     // new one
            //     let page = Page::with_capacity(self.page_size);
            //     self.cache.put(page_num, page);
            //     self.num_pages += 1;
            // } else {
            // cache miss; allocate memory and load
            // from file
            let page = self.read_from_file(page_num)?;
            let parsed_page =
                BtreePage::parse(&page, page_num, self.page_size, self.reserved_space)?;
            self.cache.put(page_num, parsed_page);
            // }
        }
        return Ok(self.cache.get_mut(&page_num).unwrap());
    }

    // pub fn insert(&mut self, page_num: usize, cell_num: usize, key: u32, row: Row) -> Result<()> {
    //     let node = self.get_page_mut(page_num).unwrap();
    //     match node.as_mut() {
    //         Node::Internal(_) => (),
    //         Node::Leaf(node) => {
    //             node.insert(cell_num, key, row)?;
    //         }
    //     }
    //     return Ok(());
    // }
}

// impl Drop for Pager {
//     fn drop(&mut self) {
//         for (i, page) in self.pages.iter().enumerate() {
//             if let Some(pg) = page {
//                 let bytes = pg.serialize();
//                 self.file_descriptor
//                     .write_all_at((i * PAGE_SIZE) as u64, &bytes)
//                     .expect("Error writing data to file.");
//             }
//         }
//     }
// }
