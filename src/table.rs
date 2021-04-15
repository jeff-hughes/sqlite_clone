use eyre::{eyre, Context, Result};
use positioned_io::{ReadAt, WriteAt};
use std::{
    convert::TryInto,
    fs::{File, OpenOptions},
};

use crate::btree::Node;

pub const ID_SIZE: usize = std::mem::size_of::<u32>();
pub const USERNAME_SIZE: usize = 32;
pub const EMAIL_SIZE: usize = 255;

const ID_OFFSET: usize = 0;
const USERNAME_OFFSET: usize = ID_OFFSET + ID_SIZE;
const EMAIL_OFFSET: usize = USERNAME_OFFSET + USERNAME_SIZE;
pub const ROW_SIZE: usize = ID_SIZE + USERNAME_SIZE + EMAIL_SIZE;

pub const PAGE_SIZE: usize = 4096;
const TABLE_MAX_PAGES: usize = 100;
const ROWS_PER_PAGE: usize = PAGE_SIZE / ROW_SIZE;
pub const TABLE_MAX_ROWS: usize = ROWS_PER_PAGE * TABLE_MAX_PAGES;

// a bit of a hack to get around issue of
// Option<Box<Page>> not implementing Copy
const PAGE_INIT: Option<Box<Node>> = None;

#[derive(Debug, Clone, Copy)]
pub struct Row {
    id: u32,
    username: [u8; USERNAME_SIZE],
    email: [u8; EMAIL_SIZE],
}

impl Row {
    pub fn new(id: u32, username: String, email: String) -> Self {
        let mut username_arr = [u8::default(); USERNAME_SIZE];
        for (i, b) in username.bytes().take(USERNAME_SIZE).enumerate() {
            username_arr[i] = b;
        }

        let mut email_arr = [u8::default(); EMAIL_SIZE];
        for (i, b) in email.bytes().take(EMAIL_SIZE).enumerate() {
            email_arr[i] = b;
        }
        return Self {
            id: id,
            username: username_arr,
            email: email_arr,
        };
    }

    pub fn serialize(&self) -> Vec<u8> {
        let mut output = Vec::new();
        output.extend(&self.id.to_le_bytes());
        output.extend(&self.username);
        output.extend(&self.email);
        return output;
    }

    pub fn deserialize(bytes: &[u8]) -> Self {
        let id = u32::from_le_bytes(
            bytes[ID_OFFSET..ID_SIZE]
                .try_into()
                .expect("Slice with incorrect length"),
        );
        let username = bytes[USERNAME_OFFSET..USERNAME_OFFSET + USERNAME_SIZE]
            .try_into()
            .expect("Slice with incorrect length");
        let email = bytes[EMAIL_OFFSET..EMAIL_OFFSET + EMAIL_SIZE]
            .try_into()
            .expect("Slice with incorrect length");
        return Self {
            id: id,
            username: username,
            email: email,
        };
    }
}

impl Default for Row {
    fn default() -> Self {
        return Self {
            id: u32::default(),
            username: [u8::default(); USERNAME_SIZE],
            email: [u8::default(); EMAIL_SIZE],
        };
    }
}

#[derive(Debug, Clone, Copy)]
struct Page {
    rows: [Row; ROWS_PER_PAGE],
}

impl Page {
    pub fn serialize(&self) -> [u8; PAGE_SIZE] {
        // always output an array of PAGE_SIZE, even
        // if page is not full
        let mut output = [u8::default(); PAGE_SIZE];
        for (i, row) in self.rows.iter().enumerate() {
            let bytes = row.serialize();
            let start_pos = i * ROW_SIZE;
            for j in 0..bytes.len() {
                output[start_pos + j] = bytes[j];
            }
        }
        return output;
    }

    pub fn deserialize(bytes: &[u8]) -> Self {
        let mut rows = [Row::default(); ROWS_PER_PAGE];
        for i in 0..ROWS_PER_PAGE {
            let start = i * ROW_SIZE;
            let end = start + ROW_SIZE;
            if start >= bytes.len() || end >= bytes.len() {
                break;
            }
            rows[i] = Row::deserialize(&bytes[(i * ROW_SIZE)..(i * ROW_SIZE + ROW_SIZE)]);
        }
        return Self { rows: rows };
    }
}

impl Default for Page {
    fn default() -> Self {
        return Self {
            rows: [Row::default(); ROWS_PER_PAGE],
        };
    }
}

#[derive(Debug)]
struct Pager {
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

#[derive(Debug)]
pub struct Table {
    pager: Pager,
    root_page_num: usize,
    cursor: Cursor,
}

impl Table {
    pub fn new(filename: &str) -> Result<Self> {
        let pager = Pager::new(filename)?;
        return Ok(Self {
            pager: pager,
            root_page_num: 0,
            cursor: Cursor::new(),
        });
    }

    pub fn execute_insert(&mut self, row: Row) -> Result<String> {
        let root_node = self.pager.get_page(self.root_page_num).unwrap();
        match root_node.as_ref() {
            Node::Internal(_) => (),
            Node::Leaf(node) => {
                if node.num_cells() >= crate::btree::LEAF_NODE_MAX_CELLS {
                    return Err(eyre!("Table full."));
                }
            }
        }

        self.cursor_move_to_end();
        self.pager
            .insert(self.cursor.page_num, self.cursor.cell_num, row.id, row)?;
        return Ok("Executed.".to_string());
    }

    pub fn execute_select(&mut self) -> Result<String> {
        let mut output = String::new();
        let mut first = true;
        self.cursor_move_to_start();
        while !self.cursor_at_end() {
            let row = self.cursor_value().unwrap();
            let username = std::str::from_utf8(&row.username)
                .unwrap()
                .trim_matches(char::from(0));
            let email = std::str::from_utf8(&row.email)
                .unwrap()
                .trim_matches(char::from(0));
            if first {
                output = format!("({}, {}, {})", row.id, username, email);
            } else {
                output = format!("{}\n({}, {}, {})", output, row.id, username, email);
            }
            first = false;
            self.cursor_advance();
        }
        return Ok(output);
    }

    fn cursor_move_to_start(&mut self) {
        self.cursor.page_num = self.root_page_num;
        self.cursor.cell_num = 0;

        let root_node = self.pager.get_page(self.root_page_num).unwrap();
        match root_node.as_ref() {
            Node::Internal(_) => (),
            Node::Leaf(node) => {
                self.cursor.at_end = node.num_cells() == 0;
            }
        }
    }

    fn cursor_move_to_end(&mut self) {
        self.cursor.page_num = self.root_page_num;
        let root_node = self.pager.get_page(self.root_page_num).unwrap();
        match root_node.as_ref() {
            Node::Internal(_) => (),
            Node::Leaf(node) => {
                self.cursor.cell_num = node.num_cells();
                self.cursor.at_end = true;
            }
        }
    }

    fn cursor_value(&mut self) -> Option<&Row> {
        let node = self.pager.get_page(self.cursor.page_num).unwrap();
        match node.as_ref() {
            Node::Internal(_) => None,
            Node::Leaf(node) => {
                return Some(&node.get_value(self.cursor.cell_num));
            }
        }
    }

    fn cursor_advance(&mut self) {
        let node = self.pager.get_page(self.cursor.page_num).unwrap();
        self.cursor.cell_num += 1;
        match node.as_ref() {
            Node::Internal(_) => (),
            Node::Leaf(nd) => {
                if self.cursor.cell_num >= nd.num_cells() {
                    self.cursor.at_end = true;
                }
            }
        }
    }

    fn cursor_at_end(&self) -> bool {
        return self.cursor.at_end;
    }
}

#[derive(Debug)]
struct Cursor {
    page_num: usize,
    cell_num: usize,
    at_end: bool,
}

impl Cursor {
    pub fn new() -> Self {
        return Self {
            page_num: 0,
            cell_num: 0,
            at_end: false, // indicates a position one past the last row
        };
    }
}
