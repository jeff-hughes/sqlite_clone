use eyre::{eyre, Context, Result};
use positioned_io::{ReadAt, WriteAt};
use std::{
    convert::TryInto,
    fs::{File, OpenOptions},
};

pub const ID_SIZE: usize = std::mem::size_of::<u32>();
pub const USERNAME_SIZE: usize = 32;
pub const EMAIL_SIZE: usize = 255;

const ID_OFFSET: usize = 0;
const USERNAME_OFFSET: usize = ID_OFFSET + ID_SIZE;
const EMAIL_OFFSET: usize = USERNAME_OFFSET + USERNAME_SIZE;
const ROW_SIZE: usize = ID_SIZE + USERNAME_SIZE + EMAIL_SIZE;

const PAGE_SIZE: usize = 4096;
const TABLE_MAX_PAGES: usize = 100;
const ROWS_PER_PAGE: usize = PAGE_SIZE / ROW_SIZE;
pub const TABLE_MAX_ROWS: usize = ROWS_PER_PAGE * TABLE_MAX_PAGES;

// a bit of a hack to get around issue of
// Option<Box<Page>> not implementing Copy
const PAGE_INIT: Option<Box<Page>> = None;

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
    pub fn serialize(&self) -> Vec<u8> {
        let mut output = Vec::new();
        for row in self.rows.iter() {
            output.append(&mut row.serialize());
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
    pages: [Option<Box<Page>>; TABLE_MAX_PAGES],
}

impl Pager {
    pub fn new(filename: &str) -> Result<Self> {
        let file = OpenOptions::new()
            .create(true)
            .read(true)
            .write(true)
            .open(filename)
            .wrap_err("Could not open file.")?;
        let file_length = file.metadata()?.len();

        return Ok(Self {
            file_descriptor: file,
            file_length: file_length as usize,
            pages: [PAGE_INIT; TABLE_MAX_PAGES],
        });
    }

    pub fn num_rows(&self) -> usize {
        return self.file_length / ROW_SIZE;
    }

    fn read_from_file(&self, page_num: usize) -> Result<Page> {
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
                Ok(_) => return Ok(Page::deserialize(&buf)),
            }
        } else {
            return Err(eyre!("Tried to access non-existent page."));
        }
    }

    pub fn get_page(&mut self, page_num: usize) -> Option<&Box<Page>> {
        if page_num >= TABLE_MAX_PAGES {
            return None;
        }

        // count number of pages, rounding up in case of
        // a partial page at the end of the file
        let num_pages = self.file_length / PAGE_SIZE + (self.file_length % PAGE_SIZE != 0) as usize;

        if self.pages[page_num].is_none() {
            if page_num >= num_pages {
                // page does not exist yet; allocate
                // new one
                self.pages[page_num] = Some(Box::new(Page::default()));
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

    pub fn get_page_mut(&mut self, page_num: usize) -> Option<&mut Box<Page>> {
        if page_num >= TABLE_MAX_PAGES {
            return None;
        }

        // count number of pages, rounding up in case of
        // a partial page at the end of the file
        let num_pages = self.file_length / PAGE_SIZE + (self.file_length % PAGE_SIZE != 0) as usize;

        if self.pages[page_num].is_none() {
            if page_num >= num_pages {
                // page does not exist yet; allocate
                // new one
                self.pages[page_num] = Some(Box::new(Page::default()));
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
}

#[derive(Debug)]
pub struct Table {
    num_rows: usize,
    pager: Pager,
}

impl Table {
    pub fn new(filename: &str) -> Result<Self> {
        let pager = Pager::new(filename)?;
        return Ok(Self {
            num_rows: pager.num_rows(),
            pager: pager,
        });
    }

    fn get_page(&mut self, row_num: &usize) -> Option<&Box<Page>> {
        return self.pager.get_page(row_num / ROWS_PER_PAGE);
    }

    fn get_page_mut(&mut self, row_num: &usize) -> Option<&mut Box<Page>> {
        return self.pager.get_page_mut(row_num / ROWS_PER_PAGE);
    }

    fn get_row(&mut self, row_num: &usize) -> &Row {
        let row_offset = row_num % ROWS_PER_PAGE;
        let page = self.get_page(row_num).unwrap();
        return &page.rows[row_offset];
    }

    fn get_row_mut(&mut self, row_num: &usize) -> &mut Row {
        let row_offset = row_num % ROWS_PER_PAGE;
        let page = self.get_page_mut(row_num).unwrap();
        return &mut page.rows[row_offset];
    }

    pub fn execute_insert(&mut self, row: Row) -> Result<String> {
        if self.num_rows >= TABLE_MAX_ROWS {
            return Err(eyre!("Table full."));
        }

        let row_slot = self.get_row_mut(&self.num_rows.clone());
        *row_slot = row;
        self.num_rows += 1;
        return Ok("Executed.".to_string());
    }

    pub fn execute_select(&mut self) -> Result<String> {
        let mut output = String::new();
        for i in 0..self.num_rows {
            let row = self.get_row(&i);
            let username = std::str::from_utf8(&row.username)
                .unwrap()
                .trim_matches(char::from(0));
            let email = std::str::from_utf8(&row.email)
                .unwrap()
                .trim_matches(char::from(0));
            if i == 0 {
                output = format!("({}, {}, {})", row.id, username, email);
            } else {
                output = format!("{}\n({}, {}, {})", output, row.id, username, email);
            }
        }
        return Ok(output);
    }
}

impl Drop for Table {
    fn drop(&mut self) {
        // TODO: this should be done at the Pager level,
        // but we have to capture those partial pages
        // for now
        let num_full_pages = self.num_rows / ROWS_PER_PAGE;
        //for (i, page) in self.pager.pages.iter().enumerate() {
        for i in 0..num_full_pages {
            if let Some(pg) = &self.pager.pages[i] {
                let bytes = pg.serialize();
                self.pager
                    .file_descriptor
                    .write_all_at((i * PAGE_SIZE) as u64, &bytes)
                    .expect("Error writing data to file.");
            }
        }

        // maybe an extra partial page to write
        let num_additional_rows = self.num_rows % ROWS_PER_PAGE;
        if num_additional_rows > 0 {
            if let Some(pg) = &self.pager.pages[num_full_pages] {
                let bytes = &pg.serialize()[0..(num_additional_rows * ROW_SIZE)];
                self.pager
                    .file_descriptor
                    .write_all_at((num_full_pages * PAGE_SIZE) as u64, &bytes)
                    .expect("Error writing data to file.");
            }
        }
    }
}
