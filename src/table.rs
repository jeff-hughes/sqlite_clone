use eyre::{eyre, Result};

const ID_SIZE: usize = std::mem::size_of::<u32>();
const USERNAME_SIZE: usize = 32;
const EMAIL_SIZE: usize = 255;
const ROW_SIZE: usize = ID_SIZE + USERNAME_SIZE + EMAIL_SIZE;

const PAGE_SIZE: usize = 4096;
const TABLE_MAX_PAGES: usize = 100;
const ROWS_PER_PAGE: usize = PAGE_SIZE / ROW_SIZE;
const TABLE_MAX_ROWS: usize = ROWS_PER_PAGE * TABLE_MAX_PAGES;

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

impl Default for Page {
    fn default() -> Self {
        return Self {
            rows: [Row::default(); ROWS_PER_PAGE],
        };
    }
}

#[derive(Debug, Clone)]
pub struct Table {
    num_rows: usize,
    pages: [Option<Box<Page>>; TABLE_MAX_PAGES],
}

impl Table {
    pub fn new() -> Self {
        return Self {
            num_rows: 0,
            pages: [PAGE_INIT; TABLE_MAX_PAGES],
        };
    }

    fn get_page(&self, row_num: &usize) -> Option<&Box<Page>> {
        let page_num = row_num / ROWS_PER_PAGE;
        return self.pages[page_num].as_ref();
    }

    fn get_page_mut(&mut self, row_num: &usize) -> Option<&mut Box<Page>> {
        let page_num = row_num / ROWS_PER_PAGE;
        if self.pages[page_num].is_none() {
            self.pages[page_num] = Some(Box::new(Page::default()));
        }
        return self.pages[page_num].as_mut();
    }

    fn get_row(&self, row_num: &usize) -> &Row {
        let row_offset = row_num % ROWS_PER_PAGE;
        let page = self.get_page(row_num).unwrap();
        return &page.rows[row_offset];
    }

    fn get_row_mut(&mut self, row_num: &usize) -> &mut Row {
        let row_offset = row_num % ROWS_PER_PAGE;
        let page = self.get_page_mut(row_num).unwrap();
        return &mut page.rows[row_offset];
    }

    pub fn execute_insert(&mut self, row: Row) -> Result<()> {
        if self.num_rows >= TABLE_MAX_ROWS {
            return Err(eyre!("Table full."));
        }

        let row_slot = self.get_row_mut(&self.num_rows.clone());
        *row_slot = row;
        self.num_rows += 1;
        return Ok(());
    }

    pub fn execute_select(&mut self) -> Result<()> {
        for i in 0..self.num_rows {
            let row = self.get_row(&i);
            let username = std::str::from_utf8(&row.username).unwrap();
            let email = std::str::from_utf8(&row.email).unwrap();
            println!("({}, {}, {})", row.id, username, email);
        }
        return Ok(());
    }
}
