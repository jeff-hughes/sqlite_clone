use eyre::{eyre, Result};
use std::convert::TryInto;

use crate::btree::Node;
use crate::pager::Pager;

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
