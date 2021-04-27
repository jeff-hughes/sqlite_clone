use derive_try_from_primitive::TryFromPrimitive;
use eyre::Result;
use std::cell::RefCell;
use std::cmp::Ordering;
use std::convert::TryFrom;
use std::rc::Rc;

use crate::datatypes::*;
use crate::pager::Pager;
use crate::parsing;
use crate::DbOptions;

#[derive(Debug)]
pub struct Btree<'a> {
    pub name: String,
    pub table_name: String,
    pub root_page: usize,
    db_options: &'a DbOptions,
    pager: Rc<RefCell<Pager>>,
}

impl<'a> Btree<'a> {
    pub fn new(
        name: String,
        table_name: String,
        root_page: usize,
        db_options: &'a DbOptions,
        pager: Rc<RefCell<Pager>>,
    ) -> Self {
        return Self {
            name: name,
            table_name: table_name,
            root_page: root_page,
            db_options: db_options,
            pager: pager,
        };
    }

    pub fn get_row(&self, row_id: VarInt) -> Option<Record> {
        return self.get_row_rcrs(row_id, self.root_page);
    }

    fn get_row_rcrs(&self, row_id: VarInt, page_num: usize) -> Option<Record> {
        let page = self.get_page(page_num);
        if let Err(_) = page {
            return None;
        }
        match page.unwrap() {
            BtreePage::TableLeaf(pg) => {
                for (row, rec) in pg.iter() {
                    if row == row_id {
                        return Some(rec);
                    }
                }
                return None;
            }
            BtreePage::TableInterior(pg) => {
                let mut child_page = None;
                for (child_ptr, key) in pg.iter() {
                    if row_id <= key {
                        child_page = Some(child_ptr);
                        break;
                    }
                }
                if child_page.is_none() {
                    child_page = Some(pg.header.right_pointer.unwrap());
                }
                return self.get_row_rcrs(row_id, child_page.unwrap() as usize);
            }
            _ => return None, // not defined for index pages
        }
    }

    pub fn get_index(&self, index: Record) -> Option<Record> {
        return self.get_index_rcrs(index, self.root_page);
    }

    fn get_index_rcrs(&self, index: Record, page_num: usize) -> Option<Record> {
        let page = self.get_page(page_num);
        if let Err(_) = page {
            return None;
        }
        match page.unwrap() {
            BtreePage::IndexLeaf(pg) => {
                for record in pg.iter() {
                    if index == record {
                        return Some(record);
                    }
                }
                return None;
            }
            BtreePage::IndexInterior(pg) => {
                let mut child_page = None;
                for (child_ptr, record) in pg.iter() {
                    if index == record {
                        return Some(record);
                    } else if index <= record {
                        child_page = Some(child_ptr);
                        break;
                    }
                }
                if child_page.is_none() {
                    child_page = Some(pg.header.right_pointer.unwrap());
                }
                return self.get_index_rcrs(index, child_page.unwrap() as usize);
            }
            _ => return None, // not defined for table pages
        }
    }

    pub fn list_records(&self) -> Vec<(VarInt, Record)> {
        return self.list_records_rcrs(self.root_page);
    }

    fn list_records_rcrs(&self, page_num: usize) -> Vec<(VarInt, Record)> {
        let mut output = Vec::new();
        let page = self.get_page(page_num);
        if let Err(_) = page {
            return output;
        }
        match page.unwrap() {
            BtreePage::TableLeaf(pg) => {
                for row in pg.iter() {
                    output.push(row);
                }
            }
            BtreePage::TableInterior(pg) => {
                for (ptr, _) in pg.iter() {
                    output.append(&mut self.list_records_rcrs(ptr as usize));
                }
            }
            _ => (), // TODO: define for index pages
        }
        return output;
    }

    fn get_page(&self, page_num: usize) -> Result<BtreePage> {
        let mut pager = self.pager.borrow_mut();
        let page = pager.get_page(page_num)?;
        return Ok((*page).clone()); // TODO: get rid of clone
    }
}

#[derive(Debug, Clone)]
pub enum BtreePage {
    TableLeaf(TableLeafPage),
    IndexLeaf(IndexLeafPage),
    TableInterior(TableInteriorPage),
    IndexInterior(IndexInteriorPage),
}

impl BtreePage {
    pub fn new(page_type: PageType, page_size: usize, reserved_space: u8) -> Self {
        let page_header = PageHeader::new(page_type, page_size, reserved_space);
        return match page_type {
            PageType::TableLeaf => Self::TableLeaf(TableLeafPage::new(
                page_header,
                &Vec::new(),
                page_size,
                reserved_space,
            )),
            PageType::IndexLeaf => Self::IndexLeaf(IndexLeafPage::new(
                page_header,
                &Vec::new(),
                page_size,
                reserved_space,
            )),
            PageType::TableInterior => {
                Self::TableInterior(TableInteriorPage::new(page_header, &Vec::new()))
            }
            PageType::IndexInterior => Self::IndexInterior(IndexInteriorPage::new(
                page_header,
                &Vec::new(),
                page_size,
                reserved_space,
            )),
        };
    }

    pub fn deserialize(
        i: &[u8],
        page_num: usize,
        page_size: usize,
        reserved_space: u8,
    ) -> Result<Self> {
        let offset = if page_num == 1 { 100 } else { 0 };
        let header = PageHeader::deserialize(&i[offset..], offset)?;
        match header.page_type {
            PageType::TableLeaf => Ok(Self::TableLeaf(TableLeafPage::new(
                header,
                i,
                page_size,
                reserved_space,
            ))),
            PageType::IndexLeaf => Ok(Self::IndexLeaf(IndexLeafPage::new(
                header,
                i,
                page_size,
                reserved_space,
            ))),
            PageType::TableInterior => Ok(Self::TableInterior(TableInteriorPage::new(header, i))),
            PageType::IndexInterior => Ok(Self::IndexInterior(IndexInteriorPage::new(
                header,
                i,
                page_size,
                reserved_space,
            ))),
        }
    }

    pub fn serialize(&self) -> Vec<u8> {
        return match self {
            Self::TableLeaf(pg) => pg.serialize(),
            Self::IndexLeaf(pg) => pg.serialize(),
            Self::TableInterior(pg) => pg.serialize(),
            Self::IndexInterior(pg) => pg.serialize(),
        };
    }

    pub fn is_interior(&self) -> bool {
        match self {
            Self::TableInterior(_) => true,
            Self::IndexInterior(_) => true,
            _ => false,
        }
    }

    pub fn is_leaf(&self) -> bool {
        match self {
            Self::TableLeaf(_) => true,
            Self::IndexLeaf(_) => true,
            _ => false,
        }
    }

    pub fn get_page_type(&self) -> String {
        match self {
            Self::TableLeaf(_) => "TableLeaf".to_string(),
            Self::IndexLeaf(_) => "IndexLeaf".to_string(),
            Self::TableInterior(_) => "TableInterior".to_string(),
            Self::IndexInterior(_) => "IndexInterior".to_string(),
        }
    }
}

#[derive(Debug, Clone)]
pub struct PageHeader {
    pub page_type: PageType,
    pub offset: usize,
    pub first_freeblock: u16,
    pub num_cells: u16,
    pub cell_start: u16,
    pub fragmented_bytes: u8,
    pub right_pointer: Option<u32>,
    pub cell_pointers: Vec<u16>,
}

impl PageHeader {
    pub fn new(page_type: PageType, page_size: usize, reserved_space: u8) -> Self {
        let cell_start = if (page_size - (reserved_space as usize)) > u16::MAX as usize {
            0
        } else {
            (page_size - (reserved_space as usize)) as u16
        };
        return Self {
            page_type: page_type,
            offset: 0,
            first_freeblock: 0,
            num_cells: 0,
            cell_start: cell_start,
            fragmented_bytes: 0,
            right_pointer: None,
            cell_pointers: Vec::new(),
        };
    }

    pub fn deserialize(i: &[u8], offset: usize) -> Result<Self> {
        let mut pos = parsing::Position::new();

        let page_type = PageType::try_from(parsing::be_u8(&i[pos.v()..pos.incr(1)])?).unwrap();
        let first_freeblock = parsing::be_u16(&i[pos.v()..pos.incr(2)])?;
        let num_cells = parsing::be_u16(&i[pos.v()..pos.incr(2)])?;
        let cell_start = parsing::be_u16(&i[pos.v()..pos.incr(2)])?;
        let fragmented_bytes = parsing::be_u8(&i[pos.v()..pos.incr(1)])?;

        let mut right_pointer = None;
        if page_type.is_interior() {
            right_pointer = Some(parsing::be_u32(&i[pos.v()..pos.incr(4)])?);
        }

        let mut cell_pointers = Vec::new();
        for _ in 0..num_cells as usize {
            cell_pointers.push(parsing::be_u16(&i[pos.v()..pos.incr(2)])?);
        }

        Ok(Self {
            page_type: page_type,
            offset: offset,
            first_freeblock: first_freeblock,
            num_cells: num_cells,
            cell_start: cell_start,
            fragmented_bytes: fragmented_bytes,
            right_pointer: right_pointer,
            cell_pointers: cell_pointers,
        })
    }

    pub fn serialize(&self) -> Vec<u8> {
        let mut output = Vec::new();
        output.push(self.page_type as u8);
        output.extend(self.first_freeblock.to_be_bytes().iter());
        output.extend(self.num_cells.to_be_bytes().iter());
        output.extend(self.cell_start.to_be_bytes().iter());
        output.push(self.fragmented_bytes);
        if let Some(ptr) = self.right_pointer {
            output.extend(ptr.to_be_bytes().iter());
        }
        for cptr in &self.cell_pointers {
            output.extend(cptr.to_be_bytes().iter());
        }
        return output;
    }
}

#[derive(Debug, Clone)]
pub struct TableLeafPage {
    pub header: PageHeader,
    pub bytes: Vec<u8>,
    pub page_size: usize,
    pub reserved_space: u8,
}

impl TableLeafPage {
    pub fn new(
        page_header: PageHeader,
        bytes: &[u8],
        page_size: usize,
        reserved_space: u8,
    ) -> Self {
        return Self {
            header: page_header,
            bytes: bytes.to_vec(),
            page_size: page_size,
            reserved_space: reserved_space,
        };
    }

    pub fn serialize(&self) -> Vec<u8> {
        let mut output = self.header.serialize();
        let offset = self.header.offset + output.len();
        output.extend(&self.bytes[offset..]);
        return output;
    }

    pub fn iter(&self) -> TableLeafIter {
        return TableLeafIter::new(&self);
    }
}

pub struct TableLeafIter<'a> {
    page: &'a TableLeafPage,
    cursor: usize,
}

impl<'a> TableLeafIter<'a> {
    pub fn new(page_ref: &'a TableLeafPage) -> Self {
        return Self {
            page: page_ref,
            cursor: 0,
        };
    }
}

impl<'a> Iterator for TableLeafIter<'a> {
    type Item = (VarInt, Record);

    fn next(&mut self) -> Option<Self::Item> {
        match self.page.header.cell_pointers.get(self.cursor) {
            None => return None,
            Some(ptr) => {
                let mut pos = parsing::Position::new();
                pos.set(*ptr as usize);
                let (payload_size, b) = VarInt::deserialize(&self.page.bytes[pos.v()..]);
                pos.incr(b);
                let (row_id, b) = VarInt::deserialize(&self.page.bytes[pos.v()..]);
                pos.incr(b);

                let payload_on_page = calc_payload_on_page(
                    self.page.page_size,
                    self.page.reserved_space as usize,
                    payload_size.0 as usize,
                    false,
                );
                let rec = Record::deserialize(&self.page.bytes[pos.v()..pos.incr(payload_on_page)])
                    .unwrap();
                self.cursor += 1;
                return Some((row_id, rec));
            }
        }
    }
}

#[derive(Debug, Clone)]
pub struct IndexLeafPage {
    pub header: PageHeader,
    pub bytes: Vec<u8>,
    pub page_size: usize,
    pub reserved_space: u8,
}

impl IndexLeafPage {
    pub fn new(
        page_header: PageHeader,
        bytes: &[u8],
        page_size: usize,
        reserved_space: u8,
    ) -> Self {
        return Self {
            header: page_header,
            bytes: bytes.to_vec(),
            page_size: page_size,
            reserved_space: reserved_space,
        };
    }

    pub fn serialize(&self) -> Vec<u8> {
        let mut output = self.header.serialize();
        let offset = self.header.offset + output.len();
        output.extend(&self.bytes[offset..]);
        return output;
    }

    pub fn iter(&self) -> IndexLeafIter {
        return IndexLeafIter::new(&self);
    }
}

pub struct IndexLeafIter<'a> {
    page: &'a IndexLeafPage,
    cursor: usize,
}

impl<'a> IndexLeafIter<'a> {
    pub fn new(page_ref: &'a IndexLeafPage) -> Self {
        return Self {
            page: page_ref,
            cursor: 0,
        };
    }
}

impl<'a> Iterator for IndexLeafIter<'a> {
    type Item = Record;

    fn next(&mut self) -> Option<Self::Item> {
        match self.page.header.cell_pointers.get(self.cursor) {
            None => return None,
            Some(ptr) => {
                let mut pos = parsing::Position::new();
                pos.set(*ptr as usize);
                let (payload_size, b) = VarInt::deserialize(&self.page.bytes[pos.v()..]);
                pos.incr(b);

                let payload_on_page = calc_payload_on_page(
                    self.page.page_size,
                    self.page.reserved_space as usize,
                    payload_size.0 as usize,
                    true,
                );
                let rec = Record::deserialize(&self.page.bytes[pos.v()..pos.incr(payload_on_page)])
                    .unwrap();
                self.cursor += 1;
                return Some(rec);
            }
        }
    }
}

#[derive(Debug, Clone)]
pub struct TableInteriorPage {
    pub header: PageHeader,
    pub bytes: Vec<u8>,
}

impl TableInteriorPage {
    pub fn new(page_header: PageHeader, bytes: &[u8]) -> Self {
        return Self {
            header: page_header,
            bytes: bytes.to_vec(),
        };
    }

    pub fn serialize(&self) -> Vec<u8> {
        let mut output = self.header.serialize();
        let offset = self.header.offset + output.len();
        output.extend(&self.bytes[offset..]);
        return output;
    }

    pub fn iter(&self) -> TableInteriorIter {
        return TableInteriorIter::new(&self);
    }
}

pub struct TableInteriorIter<'a> {
    page: &'a TableInteriorPage,
    cursor: usize,
}

impl<'a> TableInteriorIter<'a> {
    pub fn new(page_ref: &'a TableInteriorPage) -> Self {
        return Self {
            page: page_ref,
            cursor: 0,
        };
    }
}

impl<'a> Iterator for TableInteriorIter<'a> {
    type Item = (u32, VarInt);

    fn next(&mut self) -> Option<Self::Item> {
        match self.page.header.cell_pointers.get(self.cursor) {
            None => return None,
            Some(ptr) => {
                let mut pos = parsing::Position::new();
                pos.set(*ptr as usize);
                let child_ptr = parsing::be_u32(&self.page.bytes[pos.v()..pos.incr(4)]).unwrap();

                let (key, b) = VarInt::deserialize(&self.page.bytes[pos.v()..]);
                pos.incr(b);
                self.cursor += 1;
                return Some((child_ptr, key));
            }
        }
    }
}

#[derive(Debug, Clone)]
pub struct IndexInteriorPage {
    pub header: PageHeader,
    pub bytes: Vec<u8>,
    pub page_size: usize,
    pub reserved_space: u8,
}

impl IndexInteriorPage {
    pub fn new(
        page_header: PageHeader,
        bytes: &[u8],
        page_size: usize,
        reserved_space: u8,
    ) -> Self {
        return Self {
            header: page_header,
            bytes: bytes.to_vec(),
            page_size: page_size,
            reserved_space: reserved_space,
        };
    }

    pub fn serialize(&self) -> Vec<u8> {
        let mut output = self.header.serialize();
        let offset = self.header.offset + output.len();
        output.extend(&self.bytes[offset..]);
        return output;
    }

    pub fn iter(&self) -> IndexInteriorIter {
        return IndexInteriorIter::new(&self);
    }
}

pub struct IndexInteriorIter<'a> {
    page: &'a IndexInteriorPage,
    cursor: usize,
}

impl<'a> IndexInteriorIter<'a> {
    pub fn new(page_ref: &'a IndexInteriorPage) -> Self {
        return Self {
            page: page_ref,
            cursor: 0,
        };
    }
}

impl<'a> Iterator for IndexInteriorIter<'a> {
    type Item = (u32, Record);

    fn next(&mut self) -> Option<Self::Item> {
        match self.page.header.cell_pointers.get(self.cursor) {
            None => return None,
            Some(ptr) => {
                let mut pos = parsing::Position::new();
                pos.set(*ptr as usize);
                let child_ptr = parsing::be_u32(&self.page.bytes[pos.v()..pos.incr(4)]).unwrap();

                let (payload_size, b) = VarInt::deserialize(&self.page.bytes[pos.v()..]);
                pos.incr(b);

                let payload_on_page = calc_payload_on_page(
                    self.page.page_size as usize,
                    self.page.reserved_space as usize,
                    payload_size.0 as usize,
                    true,
                );

                let rec = Record::deserialize(&self.page.bytes[pos.v()..pos.incr(payload_on_page)])
                    .unwrap();
                self.cursor += 1;
                return Some((child_ptr, rec));
            }
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, TryFromPrimitive)]
#[repr(u8)]
pub enum PageType {
    IndexInterior = 0x02,
    TableInterior = 0x05,
    IndexLeaf = 0x0a,
    TableLeaf = 0x0d,
}

impl PageType {
    pub fn is_interior(&self) -> bool {
        match self {
            PageType::IndexInterior => true,
            PageType::TableInterior => true,
            _ => false,
        }
    }

    pub fn is_leaf(&self) -> bool {
        match self {
            PageType::IndexLeaf => true,
            PageType::TableLeaf => true,
            _ => false,
        }
    }
}

#[derive(Debug, Clone)]
pub struct Freeblock {
    pub next: Option<u16>,
    pub size: u16,
}

impl Freeblock {
    pub fn deserialize(i: &[u8]) -> Result<Self> {
        let mut pos = parsing::Position::new();
        let next = parsing::be_u16(&i[pos.v()..pos.incr(2)])?;
        let size = parsing::be_u16(&i[pos.v()..pos.incr(2)])?;
        return Ok(Self {
            next: if next > 0 { Some(next) } else { None },
            size: size,
        });
    }
}

#[derive(Debug, Clone)]
pub struct Record {
    pub col_types: Vec<DataType>,
    pub values: Vec<Value>,
}

impl Record {
    pub fn new(col_types: Vec<DataType>, values: Vec<Value>) -> Self {
        return Self {
            col_types: col_types,
            values: values,
        };
    }

    pub fn deserialize(i: &[u8]) -> Result<Self> {
        let mut pos = parsing::Position::new();
        let (header_size, b) = VarInt::deserialize(&i[pos.v()..]);
        pos.incr(b);
        let header_size_size = header_size.0 as usize - b;

        // get the rest of the header
        let header = &i[pos.v()..pos.incr(header_size_size)];
        let mut col_types = Vec::new();
        let mut header_left = header.len();
        pos.set(0);
        while header_left > 0 {
            let next_bytes = std::cmp::min(header_left, 9);
            let (col_type_int, b) = VarInt::deserialize(&header[pos.v()..pos.incr(next_bytes)]);
            pos.decr(next_bytes - b);
            let col_type = DataType::from_varint(col_type_int).expect("Not a valid data type.");
            col_types.push(col_type);
            header_left -= b;
        }

        let values_input = &i[header_size.0 as usize..];
        let mut values = Vec::new();
        pos.set(0);
        for col in &col_types {
            if let Some(size) = col.get_size() {
                values.push(Value::new(col, &values_input[pos.v()..pos.incr(size)]));
            }
        }

        Ok(Self {
            col_types: col_types,
            values: values,
        })
    }

    pub fn serialize(&self) -> Vec<u8> {
        let mut output = Vec::new();
        for col in &self.col_types {
            output.extend(col.to_varint().serialize());
        }
        for val in &self.values {
            output.extend(val.serialize());
        }
        return output;
    }
}

impl PartialEq for Record {
    /// Tests two Records for equality. Note that the way this is set up,
    /// comparing Records of different lengths will not be symmetric,
    /// i.e., a == b may not imply that b == a
    /// In the case of comparing Records, this is a feature, not a bug,
    /// as one of the key things we want to use this for is comparing
    /// index values, where the index stores the row number of the
    /// corresponding table value, but obviously we don't have that info
    /// when searching. In this situation, always compare
    /// search_value == index_value, so the shorter value is on the left.
    fn eq(&self, other: &Self) -> bool {
        for (i, sval) in self.values.iter().enumerate() {
            let oval = other.values.get(i);
            match oval {
                Some(oval) => {
                    if sval != oval {
                        return false;
                    }
                }
                None => return false,
            }
        }
        return true;
    }
}

impl PartialOrd for Record {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        for (i, sval) in self.values.iter().enumerate() {
            let oval = other.values.get(i);
            match oval {
                Some(oval) => {
                    if sval != oval {
                        return sval.partial_cmp(oval);
                    }
                }
                None => return Some(Ordering::Greater),
            }
        }
        return None;
    }
}

fn calc_payload_on_page(
    page_size: usize,
    reserved_space: usize,
    payload_size: usize,
    is_index_page: bool,
) -> usize {
    // the logic for these calculations is documented here, near the
    // bottom of the section:
    // https://sqlite.org/fileformat2.html#b_tree_pages
    // usable_space = U
    // max_payload = X
    // min_payload = M
    // k = K...because I honestly don't understand what this one means
    let usable_space = page_size - reserved_space;
    let max_payload = if is_index_page {
        ((usable_space - 12) * 64 / 255) - 23
    } else {
        usable_space - 35
    };
    let min_payload = ((usable_space - 12) * 32 / 255) - 23;

    let k = if payload_size < min_payload {
        min_payload
    } else {
        min_payload + (payload_size - min_payload) % (usable_space - 4)
    };
    let payload_on_page = if payload_size <= max_payload {
        payload_size
    } else if k <= max_payload {
        k
    } else {
        min_payload
    };
    return payload_on_page;
}
