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
                for (row, record) in pg.records {
                    if row == row_id {
                        return Some(record);
                    }
                }
                return None;
            }
            BtreePage::TableInterior(pg) => {
                let mut child_page = None;
                for (i, key) in pg.keys.iter().enumerate() {
                    if row_id <= *key {
                        child_page = Some(pg.pointers[i]);
                        break;
                    }
                }
                if child_page.is_none() {
                    child_page = Some(*pg.pointers.last().unwrap());
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
                for record in pg.records {
                    if index == record {
                        // TODO: not going to work properly because the
                        // Record contains the row number
                        return Some(record);
                    }
                }
                return None;
            }
            BtreePage::IndexInterior(pg) => {
                let mut child_page = None;
                for (i, record) in pg.records.into_iter().enumerate() {
                    if index == record {
                        return Some(record);
                    } else if index <= record {
                        child_page = Some(pg.pointers[i]);
                        break;
                    }
                }
                if child_page.is_none() {
                    child_page = Some(*pg.pointers.last().unwrap());
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
                for row in pg.records {
                    output.push(row);
                }
            }
            BtreePage::TableInterior(pg) => {
                for ptr in pg.pointers {
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
    pub fn new(page_type: PageType, page_size: usize, reserved_space: usize) -> Self {
        let page_header = PageHeader::new(page_type, page_size, reserved_space);
        return match page_type {
            PageType::TableLeaf => Self::TableLeaf(TableLeafPage::new(page_header)),
            PageType::IndexLeaf => Self::IndexLeaf(IndexLeafPage::new(page_header)),
            PageType::TableInterior => Self::TableInterior(TableInteriorPage::new(page_header)),
            PageType::IndexInterior => Self::IndexInterior(IndexInteriorPage::new(page_header)),
        };
    }

    pub fn parse(i: &[u8], page_num: usize, page_size: usize, reserved_space: u8) -> Result<Self> {
        let offset = if page_num == 0 { 100 } else { 0 };
        let header = PageHeader::parse(&i[offset..])?;
        match header.page_type {
            PageType::TableLeaf => Ok(Self::TableLeaf(TableLeafPage::parse(
                i,
                header,
                page_size,
                reserved_space,
            )?)),
            PageType::IndexLeaf => Ok(Self::IndexLeaf(IndexLeafPage::parse(
                i,
                header,
                page_size,
                reserved_space,
            )?)),
            PageType::TableInterior => {
                Ok(Self::TableInterior(TableInteriorPage::parse(i, header)?))
            }
            PageType::IndexInterior => Ok(Self::IndexInterior(IndexInteriorPage::parse(
                i,
                header,
                page_size,
                reserved_space,
            )?)),
        }
    }

    pub fn is_interior(&self) -> bool {
        match self {
            BtreePage::TableInterior(_) => true,
            BtreePage::IndexInterior(_) => true,
            _ => false,
        }
    }

    pub fn is_leaf(&self) -> bool {
        match self {
            BtreePage::TableLeaf(_) => true,
            BtreePage::IndexLeaf(_) => true,
            _ => false,
        }
    }
}

#[derive(Debug, Clone)]
pub struct PageHeader {
    pub page_type: PageType,
    pub first_freeblock: u16,
    pub num_cells: u16,
    pub cell_start: u16,
    pub fragmented_bytes: u8,
    pub right_pointer: Option<u32>,
    pub cell_pointers: Vec<u16>,
}

impl PageHeader {
    pub fn new(page_type: PageType, page_size: usize, reserved_space: usize) -> Self {
        let cell_start = if (page_size - reserved_space) > u16::MAX as usize {
            0
        } else {
            (page_size - reserved_space) as u16
        };
        return Self {
            page_type: page_type,
            first_freeblock: 0,
            num_cells: 0,
            cell_start: cell_start,
            fragmented_bytes: 0,
            right_pointer: None,
            cell_pointers: Vec::new(),
        };
    }

    pub fn parse(i: &[u8]) -> Result<Self> {
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
            first_freeblock: first_freeblock,
            num_cells: num_cells,
            cell_start: cell_start,
            fragmented_bytes: fragmented_bytes,
            right_pointer: right_pointer,
            cell_pointers: cell_pointers,
        })
    }
}

#[derive(Debug, Clone)]
pub struct TableLeafPage {
    pub header: PageHeader,
    pub records: Vec<(VarInt, Record)>,
}

impl TableLeafPage {
    pub fn new(header: PageHeader) -> Self {
        return Self {
            header: header,
            records: Vec::new(),
        };
    }

    pub fn parse(
        i: &[u8],
        page_header: PageHeader,
        page_size: usize,
        reserved_space: u8,
    ) -> Result<Self> {
        let mut pos = parsing::Position::new();
        let mut records = Vec::new();
        for ptr in &page_header.cell_pointers {
            pos.set(*ptr as usize);
            let (payload_size, b) = VarInt::parse(&i[pos.v()..]);
            pos.incr(b);
            let (row_id, b) = VarInt::parse(&i[pos.v()..]);
            pos.incr(b);

            let payload_on_page = calc_payload_on_page(
                page_size as usize,
                reserved_space as usize,
                payload_size.0 as usize,
                false,
            );
            let rec = Record::parse(&i[pos.v()..pos.incr(payload_on_page)])?;
            records.push((row_id, rec));
        }

        Ok(Self {
            header: page_header,
            records: records,
        })
    }
}

#[derive(Debug, Clone)]
pub struct IndexLeafPage {
    pub header: PageHeader,
    pub records: Vec<Record>,
}

impl IndexLeafPage {
    pub fn new(header: PageHeader) -> Self {
        return Self {
            header: header,
            records: Vec::new(),
        };
    }

    pub fn parse(
        i: &[u8],
        page_header: PageHeader,
        page_size: usize,
        reserved_space: u8,
    ) -> Result<Self> {
        let mut pos = parsing::Position::new();
        let mut records = Vec::new();
        for ptr in &page_header.cell_pointers {
            pos.set(*ptr as usize);
            let (payload_size, b) = VarInt::parse(&i[pos.v()..]);
            pos.incr(b);

            let payload_on_page = calc_payload_on_page(
                page_size as usize,
                reserved_space as usize,
                payload_size.0 as usize,
                true,
            );
            let rec = Record::parse(&i[pos.v()..pos.incr(payload_on_page)])?;
            records.push(rec);
        }

        Ok(Self {
            header: page_header,
            records: records,
        })
    }
}

#[derive(Debug, Clone)]
pub struct TableInteriorPage {
    pub header: PageHeader,
    pub pointers: Vec<u32>,
    pub keys: Vec<VarInt>,
}

impl TableInteriorPage {
    pub fn new(header: PageHeader) -> Self {
        return Self {
            header: header,
            pointers: Vec::new(),
            keys: Vec::new(),
        };
    }

    pub fn parse(i: &[u8], page_header: PageHeader) -> Result<Self> {
        let mut pos = parsing::Position::new();
        let mut pointers = Vec::new();
        let mut keys = Vec::new();
        for ptr in &page_header.cell_pointers {
            pos.set(*ptr as usize);
            let child_ptr = parsing::be_u32(&i[pos.v()..pos.incr(4)])?;
            pointers.push(child_ptr);
            let (key, b) = VarInt::parse(&i[pos.v()..]);
            pos.incr(b);
            keys.push(key);
        }

        if let Some(right_ptr) = page_header.right_pointer {
            pointers.push(right_ptr);
        }
        Ok(Self {
            header: page_header,
            pointers: pointers,
            keys: keys,
        })
    }
}

#[derive(Debug, Clone)]
pub struct IndexInteriorPage {
    pub header: PageHeader,
    pub pointers: Vec<u32>,
    pub records: Vec<Record>,
}

impl IndexInteriorPage {
    pub fn new(header: PageHeader) -> Self {
        return Self {
            header: header,
            pointers: Vec::new(),
            records: Vec::new(),
        };
    }

    pub fn parse(
        i: &[u8],
        page_header: PageHeader,
        page_size: usize,
        reserved_space: u8,
    ) -> Result<Self> {
        let mut pos = parsing::Position::new();
        let mut pointers = Vec::new();
        let mut records = Vec::new();
        for ptr in &page_header.cell_pointers {
            pos.set(*ptr as usize);
            let child_ptr = parsing::be_u32(&i[pos.v()..pos.incr(4)])?;
            pointers.push(child_ptr);

            let (payload_size, b) = VarInt::parse(&i[pos.v()..]);
            pos.incr(b);

            let payload_on_page = calc_payload_on_page(
                page_size as usize,
                reserved_space as usize,
                payload_size.0 as usize,
                true,
            );
            let rec = Record::parse(&i[pos.v()..pos.incr(payload_on_page)])?;
            records.push(rec);
        }

        if let Some(right_ptr) = page_header.right_pointer {
            pointers.push(right_ptr);
        }
        Ok(Self {
            header: page_header,
            pointers: pointers,
            records: records,
        })
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

    pub fn parse(i: &[u8]) -> Result<Self> {
        let mut pos = parsing::Position::new();
        let (header_size, b) = VarInt::parse(&i[pos.v()..]);
        pos.incr(b);
        let header_size_size = header_size.0 as usize - b;

        // get the rest of the header
        let header = &i[pos.v()..pos.incr(header_size_size)];
        let mut col_types = Vec::new();
        let mut header_left = header.len();
        pos.set(0);
        while header_left > 0 {
            let next_bytes = std::cmp::min(header_left, 9);
            let (col_type_int, b) = VarInt::parse(&header[pos.v()..pos.incr(next_bytes)]);
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
