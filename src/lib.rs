use derive_try_from_primitive::TryFromPrimitive;
use eyre::Result;
use nom::{
    bytes::complete::{tag, take},
    combinator::{cond, map, verify},
    error::context,
    multi::count,
    number::complete::{be_u16, be_u32, be_u8},
    sequence::tuple,
};
use std::convert::{TryFrom, TryInto};

pub type Input<'a> = &'a [u8];
pub type NomResult<'a, O> = nom::IResult<Input<'a>, O, nom::error::VerboseError<Input<'a>>>;

#[derive(Debug, Copy, Clone)]
pub struct FileHeader {
    pub page_size: u16,
    pub file_write_version: FileVersion,
    pub file_read_version: FileVersion,
    pub reserved_space: u8,
    pub max_payload: u8,
    pub min_payload: u8,
    pub leaf_payload: u8,
    pub change_counter: u32,
    pub num_pages: u32,
    pub first_freelist: u32,
    pub num_freelist: u32,
    pub schema_cookie: u32,
    pub schema_format: u32,
    pub cache_size: u32,
    pub largest_root_page: u32,
    pub encoding: TextEncoding,
    pub user_version: u32,
    pub incremental_vacuum: bool,
    pub app_id: u32,
    pub version_valid_for: u32,
    pub sqlite_version: u32,
}

impl FileHeader {
    const MAGIC: &'static [u8] = "SQLite format 3\0".as_bytes();

    pub fn parse(i: Input) -> NomResult<Self> {
        let total_size = i.len();
        let (i, _) = context("Magic", tag(Self::MAGIC))(i)?;

        // page size must be a power of two between 512 and 32768
        // inclusive, or the value 1 representing a page size of 65536
        let (i, page_size) = verify(be_u16, |&x| {
            x == 1 || (x >= 512 && x <= 32768 && x % 2 == 0)
        })(i)?;
        let (i, (file_write, file_read)) = tuple((
            context(
                "File write version",
                map(be_u8, |x| FileVersion::try_from(x).unwrap()),
            ),
            context(
                "File read version",
                map(be_u8, |x| FileVersion::try_from(x).unwrap()),
            ),
        ))(i)?;
        let (i, reserved_space) = be_u8(i)?;
        let (i, (max_payload, min_payload, leaf_payload)) = tuple((
            context(
                "Maximum embedded payload fraction",
                verify(be_u8, |&x| x == 64),
            ),
            context(
                "Minimum embedded payload fraction",
                verify(be_u8, |&x| x == 32),
            ),
            context("Leaf payload fraction", verify(be_u8, |&x| x == 32)),
        ))(i)?;
        let (i, change_counter) = be_u32(i)?;
        let (i, mut num_pages) = be_u32(i)?;
        let (i, first_freelist) = be_u32(i)?;
        let (i, num_freelist) = be_u32(i)?;
        let (i, schema_cookie) = be_u32(i)?;
        let (i, schema_format) = be_u32(i)?;
        let (i, cache_size) = be_u32(i)?;
        let (i, largest_root_page) = be_u32(i)?;
        let (i, encoding) = map(be_u32, |x| TextEncoding::try_from(x).unwrap())(i)?;
        let (i, user_version) = be_u32(i)?;
        let (i, incremental_vacuum) = map(be_u32, |x| x != 0)(i)?;
        let (i, app_id) = be_u32(i)?;
        let (i, _) = take(20u8)(i)?;
        let (i, version_valid_for) = be_u32(i)?;
        let (i, sqlite_version) = be_u32(i)?;

        // The in-header database size (num_pages) is only considered to
        // be valid if it is non-zero and if the 4-byte change counter
        // at offset 24 exactly matches the 4-byte version-valid-for
        // number at offset 92. The in-header database size is always
        // valid when the database is only modified using recent
        // versions of SQLite, versions 3.7.0 (2010-07-21) and later. If
        // a legacy version of SQLite writes to the database, it will
        // not know to update the in-header database size and so the
        // in-header database size could be incorrect. But legacy
        // versions of SQLite will also leave the version-valid-for
        // number at offset 92 unchanged so it will not match the
        // change-counter. Hence, invalid in-header database sizes can
        // be detected (and ignored) by observing when the
        // change-counter does not match the version-valid-for number.
        if num_pages == 0 || change_counter != version_valid_for {
            num_pages = total_size as u32 / page_size as u32;
        }

        Ok((
            i,
            Self {
                page_size: page_size,
                file_write_version: file_write,
                file_read_version: file_read,
                reserved_space: reserved_space,
                max_payload: max_payload,
                min_payload: min_payload,
                leaf_payload: leaf_payload,
                change_counter: change_counter,
                num_pages: num_pages,
                first_freelist: first_freelist,
                num_freelist: num_freelist,
                schema_cookie: schema_cookie,
                schema_format: schema_format,
                cache_size: cache_size,
                largest_root_page: largest_root_page,
                encoding: encoding,
                user_version: user_version,
                incremental_vacuum: incremental_vacuum,
                app_id: app_id,
                version_valid_for: version_valid_for,
                sqlite_version: sqlite_version,
            },
        ))
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, TryFromPrimitive)]
#[repr(u8)]
pub enum FileVersion {
    Legacy = 0x1,
    WAL = 0x2,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, TryFromPrimitive)]
#[repr(u32)]
pub enum TextEncoding {
    Utf8 = 0x1,
    Utf16le = 0x2,
    Utf16be = 0x3,
}

// TODO: only properly parses table leaf pages so far
#[derive(Debug)]
pub struct BtreePage {
    pub page_type: PageType,
    pub first_freeblock: u16,
    pub num_cells: u16,
    pub cell_start: u16,
    pub fragmented_bytes: u8,
    pub right_pointer: Option<u32>,
    pub cell_pointers: Vec<u16>,
    pub records: Vec<(VarInt, Record)>,
}

impl BtreePage {
    pub fn parse(i: Input, offset: usize, file_header: FileHeader) -> NomResult<Self> {
        let full_input = i.clone();
        let (i, page_type) = map(be_u8, |x| PageType::try_from(x).unwrap())(i)?;
        let (i, first_freeblock) = be_u16(i)?;
        let (i, num_cells) = be_u16(i)?;
        let (i, cell_start) = be_u16(i)?;
        let (i, fragmented_bytes) = be_u8(i)?;
        let (i, right_pointer) = cond(page_type.is_interior(), be_u32)(i)?;
        //println!("{:?} {:?}", page_type, num_cells);

        let (i, cell_pointers) = count(be_u16, num_cells as usize)(i)?;
        let mut records = Vec::new();
        for ptr in &cell_pointers {
            let (fi, payload_size) = VarInt::parse(&full_input[((*ptr as usize) - offset)..]);
            let (fi, row_id) = VarInt::parse(&fi[..]);

            let payload_on_page = Self::calc_payload_on_page(
                file_header.page_size as usize,
                file_header.reserved_space as usize,
                payload_size.0 as usize,
            );
            let (_, rec) = Record::parse(&fi[..payload_on_page])?;
            records.push((row_id, rec));
        }

        Ok((
            i,
            Self {
                page_type: page_type,
                first_freeblock: first_freeblock,
                num_cells: num_cells,
                cell_start: cell_start,
                fragmented_bytes: fragmented_bytes,
                right_pointer: right_pointer,
                cell_pointers: cell_pointers,
                records: records,
            },
        ))
    }

    fn calc_payload_on_page(page_size: usize, reserved_space: usize, payload_size: usize) -> usize {
        // the logic for these calculations is documented here, near the
        // bottom of the section:
        // https://sqlite.org/fileformat2.html#b_tree_pages
        // usable_space = U
        // max_payload = X
        // min_payload = M
        // k = K...because I honestly don't understand what this one means
        let usable_space = page_size - reserved_space;
        let max_payload = usable_space - 35;
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
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, TryFromPrimitive)]
#[repr(u8)]
pub enum PageType {
    InteriorIndex = 0x02,
    InteriorTable = 0x05,
    LeafIndex = 0x0a,
    LeafTable = 0x0d,
}

impl PageType {
    pub fn is_interior(&self) -> bool {
        match self {
            PageType::InteriorIndex => true,
            PageType::InteriorTable => true,
            _ => false,
        }
    }

    pub fn is_leaf(&self) -> bool {
        match self {
            PageType::LeafIndex => true,
            PageType::LeafTable => true,
            _ => false,
        }
    }
}

#[derive(Debug)]
pub struct Record {
    pub col_types: Vec<DataType>,
    pub values: Vec<Value>,
}

impl Record {
    pub fn parse(i: Input) -> NomResult<Self> {
        let full_input = i.clone();
        let (i, header_size) = VarInt::parse(i);
        let header_size_size = full_input.len() - i.len();

        // get the rest of the header
        let mut header = &i[..header_size.0 as usize - header_size_size];
        let mut col_types = Vec::new();
        while header.len() > 0 {
            let (hd, col_type_int) = VarInt::parse(header);
            let col_type = DataType::from_varint(col_type_int).expect("Not a valid data type.");
            col_types.push(col_type);
            header = hd;
        }

        let values_input = &full_input[header_size.0 as usize..];
        let mut offset = 0;
        let mut values = Vec::new();
        for col in &col_types {
            if let Some(size) = col.get_size() {
                values.push(Value::new(col, &values_input[offset..(offset + size)]));
                offset += size;
            }
        }

        Ok((
            i,
            Self {
                col_types: col_types,
                values: values,
            },
        ))
    }
}

#[derive(Debug, Clone, Copy)]
pub struct VarInt(i64);

impl VarInt {
    // based off: https://docs.rs/sqlite_varint/0.1.2/src/sqlite_varint/lib.rs.html
    pub fn parse(bytes: &[u8]) -> (&[u8], Self) {
        let mut varint: i64 = 0;
        let mut bytes_read: usize = 0;
        for (i, byte) in bytes.iter().enumerate().take(9) {
            bytes_read += 1;
            if i == 8 {
                varint = (varint << 8) | *byte as i64;
                break;
            } else {
                varint = (varint << 7) | (*byte & 0b0111_1111) as i64;
                if *byte < 0b1000_0000 {
                    break;
                }
            }
        }
        return (&bytes[bytes_read..], Self(varint));
    }
}

#[derive(Debug, Clone, Copy)]
pub enum DataType {
    Null(usize),
    Int8(usize),
    Int16(usize),
    Int24(usize),
    Int32(usize),
    Int48(usize),
    Int64(usize),
    Float(usize),
    Bool0(usize),
    Bool1(usize),
    Internal,
    Blob(usize),
    String(usize),
}

impl DataType {
    pub fn from_varint(value: VarInt) -> Result<Self> {
        let non_neg = value.0.try_into()?;
        Ok(match non_neg {
            0 => Self::Null(0),
            1 => Self::Int8(1),
            2 => Self::Int16(2),
            3 => Self::Int24(3),
            4 => Self::Int32(4),
            5 => Self::Int48(6),
            6 => Self::Int64(8),
            7 => Self::Float(8),
            8 => Self::Bool0(0),
            9 => Self::Bool1(0),
            10 | 11 => Self::Internal,
            x if x % 2 == 0 => Self::Blob((x - 12) / 2),
            x => Self::String((x - 13) / 2),
        })
    }

    pub fn get_size(&self) -> Option<usize> {
        match self {
            Self::Internal => None,
            Self::Null(s)
            | Self::Int8(s)
            | Self::Int16(s)
            | Self::Int24(s)
            | Self::Int32(s)
            | Self::Int48(s)
            | Self::Int64(s)
            | Self::Float(s)
            | Self::Bool0(s)
            | Self::Bool1(s)
            | Self::Blob(s)
            | Self::String(s) => Some(*s),
        }
    }
}

#[derive(Debug, Clone)]
pub enum Value {
    Null,
    Int8(i8),
    Int16(i16),
    Int24(i32),
    Int32(i32),
    Int48(i64),
    Int64(i64),
    Float(f64),
    Bool(bool),
    Internal(Vec<u8>),
    Blob(Vec<u8>),
    String(String),
}

impl Value {
    pub fn new(data_type: &DataType, value: &[u8]) -> Self {
        match data_type {
            DataType::Null(_) => Self::Null,
            DataType::Int8(_) => Self::Int8(i8::from_be_bytes(
                value.try_into().expect("Slice with incorrect length"),
            )),
            DataType::Int16(_) => Self::Int16(i16::from_be_bytes(
                value.try_into().expect("Slice with incorrect length"),
            )),
            DataType::Int24(_) => Self::Int24(i32::from_be_bytes(
                value.try_into().expect("Slice with incorrect length"),
            )),
            DataType::Int32(_) => Self::Int32(i32::from_be_bytes(
                value.try_into().expect("Slice with incorrect length"),
            )),
            DataType::Int48(_) => Self::Int48(i64::from_be_bytes(
                value.try_into().expect("Slice with incorrect length"),
            )),
            DataType::Int64(_) => Self::Int64(i64::from_be_bytes(
                value.try_into().expect("Slice with incorrect length"),
            )),
            DataType::Float(_) => Self::Float(f64::from_be_bytes(
                value.try_into().expect("Slice with incorrect length"),
            )),
            DataType::Bool0(_) => Self::Bool(false),
            DataType::Bool1(_) => Self::Bool(true),
            DataType::Internal => Self::Internal(value.into()),
            DataType::Blob(_) => Self::Blob(value.into()),
            DataType::String(_) => Self::String(String::from_utf8_lossy(value).into()),
        }
    }
}
