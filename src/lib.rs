use derive_try_from_primitive::TryFromPrimitive;
use eyre::Result;
use nom::{
    bytes::complete::{tag, take},
    combinator::{cond, map, verify},
    error::context,
    number::complete::{be_u16, be_u32, be_u8},
    sequence::tuple,
};
use std::convert::TryFrom;

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
}

impl BtreePage {
    pub fn parse(i: Input) -> NomResult<Self> {
        let (i, page_type) = map(be_u8, |x| PageType::try_from(x).unwrap())(i)?;
        let (i, first_freeblock) = be_u16(i)?;
        let (i, num_cells) = be_u16(i)?;
        let (i, cell_start) = be_u16(i)?;
        let (i, fragmented_bytes) = be_u8(i)?;
        let (i, right_pointer) = cond(page_type.is_interior(), be_u32)(i)?;

        Ok((
            i,
            Self {
                page_type: page_type,
                first_freeblock: first_freeblock,
                num_cells: num_cells,
                cell_start: cell_start,
                fragmented_bytes: fragmented_bytes,
                right_pointer: right_pointer,
            },
        ))
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
