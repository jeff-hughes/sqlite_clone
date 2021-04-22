use derive_try_from_primitive::TryFromPrimitive;
use eyre::{eyre, Result};
use positioned_io::ReadAt;
use std::convert::{TryFrom, TryInto};
use std::fs::File;

pub mod btree;
pub mod datatypes;
pub mod pager;
pub mod parsing;

const SQLITE_MAJOR_VERSION: u16 = 3;
const SQLITE_MINOR_VERSION: u16 = 35;
const SQLITE_PATCH_VERSION: u16 = 4;

#[derive(Debug, Copy, Clone)]
pub struct DbOptions {
    pub page_size: usize,
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
    pub cache_size: i32,
    pub largest_root_page: u32,
    pub encoding: TextEncoding,
    pub user_version: u32,
    pub incremental_vacuum: bool,
    pub app_id: u32,
    pub version_valid_for: u32,
    pub sqlite_version: u32,
}

impl DbOptions {
    const MAGIC: &'static [u8] = "SQLite format 3\0".as_bytes();

    pub fn init(filename: &str) -> Result<Self> {
        let file = File::open(filename)?;
        let file_length = file.metadata()?.len() as usize;

        if file_length > 0 {
            // file header is 100 bytes long
            let mut buf = vec![0; 100];
            let _ = file.read_at(0, &mut buf)?;
            return Self::deserialize(&buf);
        } else {
            // set defaults
            let sqlite_version = SQLITE_MAJOR_VERSION as u32 * 1_000_000
                + SQLITE_MINOR_VERSION as u32 * 1000
                + SQLITE_PATCH_VERSION as u32;
            return Ok(Self {
                page_size: 4096,
                file_write_version: FileVersion::Legacy,
                file_read_version: FileVersion::Legacy,
                reserved_space: 0,
                max_payload: 64,
                min_payload: 32,
                leaf_payload: 32,
                change_counter: 0,
                num_pages: 0,
                first_freelist: 0,
                num_freelist: 0,
                schema_cookie: 0,
                schema_format: 4,
                cache_size: 0,
                largest_root_page: 0,
                encoding: TextEncoding::Utf8,
                user_version: 0,
                incremental_vacuum: false,
                app_id: 0,
                version_valid_for: 0,
                sqlite_version: sqlite_version,
            });
        }
    }

    pub fn deserialize(i: &[u8]) -> Result<Self> {
        let total_size = i.len();
        let mut pos = parsing::Position::new();

        if &i[pos.v()..pos.incr(Self::MAGIC.len())] != Self::MAGIC {
            return Err(eyre!("Not a valid sqlite file -- no magic number!"));
        }

        // page size must be a power of two between 512 and 32768
        // inclusive, or the value 1 representing a page size of 65536
        let mut page_size = parsing::be_u16(&i[pos.v()..pos.incr(2)])? as usize;
        if page_size != 1 && (page_size <= 512 || page_size >= 32768 || page_size % 2 != 0) {
            return Err(eyre!("Page size is invalid."));
        } else if page_size == 1 {
            page_size = 65536; // this value does not fit into a u16 and
                               // is thus represented by 0x00 0x01
        }

        let file_write = FileVersion::try_from(parsing::be_u8(&i[pos.v()..pos.incr(1)])?).unwrap();
        let file_read = FileVersion::try_from(parsing::be_u8(&i[pos.v()..pos.incr(1)])?).unwrap();

        let reserved_space = parsing::be_u8(&i[pos.v()..pos.incr(1)])?;
        let max_payload = parsing::be_u8(&i[pos.v()..pos.incr(1)])?;
        let min_payload = parsing::be_u8(&i[pos.v()..pos.incr(1)])?;
        let leaf_payload = parsing::be_u8(&i[pos.v()..pos.incr(1)])?;
        if max_payload != 64 || min_payload != 32 || leaf_payload != 32 {
            return Err(eyre!("Invalid payload fraction sizes"));
        }

        let change_counter = parsing::be_u32(&i[pos.v()..pos.incr(4)])?;
        let mut num_pages = parsing::be_u32(&i[pos.v()..pos.incr(4)])?;
        let first_freelist = parsing::be_u32(&i[pos.v()..pos.incr(4)])?;
        let num_freelist = parsing::be_u32(&i[pos.v()..pos.incr(4)])?;
        let schema_cookie = parsing::be_u32(&i[pos.v()..pos.incr(4)])?;
        let schema_format = parsing::be_u32(&i[pos.v()..pos.incr(4)])?;
        let cache_size = parsing::be_i32(&i[pos.v()..pos.incr(4)])?;
        let largest_root_page = parsing::be_u32(&i[pos.v()..pos.incr(4)])?;
        let encoding = TextEncoding::try_from(parsing::be_u32(&i[pos.v()..pos.incr(4)])?).unwrap();
        let user_version = parsing::be_u32(&i[pos.v()..pos.incr(4)])?;
        let incremental_vacuum = parsing::be_u32(&i[pos.v()..pos.incr(4)])? != 0;
        let app_id = parsing::be_u32(&i[pos.v()..pos.incr(4)])?;

        pos.incr(20); // unused space

        let version_valid_for = parsing::be_u32(&i[pos.v()..pos.incr(4)])?;
        let sqlite_version = parsing::be_u32(&i[pos.v()..pos.incr(4)])?;

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

        Ok(Self {
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
        })
    }

    pub fn serialize(&self) -> Vec<u8> {
        let mut output = Vec::with_capacity(100);
        output.extend_from_slice(Self::MAGIC);
        let page_size = if self.page_size == 65536 {
            1
        } else {
            self.page_size as u16
        };
        output.extend(page_size.to_be_bytes().iter());
        output.push(self.file_write_version as u8);
        output.push(self.file_read_version as u8);
        output.push(self.reserved_space);

        output.push(self.max_payload);
        output.push(self.min_payload);
        output.push(self.leaf_payload);

        output.extend(self.change_counter.to_be_bytes().iter());
        output.extend(self.num_pages.to_be_bytes().iter());
        output.extend(self.first_freelist.to_be_bytes().iter());
        output.extend(self.num_freelist.to_be_bytes().iter());

        output.extend(self.schema_cookie.to_be_bytes().iter());
        output.extend(self.schema_format.to_be_bytes().iter());

        output.extend(self.cache_size.to_be_bytes().iter());
        output.extend(self.largest_root_page.to_be_bytes().iter());

        let encoding: u32 = (self.encoding as u32).try_into().unwrap();
        output.extend(encoding.to_be_bytes().iter());
        output.extend(self.user_version.to_be_bytes().iter());

        let incr_vacuum = self.incremental_vacuum as u32;
        output.extend(incr_vacuum.to_be_bytes().iter());
        output.extend(self.app_id.to_be_bytes().iter());
        output.extend(&[0u8; 20]); // unused space

        output.extend(self.version_valid_for.to_be_bytes().iter());
        output.extend(self.sqlite_version.to_be_bytes().iter());
        return output;
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
