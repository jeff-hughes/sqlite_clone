use derive_try_from_primitive::TryFromPrimitive;
use eyre::Result;
use nom::{
    bytes::complete::tag,
    combinator::{map, verify},
    error::context,
    number::complete::{be_u16, be_u8},
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
}

impl FileHeader {
    const MAGIC: &'static [u8] = "SQLite format 3\0".as_bytes();

    pub fn parse(i: Input) -> NomResult<Self> {
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

        Ok((
            i,
            Self {
                page_size: page_size,
                file_write_version: file_write,
                file_read_version: file_read,
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
