use eyre::Result;
use std::convert::TryInto;

#[derive(Debug, Clone, Copy)]
pub struct VarInt(pub i64);

impl VarInt {
    // based off: https://docs.rs/sqlite_varint/0.1.2/src/sqlite_varint/lib.rs.html
    pub fn parse(bytes: &[u8]) -> (Self, usize) {
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
        return (Self(varint), bytes_read);
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
    Integer0(usize),
    Integer1(usize),
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
            8 => Self::Integer0(0),
            9 => Self::Integer1(0),
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
            | Self::Integer0(s)
            | Self::Integer1(s)
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
    Integer0,
    Integer1,
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
            DataType::Integer0(_) => Self::Integer0,
            DataType::Integer1(_) => Self::Integer1,
            DataType::Internal => Self::Internal(value.into()),
            DataType::Blob(_) => Self::Blob(value.into()),
            DataType::String(_) => Self::String(String::from_utf8_lossy(value).into()),
        }
    }
}
