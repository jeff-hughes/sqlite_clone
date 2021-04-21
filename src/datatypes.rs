use eyre::Result;
use std::cmp::Ordering;
use std::convert::TryInto;

#[derive(Debug, Clone, Copy, Eq, PartialEq, Ord, PartialOrd)]
pub struct VarInt(pub i64);

impl VarInt {
    pub fn new(value: i64) -> Self {
        return Self(value);
    }

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

    pub fn get_int_val(&self) -> Option<i64> {
        return match self {
            Self::Int8(v) => Some(*v as i64),
            Self::Int16(v) => Some(*v as i64),
            Self::Int24(v) => Some(*v as i64),
            Self::Int32(v) => Some(*v as i64),
            Self::Int48(v) => Some(*v as i64),
            Self::Int64(v) => Some(*v as i64),
            Self::Integer0 => Some(0),
            Self::Integer1 => Some(1),
            _ => None,
        };
    }
}

impl PartialEq for Value {
    fn eq(&self, other: &Self) -> bool {
        return match self {
            Value::Null => match other {
                Value::Null => true,
                _ => false,
            },
            Value::Int8(s) => match other {
                Value::Int8(o) => *s == *o,
                Value::Int16(o) => *s as i16 == *o,
                Value::Int24(o) => *s as i32 == *o,
                Value::Int32(o) => *s as i32 == *o,
                Value::Int48(o) => *s as i64 == *o,
                Value::Int64(o) => *s as i64 == *o,
                Value::Float(o) => *s as f64 == *o,
                Value::Integer0 => *s == 0,
                Value::Integer1 => *s == 1,
                _ => false,
            },
            Value::Int16(s) => match other {
                Value::Int8(o) => *s == *o as i16,
                Value::Int16(o) => *s == *o,
                Value::Int24(o) => *s as i32 == *o,
                Value::Int32(o) => *s as i32 == *o,
                Value::Int48(o) => *s as i64 == *o,
                Value::Int64(o) => *s as i64 == *o,
                Value::Float(o) => *s as f64 == *o,
                Value::Integer0 => *s == 0,
                Value::Integer1 => *s == 1,
                _ => false,
            },
            Value::Int24(s) => match other {
                Value::Int8(o) => *s == *o as i32,
                Value::Int16(o) => *s == *o as i32,
                Value::Int24(o) => *s == *o,
                Value::Int32(o) => *s == *o,
                Value::Int48(o) => *s as i64 == *o,
                Value::Int64(o) => *s as i64 == *o,
                Value::Float(o) => *s as f64 == *o,
                Value::Integer0 => *s == 0,
                Value::Integer1 => *s == 1,
                _ => false,
            },
            Value::Int32(s) => match other {
                Value::Int8(o) => *s == *o as i32,
                Value::Int16(o) => *s == *o as i32,
                Value::Int24(o) => *s == *o,
                Value::Int32(o) => *s == *o,
                Value::Int48(o) => *s as i64 == *o,
                Value::Int64(o) => *s as i64 == *o,
                Value::Float(o) => *s as f64 == *o,
                Value::Integer0 => *s == 0,
                Value::Integer1 => *s == 1,
                _ => false,
            },
            Value::Int48(s) => match other {
                Value::Int8(o) => *s == *o as i64,
                Value::Int16(o) => *s == *o as i64,
                Value::Int24(o) => *s == *o as i64,
                Value::Int32(o) => *s == *o as i64,
                Value::Int48(o) => *s == *o,
                Value::Int64(o) => *s == *o,
                Value::Float(o) => *s as f64 == *o,
                Value::Integer0 => *s == 0,
                Value::Integer1 => *s == 1,
                _ => false,
            },
            Value::Int64(s) => match other {
                Value::Int8(o) => *s == *o as i64,
                Value::Int16(o) => *s == *o as i64,
                Value::Int24(o) => *s == *o as i64,
                Value::Int32(o) => *s == *o as i64,
                Value::Int48(o) => *s == *o,
                Value::Int64(o) => *s == *o,
                Value::Float(o) => *s as f64 == *o,
                Value::Integer0 => *s == 0,
                Value::Integer1 => *s == 1,
                _ => false,
            },
            Value::Float(s) => match other {
                Value::Int8(o) => *s == *o as f64,
                Value::Int16(o) => *s == *o as f64,
                Value::Int24(o) => *s == *o as f64,
                Value::Int32(o) => *s == *o as f64,
                Value::Int48(o) => *s == *o as f64,
                Value::Int64(o) => *s == *o as f64,
                Value::Float(o) => *s == *o,
                Value::Integer0 => *s == 0.0,
                Value::Integer1 => *s == 1.0,
                _ => false,
            },
            Value::Integer0 => match other {
                Value::Int8(o) => *o == 0,
                Value::Int16(o) => *o == 0,
                Value::Int24(o) => *o == 0,
                Value::Int32(o) => *o == 0,
                Value::Int48(o) => *o == 0,
                Value::Int64(o) => *o == 0,
                Value::Float(o) => *o == 0.0,
                Value::Integer0 => true,
                Value::Integer1 => false,
                _ => false,
            },
            Value::Integer1 => match other {
                Value::Int8(o) => *o == 1,
                Value::Int16(o) => *o == 1,
                Value::Int24(o) => *o == 1,
                Value::Int32(o) => *o == 1,
                Value::Int48(o) => *o == 1,
                Value::Int64(o) => *o == 1,
                Value::Float(o) => *o == 1.0,
                Value::Integer0 => false,
                Value::Integer1 => true,
                _ => false,
            },
            Value::Internal(s) => match other {
                Value::Internal(o) => *s == *o,
                _ => false,
            },
            Value::Blob(s) => match other {
                Value::Blob(o) => *s == *o,
                _ => false,
            },
            Value::String(s) => match other {
                Value::String(o) => *s == *o,
                _ => false,
            },
        };
    }
}

impl PartialOrd for Value {
    /// 1. NULL values (serial type 0) sort first.
    /// 2. Numeric values (serial types 1 through 9) sort after NULLs
    ///    and in numeric order.
    /// 3. Text values (odd serial types 13 and larger) sort after
    ///    numeric values in the order determined by the columns
    ///    collating function.
    /// 4. BLOB values (even serial types 12 and larger) sort last and
    ///    in the order determined by memcmp().
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        return match self {
            Value::Null => match other {
                Value::Null => Some(Ordering::Equal),
                Value::Internal(_) => None,
                _ => Some(Ordering::Less),
            },
            Value::Int8(s) => match other {
                Value::Null => Some(Ordering::Greater),
                Value::Int8(o) => s.partial_cmp(o),
                Value::Int16(o) => (*s as i16).partial_cmp(o),
                Value::Int24(o) => (*s as i32).partial_cmp(o),
                Value::Int32(o) => (*s as i32).partial_cmp(o),
                Value::Int48(o) => (*s as i64).partial_cmp(o),
                Value::Int64(o) => (*s as i64).partial_cmp(o),
                Value::Float(o) => (*s as f64).partial_cmp(o),
                Value::Integer0 => s.partial_cmp(&0),
                Value::Integer1 => s.partial_cmp(&1),
                Value::Internal(_) => None,
                _ => Some(Ordering::Less),
            },
            Value::Int16(s) => match other {
                Value::Null => Some(Ordering::Greater),
                Value::Int8(o) => s.partial_cmp(&(*o as i16)),
                Value::Int16(o) => s.partial_cmp(o),
                Value::Int24(o) => (*s as i32).partial_cmp(o),
                Value::Int32(o) => (*s as i32).partial_cmp(o),
                Value::Int48(o) => (*s as i64).partial_cmp(o),
                Value::Int64(o) => (*s as i64).partial_cmp(o),
                Value::Float(o) => (*s as f64).partial_cmp(o),
                Value::Integer0 => s.partial_cmp(&0),
                Value::Integer1 => s.partial_cmp(&1),
                Value::Internal(_) => None,
                _ => Some(Ordering::Less),
            },
            Value::Int24(s) => match other {
                Value::Null => Some(Ordering::Greater),
                Value::Int8(o) => s.partial_cmp(&(*o as i32)),
                Value::Int16(o) => s.partial_cmp(&(*o as i32)),
                Value::Int24(o) => s.partial_cmp(o),
                Value::Int32(o) => s.partial_cmp(o),
                Value::Int48(o) => (*s as i64).partial_cmp(o),
                Value::Int64(o) => (*s as i64).partial_cmp(o),
                Value::Float(o) => (*s as f64).partial_cmp(o),
                Value::Integer0 => s.partial_cmp(&0),
                Value::Integer1 => s.partial_cmp(&1),
                Value::Internal(_) => None,
                _ => Some(Ordering::Less),
            },
            Value::Int32(s) => match other {
                Value::Null => Some(Ordering::Greater),
                Value::Int8(o) => s.partial_cmp(&(*o as i32)),
                Value::Int16(o) => s.partial_cmp(&(*o as i32)),
                Value::Int24(o) => s.partial_cmp(o),
                Value::Int32(o) => s.partial_cmp(o),
                Value::Int48(o) => (*s as i64).partial_cmp(o),
                Value::Int64(o) => (*s as i64).partial_cmp(o),
                Value::Float(o) => (*s as f64).partial_cmp(o),
                Value::Integer0 => s.partial_cmp(&0),
                Value::Integer1 => s.partial_cmp(&1),
                Value::Internal(_) => None,
                _ => Some(Ordering::Less),
            },
            Value::Int48(s) => match other {
                Value::Null => Some(Ordering::Greater),
                Value::Int8(o) => s.partial_cmp(&(*o as i64)),
                Value::Int16(o) => s.partial_cmp(&(*o as i64)),
                Value::Int24(o) => s.partial_cmp(&(*o as i64)),
                Value::Int32(o) => s.partial_cmp(&(*o as i64)),
                Value::Int48(o) => s.partial_cmp(o),
                Value::Int64(o) => s.partial_cmp(o),
                Value::Float(o) => (*s as f64).partial_cmp(o),
                Value::Integer0 => s.partial_cmp(&0),
                Value::Integer1 => s.partial_cmp(&1),
                Value::Internal(_) => None,
                _ => Some(Ordering::Less),
            },
            Value::Int64(s) => match other {
                Value::Null => Some(Ordering::Greater),
                Value::Int8(o) => s.partial_cmp(&(*o as i64)),
                Value::Int16(o) => s.partial_cmp(&(*o as i64)),
                Value::Int24(o) => s.partial_cmp(&(*o as i64)),
                Value::Int32(o) => s.partial_cmp(&(*o as i64)),
                Value::Int48(o) => s.partial_cmp(o),
                Value::Int64(o) => s.partial_cmp(o),
                Value::Float(o) => (*s as f64).partial_cmp(o),
                Value::Integer0 => s.partial_cmp(&0),
                Value::Integer1 => s.partial_cmp(&1),
                Value::Internal(_) => None,
                _ => Some(Ordering::Less),
            },
            Value::Float(s) => match other {
                Value::Null => Some(Ordering::Greater),
                Value::Int8(o) => s.partial_cmp(&(*o as f64)),
                Value::Int16(o) => s.partial_cmp(&(*o as f64)),
                Value::Int24(o) => s.partial_cmp(&(*o as f64)),
                Value::Int32(o) => s.partial_cmp(&(*o as f64)),
                Value::Int48(o) => s.partial_cmp(&(*o as f64)),
                Value::Int64(o) => s.partial_cmp(&(*o as f64)),
                Value::Float(o) => (*s as f64).partial_cmp(&(*o as f64)),
                Value::Integer0 => s.partial_cmp(&0.0),
                Value::Integer1 => s.partial_cmp(&1.0),
                Value::Internal(_) => None,
                _ => Some(Ordering::Less),
            },
            Value::Integer0 => match other {
                Value::Null => Some(Ordering::Greater),
                Value::Int8(o) => 0.partial_cmp(o),
                Value::Int16(o) => 0.partial_cmp(o),
                Value::Int24(o) => 0.partial_cmp(o),
                Value::Int32(o) => 0.partial_cmp(o),
                Value::Int48(o) => 0.partial_cmp(o),
                Value::Int64(o) => 0.partial_cmp(o),
                Value::Float(o) => 0.0.partial_cmp(o),
                Value::Integer0 => Some(Ordering::Equal),
                Value::Integer1 => Some(Ordering::Less),
                Value::Internal(_) => None,
                _ => Some(Ordering::Less),
            },
            Value::Integer1 => match other {
                Value::Null => Some(Ordering::Greater),
                Value::Int8(o) => 1.partial_cmp(o),
                Value::Int16(o) => 1.partial_cmp(o),
                Value::Int24(o) => 1.partial_cmp(o),
                Value::Int32(o) => 1.partial_cmp(o),
                Value::Int48(o) => 1.partial_cmp(o),
                Value::Int64(o) => 1.partial_cmp(o),
                Value::Float(o) => 1.0.partial_cmp(o),
                Value::Integer0 => Some(Ordering::Greater),
                Value::Integer1 => Some(Ordering::Equal),
                Value::Internal(_) => None,
                _ => Some(Ordering::Less),
            },
            Value::String(s) => match other {
                Value::String(o) => s.partial_cmp(o),
                Value::Blob(_) => Some(Ordering::Less),
                Value::Internal(_) => None,
                _ => Some(Ordering::Greater),
            },
            Value::Blob(s) => match other {
                Value::Blob(o) => s.partial_cmp(o),
                Value::Internal(_) => None,
                _ => Some(Ordering::Greater),
            },
            Value::Internal(_) => None,
        };
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn varint_1byte() {
        // only first byte is important -- high order bit not set
        let bytes = vec![0x01, 0x25, 0x37, 0xf2, 0xaa, 0x51, 0x99, 0xe3, 0x1b];
        let varint = VarInt::parse(&bytes);
        assert_eq!(varint.0 .0, 1);
        assert_eq!(varint.1, 1);
    }

    #[test]
    fn varint_2bytes() {
        // only first two bytes are important
        let bytes = vec![0x81, 0x25, 0x37, 0xf2, 0xaa, 0x51, 0x99, 0xe3, 0x1b];
        let varint = VarInt::parse(&bytes);
        assert_eq!(varint.0 .0, 0x80 + 0x25);
        assert_eq!(varint.1, 2);
    }

    #[test]
    fn varint_3bytes() {
        // only first three bytes are important
        let bytes = vec![0x81, 0xa5, 0x37, 0xf2, 0xaa, 0x51, 0x99, 0xe3, 0x1b];
        let varint = VarInt::parse(&bytes);
        assert_eq!(varint.0 .0, 0x4000 + 0x1280 + 0x37);
        assert_eq!(varint.1, 3);
    }

    #[test]
    fn varint_4bytes() {
        // only first four bytes are important
        let bytes = vec![0x81, 0xa5, 0x97, 0x62, 0xaa, 0x51, 0x99, 0xe3, 0x1b];
        let varint = VarInt::parse(&bytes);
        assert_eq!(varint.0 .0, 0x200000 + 0x94000 + 0xb80 + 0x62);
        assert_eq!(varint.1, 4);
    }

    #[test]
    fn varint_5bytes() {
        // only first five bytes are important
        let bytes = vec![0x81, 0xa5, 0x97, 0xf2, 0x3a, 0x51, 0x99, 0xe3, 0x1b];
        let varint = VarInt::parse(&bytes);
        assert_eq!(
            varint.0 .0,
            0x10000000 + 0x4a00000 + 0x5c000 + 0x3900 + 0x3a
        );
        assert_eq!(varint.1, 5);
    }

    #[test]
    fn varint_6bytes() {
        // only first six bytes are important
        let bytes = vec![0x81, 0xa5, 0x97, 0xf2, 0xaa, 0x51, 0x99, 0xe3, 0x1b];
        let varint = VarInt::parse(&bytes);
        assert_eq!(
            varint.0 .0,
            0x800000000 + 0x250000000 + 0x2e00000 + 0x1c8000 + 0x1500 + 0x51
        );
        assert_eq!(varint.1, 6);
    }

    #[test]
    fn varint_7bytes() {
        // only first seven bytes are important
        let bytes = vec![0x81, 0xa5, 0x97, 0xf2, 0xaa, 0x81, 0x69, 0xe3, 0x1b];
        let varint = VarInt::parse(&bytes);
        assert_eq!(
            varint.0 .0,
            0x40000000000 + 0x12800000000 + 0x170000000 + 0xe400000 + 0xa8000 + 0x80 + 0x69
        );
        assert_eq!(varint.1, 7);
    }

    #[test]
    fn varint_8bytes() {
        // only first eight bytes are important
        let bytes = vec![0x81, 0xa5, 0x97, 0xf2, 0xaa, 0x81, 0x99, 0x23, 0x1b];
        let varint = VarInt::parse(&bytes);
        assert_eq!(
            varint.0 .0,
            0x2000000000000
                + 0x940000000000
                + 0xb800000000
                + 0x720000000
                + 0x5400000
                + 0x4000
                + 0xc80
                + 0x23
        );
        assert_eq!(varint.1, 8);
    }

    #[test]
    fn varint_9bytes() {
        let bytes = vec![0x81, 0xa5, 0x97, 0xf2, 0xaa, 0x81, 0x99, 0x83, 0x1b];
        let varint = VarInt::parse(&bytes);
        assert_eq!(
            varint.0 .0,
            0x200000000000000
                + 0x94000000000000
                + 0xb80000000000
                + 0x72000000000
                + 0x540000000
                + 0x400000
                + 0xc8000
                + 0x300
                + 0x1b
        );
        assert_eq!(varint.1, 9);
    }

    #[test]
    fn value_order() {
        let val_null = Value::Null;
        let val_int8_1 = Value::Int8(1);
        let val_int8_2 = Value::Int8(2);
        let val_int16_1 = Value::Int16(1);
        let val_int16_2 = Value::Int16(2);
        let val_int24_1 = Value::Int24(1);
        let val_int24_2 = Value::Int24(2);
        let val_int32_1 = Value::Int32(1);
        let val_int32_2 = Value::Int32(2);
        let val_int48_1 = Value::Int48(1);
        let val_int48_2 = Value::Int48(2);
        let val_int64_1 = Value::Int64(1);
        let val_int64_2 = Value::Int64(2);
        let val_float_1 = Value::Float(1.0);
        let val_float_2 = Value::Float(2.0);
        let val_float_nan = Value::Float(f64::NAN);
        let val_int0 = Value::Integer0;
        let val_int1 = Value::Integer1;
        let val_string_a = Value::String("a".to_string());
        let val_string_b = Value::String("b".to_string());
        let val_blob_1 = Value::Blob(vec![0x01]);
        let val_blob_2 = Value::Blob(vec![0x02]);

        // NULLs always sorted first
        assert!(val_null < val_int8_1);
        assert!(val_null < val_int16_1);
        assert!(val_null < val_int16_1);
        assert!(val_null < val_int24_1);
        assert!(val_null < val_int32_1);
        assert!(val_null < val_int48_1);
        assert!(val_null < val_int64_1);
        assert!(val_null < val_float_1);
        assert!(val_null < val_float_nan);
        assert!(val_null < val_int0);
        assert!(val_null < val_int1);
        assert!(val_null < val_string_a);
        assert!(val_null < val_blob_1);

        assert!(val_int8_1 < val_int8_2);
        assert!(val_int8_1 == val_int16_1);
        assert!(val_int8_1 < val_int16_2);
        assert!(val_int8_1 == val_int24_1);
        assert!(val_int8_1 < val_int24_2);
        assert!(val_int8_1 == val_int32_1);
        assert!(val_int8_1 < val_int32_2);
        assert!(val_int8_1 == val_int48_1);
        assert!(val_int8_1 < val_int48_2);
        assert!(val_int8_1 == val_int64_1);
        assert!(val_int8_1 < val_int64_2);
        assert!(val_int8_1 == val_float_1);
        assert!(val_int8_1 < val_float_2);
        assert!(val_int8_1 != val_float_nan);
        assert!(val_int8_1 > val_int0);
        assert!(val_int8_1 == val_int1);
        assert!(val_int8_1 < val_string_a);
        assert!(val_int8_1 < val_blob_1);

        assert!(val_int16_1 < val_int16_2);
        assert!(val_int24_1 < val_int24_2);
        assert!(val_int32_1 < val_int32_2);
        assert!(val_int48_1 < val_int48_2);
        assert!(val_int64_1 < val_int64_2);

        assert!(val_float_1 < val_float_2);
        assert!(val_float_1 != val_float_nan);
        assert!(val_float_2 != val_float_nan);

        assert!(val_int0 < val_int1);

        assert!(val_string_a < val_string_b);
        assert!(val_string_a < val_blob_1);
        assert!(val_string_a < val_blob_2);

        assert!(val_blob_1 < val_blob_2);
    }
}
