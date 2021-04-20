use eyre::Result;
use std::convert::TryInto;

pub fn be_u8(value: &[u8]) -> Result<u8> {
    return Ok(u8::from_be_bytes(value.try_into()?));
}

pub fn be_u16(value: &[u8]) -> Result<u16> {
    return Ok(u16::from_be_bytes(value.try_into()?));
}

pub fn be_u32(value: &[u8]) -> Result<u32> {
    return Ok(u32::from_be_bytes(value.try_into()?));
}

pub fn be_i32(value: &[u8]) -> Result<i32> {
    return Ok(i32::from_be_bytes(value.try_into()?));
}

#[derive(Debug)]
pub struct Position {
    pub pos: usize,
}

impl Position {
    pub fn new() -> Self {
        return Self { pos: 0 };
    }

    pub fn v(&self) -> usize {
        return self.pos;
    }

    pub fn incr(&mut self, value: usize) -> usize {
        self.pos += value;
        return self.pos;
    }

    pub fn decr(&mut self, value: usize) -> usize {
        self.pos -= value;
        return self.pos;
    }

    pub fn set(&mut self, value: usize) -> usize {
        self.pos = value;
        return self.pos;
    }
}
