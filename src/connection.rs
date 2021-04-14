use crate::table::Table;
use eyre::Result;

pub struct Connection {
    pub table: Table,
}

impl Connection {
    pub fn new(filename: &str) -> Result<Self> {
        return Ok(Self {
            table: Table::new(filename)?,
        });
    }
}
