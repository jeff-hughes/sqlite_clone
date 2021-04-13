use eyre::{eyre, Result};
use lazy_static::lazy_static;
use regex::Regex;

use crate::table::{Row, Table};

lazy_static! {
    static ref RE_INSERT: Regex = Regex::new(r"insert ([0-9]+) (.+) (.+)").unwrap();
}

#[derive(Debug, Clone)]
pub enum StatementType {
    INSERT,
    SELECT,
}

#[derive(Debug)]
pub struct Statement<'a> {
    stype: StatementType,
    table: &'a mut Table,
    row_to_insert: Option<Row>,
}

impl<'a> Statement<'a> {
    fn new(stmt_type: StatementType, table: &'a mut Table, row_to_insert: Option<Row>) -> Self {
        return Self {
            stype: stmt_type,
            table: table,
            row_to_insert: row_to_insert,
        };
    }

    pub fn prepare(table: &mut Table, input: String) -> Result<Statement> {
        if input.starts_with("insert") {
            let caps = RE_INSERT.captures(&input);
            match caps {
                Some(caps) => {
                    let id = caps[1].parse::<u32>().expect("ID must be an integer");
                    return Ok(Statement::new(
                        StatementType::INSERT,
                        table,
                        Some(Row::new(id, caps[2].to_string(), caps[3].to_string())),
                    ));
                }
                None => {
                    return Err(eyre!("Syntax error."));
                }
            }
        } else if input.starts_with("select") {
            return Ok(Statement::new(StatementType::SELECT, table, None));
        }
        return Err(eyre!("Unrecognized command {}.", input));
    }

    pub fn execute(&mut self) -> Result<()> {
        let result;
        match self.stype {
            StatementType::INSERT => {
                result = self.table.execute_insert(self.row_to_insert.unwrap());
            }
            StatementType::SELECT => {
                result = self.table.execute_select();
            }
        }
        return result;
    }
}
