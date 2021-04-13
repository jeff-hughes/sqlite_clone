use eyre::{eyre, Result, WrapErr};
use lazy_static::lazy_static;
use regex::Regex;

use crate::table::{self, Row, Table};

lazy_static! {
    static ref RE_INSERT: Regex = Regex::new(r"insert (.+) (.+) (.+)").unwrap();
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
                    let id = caps[1]
                        .parse::<u32>()
                        .wrap_err("ID must be a positive integer.")?;

                    let username = caps[2].to_string();
                    if username.len() > table::USERNAME_SIZE {
                        return Err(eyre!("String is too long."));
                    }

                    let email = caps[3].to_string();
                    if email.len() > table::EMAIL_SIZE {
                        return Err(eyre!("String is too long."));
                    }

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

    pub fn execute(&mut self) -> Result<String> {
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
