use eyre::{eyre, Result};
#[derive(Debug, Clone)]
pub enum StatementType {
    INSERT,
    SELECT,
}

#[derive(Debug, Clone)]
pub struct Statement {
    stype: StatementType,
}

impl Statement {
    fn new(stmt_type: StatementType) -> Self {
        return Self { stype: stmt_type };
    }
}

pub fn prepare_statement(input: String) -> Result<Statement> {
    if input.starts_with("insert") {
        return Ok(Statement::new(StatementType::INSERT));
    } else if input.starts_with("select") {
        return Ok(Statement::new(StatementType::SELECT));
    }
    return Err(eyre!("Unrecognized command {}.", input));
}

pub fn execute_statement(stmt: Statement) -> Result<()> {
    match stmt.stype {
        StatementType::INSERT => println!("This is where we would do an insert."),
        StatementType::SELECT => println!("This is where we would do a select."),
    }
    return Ok(());
}
