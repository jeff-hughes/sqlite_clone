use eyre::{eyre, Result};
use std::io::{stdin, stdout, Write};
use std::process::exit;

mod statement;
use crate::statement::{execute_statement, prepare_statement};

fn main() {
    loop {
        let mut input_buffer = String::new();
        print_prompt();
        stdin()
            .read_line(&mut input_buffer)
            .expect("Error reading input");
        let trimmed_input = input_buffer.trim().to_string();

        // check for "meta-commands", starting with '.'
        if trimmed_input.chars().next() == Some('.') {
            if let Err(err) = do_meta_command(trimmed_input) {
                println!("{}", err);
            }
        } else {
            match prepare_statement(trimmed_input) {
                Err(err) => println!("{}", err),
                Ok(stmt) => {
                    let _ = execute_statement(stmt);
                    println!("Executed.");
                }
            }
        }
    }
}

fn print_prompt() {
    print!("db > ");
    stdout().flush().unwrap();
}

fn do_meta_command(input: String) -> Result<()> {
    if input == ".exit" {
        exit(0);
    } else {
        return Err(eyre!("Unrecognized command {}.", input));
    }
}
