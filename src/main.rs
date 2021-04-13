use std::io::{stdin, stdout, Write};

use sqlite_clone::do_meta_command;
use sqlite_clone::statement::Statement;
use sqlite_clone::table::Table;

fn main() {
    let mut table = Table::new();
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
            match Statement::prepare(&mut table, trimmed_input) {
                Err(err) => println!("{}", err),
                Ok(mut stmt) => {
                    let _ = stmt.execute();
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
