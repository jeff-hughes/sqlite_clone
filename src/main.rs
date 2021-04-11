use std::{
    io::{stdin, stdout, Write},
    process::exit,
};

fn main() {
    loop {
        let mut input_buffer = String::new();
        print_prompt();
        stdin()
            .read_line(&mut input_buffer)
            .expect("Error reading input");
        if input_buffer.trim() == ".exit" {
            exit(0);
        } else {
            println!("Unrecognized command {}.", input_buffer.trim());
        }
    }
}

fn print_prompt() {
    print!("db > ");
    stdout().flush().unwrap();
}
