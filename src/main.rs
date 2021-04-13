use std::io::{self, BufRead, Write};

use sqlite_clone::do_meta_command;
use sqlite_clone::statement::Statement;
use sqlite_clone::table::Table;

fn main() {
    let mut table = Table::new();
    loop {
        let stdin = io::stdin();
        let stdin_lock = stdin.lock();
        let mut stdout = io::stdout();
        let input = prompt(stdin_lock, &mut stdout, "db > ");
        handle_input(input, &mut table, &mut stdout);
    }
}

fn prompt<R, W>(mut reader: R, mut writer: W, prompt: &str) -> String
where
    R: BufRead,
    W: Write,
{
    write!(&mut writer, "{}", prompt).expect("Unable to write");
    writer.flush().unwrap();
    let mut buffer = String::new();
    reader.read_line(&mut buffer).expect("Error reading input");
    return buffer;
}

fn print_output<W>(writer: &mut W, output: &str)
where
    W: Write,
{
    write!(writer, "{}\n", output).expect("Unable to write");
    writer.flush().unwrap();
}

fn handle_input<W>(input: String, table: &mut Table, writer: &mut W)
where
    W: Write,
{
    let trimmed_input = input.trim().to_string();

    // check for "meta-commands", starting with '.'
    if trimmed_input.chars().next() == Some('.') {
        if let Err(err) = do_meta_command(trimmed_input) {
            print_output(writer, &err.to_string());
        }
    } else {
        match Statement::prepare(table, trimmed_input) {
            Err(err) => print_output(writer, &err.to_string()),
            Ok(mut stmt) => match stmt.execute() {
                Ok(result) => print_output(writer, &result),
                Err(err) => print_output(writer, &err.to_string()),
            },
        }
    }
}

#[test]
fn test_insert_retrieve() {
    let mut table = Table::new();
    let commands = vec!["insert 1 user1 person1@example.com", "select"];
    let expected_outputs = vec!["Executed.\n", "(1, user1, person1@example.com)\n"];

    for (i, command) in commands.iter().enumerate() {
        let mut output = Vec::new();
        let input = prompt(command.as_bytes(), &mut output, "db > ");
        let mut output2 = Vec::new();
        handle_input(input, &mut table, &mut output2);
        let output2 = String::from_utf8(output2).expect("Not UTF-8");
        assert_eq!(expected_outputs[i], output2);
    }
}

#[test]
fn test_too_many_rows() {
    let num_rows = sqlite_clone::table::TABLE_MAX_ROWS + 1;

    let mut table = Table::new();
    for i in 0..num_rows {
        let mut output = Vec::new();
        let command = format!("insert {} user{} person{}@example.com", i, i, i);
        let input = prompt(command.as_bytes(), &mut output, "db > ");
        let mut output2 = Vec::new();
        handle_input(input, &mut table, &mut output2);
        let output2 = String::from_utf8(output2).expect("Not UTF-8");

        if i == num_rows - 1 {
            assert_eq!("Table full.\n", output2);
        }
    }
}

#[test]
fn test_string_too_long() {
    let username = "a".repeat(sqlite_clone::table::USERNAME_SIZE + 1);
    let email = "a".repeat(sqlite_clone::table::EMAIL_SIZE + 1);

    let mut table = Table::new();
    let mut output = Vec::new();
    let command = format!("insert 1 {} {}", username, email);
    let input = prompt(command.as_bytes(), &mut output, "db > ");
    let mut output2 = Vec::new();
    handle_input(input, &mut table, &mut output2);
    let output2 = String::from_utf8(output2).expect("Not UTF-8");

    assert_eq!("String is too long.\n", output2);
}

#[test]
fn test_negative_id() {
    let mut table = Table::new();
    let mut output = Vec::new();
    let command = "insert -1 foo bar".to_string();
    let input = prompt(command.as_bytes(), &mut output, "db > ");
    let mut output2 = Vec::new();
    handle_input(input, &mut table, &mut output2);
    let output2 = String::from_utf8(output2).expect("Not UTF-8");

    assert_eq!("ID must be a positive integer.\n", output2);
}
