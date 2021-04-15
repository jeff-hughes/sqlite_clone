use eyre::Result;
use std::io::{self, BufRead, Write};
use std::{env, process::exit};

use eyre::Context;
use sqlite_clone::connection::Connection;
use sqlite_clone::do_meta_command;
use sqlite_clone::statement::Statement;

fn main() -> Result<()> {
    let args: Vec<String> = env::args().collect();
    if args.len() < 2 {
        println!("Error: Must supply a database filename.");
        exit(1);
    }
    let filename = &args[1];

    let mut connection = Connection::new(&filename).wrap_err("Failed to open or read file.")?;
    let mut to_continue = true;
    while to_continue {
        let stdin = io::stdin();
        let stdin_lock = stdin.lock();
        let mut stdout = io::stdout();
        let input = prompt(stdin_lock, &mut stdout, "db > ");
        to_continue = handle_input(input, &mut connection, &mut stdout);
    }
    Ok(())
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

fn handle_input<W>(input: String, conn: &mut Connection, writer: &mut W) -> bool
where
    W: Write,
{
    let trimmed_input = input.trim().to_string();

    // check for "meta-commands", starting with '.'
    if trimmed_input.chars().next() == Some('.') {
        // setting a special case here for now, but
        // eventually we might want to handle this
        // differently
        if trimmed_input == ".exit" {
            return false;
        }
        if let Err(err) = do_meta_command(conn, trimmed_input) {
            print_output(writer, &err.to_string());
        }
    } else {
        match Statement::prepare(&mut conn.table, trimmed_input) {
            Err(err) => print_output(writer, &err.to_string()),
            Ok(mut stmt) => match stmt.execute() {
                Ok(result) => print_output(writer, &result),
                Err(err) => print_output(writer, &err.to_string()),
            },
        }
    }
    return true;
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::NamedTempFile;

    fn get_tempfile() -> String {
        let file = NamedTempFile::new().unwrap();
        return file.path().to_string_lossy().into();
    }

    #[test]
    fn test_insert_retrieve() {
        let tempfile = get_tempfile();
        let mut connection = Connection::new(&tempfile).unwrap();
        let commands = vec!["insert 1 user1 person1@example.com", "select"];
        let expected_outputs = vec!["Executed.\n", "(1, user1, person1@example.com)\n"];

        for (i, command) in commands.iter().enumerate() {
            let mut output = Vec::new();
            let input = prompt(command.as_bytes(), &mut output, "db > ");
            let mut output2 = Vec::new();
            handle_input(input, &mut connection, &mut output2);
            let output2 = String::from_utf8(output2).expect("Not UTF-8");
            assert_eq!(expected_outputs[i], output2);
        }
    }

    #[test]
    fn test_too_many_rows() {
        let num_rows = sqlite_clone::table::TABLE_MAX_ROWS + 1;
        let tempfile = get_tempfile();

        let mut connection = Connection::new(&tempfile).unwrap();
        for i in 0..num_rows {
            let mut output = Vec::new();
            let command = format!("insert {} user{} person{}@example.com", i, i, i);
            let input = prompt(command.as_bytes(), &mut output, "db > ");
            let mut output2 = Vec::new();
            handle_input(input, &mut connection, &mut output2);
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
        let tempfile = get_tempfile();

        let mut connection = Connection::new(&tempfile).unwrap();
        let mut output = Vec::new();
        let command = format!("insert 1 {} {}", username, email);
        let input = prompt(command.as_bytes(), &mut output, "db > ");
        let mut output2 = Vec::new();
        handle_input(input, &mut connection, &mut output2);
        let output2 = String::from_utf8(output2).expect("Not UTF-8");

        assert_eq!("String is too long.\n", output2);
    }

    #[test]
    fn test_negative_id() {
        let tempfile = get_tempfile();
        let mut connection = Connection::new(&tempfile).unwrap();
        let mut output = Vec::new();
        let command = "insert -1 foo bar".to_string();
        let input = prompt(command.as_bytes(), &mut output, "db > ");
        let mut output2 = Vec::new();
        handle_input(input, &mut connection, &mut output2);
        let output2 = String::from_utf8(output2).expect("Not UTF-8");

        assert_eq!("ID must be a positive integer.\n", output2);
    }
}
