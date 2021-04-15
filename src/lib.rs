use eyre::{eyre, Result};
// use std::process::exit;

pub mod btree;
pub mod connection;
pub mod statement;
pub mod table;

pub fn do_meta_command(input: String) -> Result<()> {
    // if input == ".exit" {
    //     exit(0);
    // } else {
    return Err(eyre!("Unrecognized command {}.", input));
    // }
}
