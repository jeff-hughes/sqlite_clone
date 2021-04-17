//use eyre::Result;
use std::fs;
use std::{env, process::exit};

use sqlite_clone::{BtreePage, FileHeader};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args: Vec<String> = env::args().collect();
    if args.len() < 2 {
        println!("Error: Must supply a database filename.");
        exit(1);
    }
    let filename = &args[1];
    let input = fs::read(&filename)?;

    let (i, file_header) = FileHeader::parse(&input[..]).map_err(|e| format!("{:?}", e))?;
    println!("{:?}", file_header);

    // TODO: get rid of clone
    let (_, btree_header) = BtreePage::parse(i, input.len() - i.len(), file_header.clone())
        .map_err(|e| format!("{:?}", e))?;
    println!("{:?}", btree_header);

    Ok(())
}
