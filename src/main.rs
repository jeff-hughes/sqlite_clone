use eyre::Result;
use std::fs;
use std::{env, process::exit};

use sqlite_clone::{BtreePage, FileHeader, Value};

fn main() -> Result<()> {
    let args: Vec<String> = env::args().collect();
    if args.len() < 2 {
        println!("Error: Must supply a database filename.");
        exit(1);
    }
    let filename = &args[1];
    let input = fs::read(&filename)?;

    let file_header = FileHeader::parse(&input[..])?;
    println!("{:?}", file_header);

    let page_size = file_header.page_size as usize;

    let sqlite_schema = BtreePage::parse(&input[100..], 100, file_header.clone())?;
    println!("{:?}", sqlite_schema);

    for table in sqlite_schema.records {
        // sqlite_schema has the following layout:
        // CREATE TABLE sqlite_schema(
        //     type text,
        //     name text,
        //     tbl_name text,
        //     rootpage integer,
        //     sql text
        // );
        let table_vals = table.1.values;

        match &table_vals[0] {
            Value::String(ttype) if ttype == "table" => {
                // rootpage should always be an i8 value for tables and
                // indexes, and 0 or NULL for views, triggers, and
                // virtual tables
                let page_num = match table_vals[3] {
                    Value::Int8(val) => Ok(val as usize),
                    _ => Err("Unexpected value"),
                }
                .unwrap();
                let page_start = (page_num - 1) * page_size;
                let page_end = page_start + page_size;
                println!("{:?} {:x?} {:x?}", page_num, page_start, page_end);
                let btree = BtreePage::parse(&input[page_start..page_end], 0, file_header.clone())?;
                println!("{:?} {:?}", table_vals[2], btree);
            }
            _ => (),
        }
    }

    Ok(())
}
