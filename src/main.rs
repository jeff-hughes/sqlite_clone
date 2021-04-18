use eyre::{eyre, Result};
use std::fs;
use std::{env, process::exit};

use sqlite_clone::btree::{Btree, BtreePage, PageHeader};
use sqlite_clone::datatypes::Value;
use sqlite_clone::FileHeader;

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

    let schema_header = PageHeader::parse(&input[100..])?;
    let sqlite_schema = BtreePage::parse(&input[100..], 100, schema_header, &file_header)?;
    println!("{:?}", sqlite_schema);

    match sqlite_schema {
        BtreePage::TableLeaf(page) => {
            for table in page.records.into_iter().take(2) {
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
                    Value::String(ttype) if ttype == "table" || ttype == "index" => {
                        // rootpage should always be an i8 value for tables and
                        // indexes, and 0 or NULL for views, triggers, and
                        // virtual tables
                        let page_num = match table_vals[3] {
                            Value::Int8(val) => Ok(val as usize),
                            _ => Err("Unexpected value"),
                        }
                        .unwrap();
                        let btree = Btree::parse(&input[..], page_num, 0, &file_header)?;
                        println!("{:?} {:?}", table_vals[2], btree);
                    }
                    _ => (),
                }
            }
        }
        _ => return Err(eyre!("Could not read database schema.")),
    }

    Ok(())
}
