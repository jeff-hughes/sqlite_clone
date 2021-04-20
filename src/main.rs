use eyre::Result;
use std::cell::RefCell;
use std::collections::HashMap;
use std::rc::Rc;
use std::{env, process::exit};

use sqlite_clone::btree::Btree;
use sqlite_clone::datatypes::{Value, VarInt};
use sqlite_clone::pager::Pager;
use sqlite_clone::DbOptions;

fn main() -> Result<()> {
    let args: Vec<String> = env::args().collect();
    if args.len() < 2 {
        println!("Error: Must supply a database filename.");
        exit(1);
    }
    let filename = &args[1];
    let db_options = DbOptions::init(&filename)?;
    println!("{:?}", db_options);

    let pager = Rc::new(RefCell::new(Pager::new(&filename, &db_options)?));

    let schema = Btree::new(
        "sqlite_schema".to_string(),
        "sqlite_schema".to_string(),
        1,
        &db_options,
        pager.clone(),
    );
    let sqlite_schema = schema.list_records();

    // sqlite_schema has the following layout:
    // CREATE TABLE sqlite_schema(
    //     type text,
    //     name text,
    //     tbl_name text,
    //     rootpage integer,
    //     sql text
    // );
    let mut tables = HashMap::new();
    let mut indexes = HashMap::new();
    for (_, table) in sqlite_schema {
        let table_vals = table.values;

        match &table_vals[0] {
            Value::String(ttype) if ttype == "table" || ttype == "index" => {
                // rootpage should always be an i8 value for tables and
                // indexes, and 0 or NULL for views, triggers, and
                // virtual tables
                let name = match &table_vals[1] {
                    Value::String(val) => Ok(val.clone()),
                    _ => Err("Unexpected value"),
                }
                .unwrap();
                let table_name = match &table_vals[2] {
                    Value::String(val) => Ok(val.clone()),
                    _ => Err("Unexpected value"),
                }
                .unwrap();
                let root_page = match &table_vals[3] {
                    Value::Int8(val) => Ok(*val as usize),
                    _ => Err("Unexpected value"),
                }
                .unwrap();

                if ttype == "table" {
                    tables.insert(
                        name.clone(),
                        Btree::new(name, table_name, root_page, &db_options, pager.clone()),
                    );
                } else if ttype == "index" {
                    indexes.insert(
                        name.clone(),
                        Btree::new(name, table_name, root_page, &db_options, pager.clone()),
                    );
                }
            }
            _ => (),
        }
    }

    println!("Tables:");
    for key in tables.keys() {
        println!(" - {}", key);
    }
    println!("Indexes:");
    for key in indexes.keys() {
        println!(" - {}", key);
    }

    // pull a random row, just to check things are working
    let podcasts_table = tables.get("podcasts").unwrap();
    let row12 = podcasts_table.get_row(VarInt::new(12));
    println!("{:?}", row12);

    Ok(())
}
