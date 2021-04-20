use eyre::{eyre, Result};
use std::cell::RefCell;
use std::rc::Rc;
use std::{env, process::exit};

use sqlite_clone::btree::{Btree, BtreePage};
use sqlite_clone::datatypes::Value;
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
    let sqlite_schema = schema.get_page(1)?;
    println!("{:?}", sqlite_schema);

    match sqlite_schema {
        BtreePage::TableLeaf(page) => {
            for table in page.records {
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
                        let btree =
                            Btree::new(name, table_name, root_page, &db_options, pager.clone());
                        let page = btree.get_page(root_page)?;
                        println!("{:?} {:?}", btree.name, page);
                    }
                    _ => (),
                }
            }
        }
        _ => return Err(eyre!("Could not read database schema.")),
    }

    Ok(())
}
