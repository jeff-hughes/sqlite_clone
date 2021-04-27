use eyre::Result;
use std::cell::RefCell;
// use std::collections::HashMap;
use std::rc::Rc;
use std::{env, process::exit};

// use sqlite_clone::btree::{Btree, Record};
// use sqlite_clone::datatypes::{DataType, Value, VarInt};
use sqlite_clone::pager::{FreelistPage, Pager};
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
    let mut pgr_borrow = pager.borrow_mut();

    let mut freelist_pages = Vec::new();
    freelist_pages.push(db_options.first_freelist as usize);
    let freelist =
        FreelistPage::deserialize(&pgr_borrow.read_from_file(db_options.first_freelist as usize)?)?;
    freelist_pages.extend(freelist.free_pages);

    while let Some(next) = freelist.next_page_link {
        freelist_pages.push(next);
        let freelist = FreelistPage::deserialize(&pgr_borrow.read_from_file(next)?)?;
        freelist_pages.extend(&freelist.free_pages);
    }
    // println!("{} {:?}", freelist_page_nums.len(), freelist_page_nums);

    // let mut ints = Vec::new();
    // let pg = pgr_borrow.read_from_file(db_options.first_freelist as usize)?;
    // for n in 0..(pg.len() / 4) {
    //     ints.push(sqlite_clone::parsing::be_u32(&pg[(n * 4)..(n * 4) + 4])? as usize);
    // }
    // println!("{:?}", ints);

    let bytes_in = std::fs::read(&filename)?;

    let mut bytes_out = Vec::new();
    bytes_out.extend(db_options.serialize());
    for pg_num in 1..=10 {
        if !freelist_pages.contains(&pg_num) {
            let page = pgr_borrow.get_page(pg_num)?;
            println!("{} {}", pg_num, page.get_page_type());
            bytes_out.extend(page.serialize());
        } else {
        }
    }

    println!("Output length: {}", bytes_out.len());
    let mut all_identical = None;
    for (i, b) in bytes_out.iter().enumerate() {
        if *b != bytes_in[i] {
            println!("Output file is not the same as the input! At length {}", i);
            all_identical = Some(i);
            break;
        }
    }
    if all_identical.is_none() {
        println!("Output file is the same as the input");
    } else {
        let i = all_identical.unwrap();
        // let min_val = if i < 5 { 0 } else { i - 5 };
        let min_val = i - 20;
        let max_val = if i + 5 > bytes_in.len() || i + 5 > bytes_out.len() {
            std::cmp::min(bytes_in.len(), bytes_out.len())
        } else {
            i + 5
        };
        println!("{:?}", &bytes_in[min_val..max_val]);
        println!("{:?}", &bytes_out[min_val..max_val]);
    }

    // let schema = Btree::new(
    //     "sqlite_schema".to_string(),
    //     "sqlite_schema".to_string(),
    //     1,
    //     &db_options,
    //     pager.clone(),
    // );
    // let sqlite_schema = schema.list_records();

    // sqlite_schema has the following layout:
    // CREATE TABLE sqlite_schema(
    //     type text,
    //     name text,
    //     tbl_name text,
    //     rootpage integer,
    //     sql text
    // );

    // let mut tables = HashMap::new();
    // let mut indexes = HashMap::new();
    // for (_, table) in sqlite_schema {
    //     let table_vals = table.values;

    //     match &table_vals[0] {
    //         Value::String(ttype) if ttype == "table" || ttype == "index" => {
    //             // rootpage should always be an i8 value for tables and
    //             // indexes, and 0 or NULL for views, triggers, and
    //             // virtual tables
    //             let name = match &table_vals[1] {
    //                 Value::String(val) => Ok(val.clone()),
    //                 _ => Err("Unexpected value"),
    //             }
    //             .unwrap();
    //             let table_name = match &table_vals[2] {
    //                 Value::String(val) => Ok(val.clone()),
    //                 _ => Err("Unexpected value"),
    //             }
    //             .unwrap();
    //             let root_page = match &table_vals[3] {
    //                 Value::Int8(val) => Ok(*val as usize),
    //                 _ => Err("Unexpected value"),
    //             }
    //             .unwrap();

    //             if ttype == "table" {
    //                 tables.insert(
    //                     name.clone(),
    //                     Btree::new(name, table_name, root_page, &db_options, pager.clone()),
    //                 );
    //             } else if ttype == "index" {
    //                 indexes.insert(
    //                     name.clone(),
    //                     Btree::new(name, table_name, root_page, &db_options, pager.clone()),
    //                 );
    //             }
    //         }
    //         _ => (),
    //     }
    // }

    // println!("Tables:");
    // for key in tables.keys() {
    //     println!(" - {}", key);
    // }
    // println!("Indexes:");
    // for key in indexes.keys() {
    //     println!(" - {}", key);
    // }

    // // navigate an index
    // let podcasts_index = indexes.get("sqlite_autoindex_podcasts_1").unwrap();
    // let index_str = "https://feeds.megaphone.fm/replyall".to_string();

    // let index = Record::new(
    //     vec![DataType::String(index_str.len())],
    //     vec![Value::String(index_str)],
    // );
    // let res = podcasts_index.get_index(index);
    // println!("{:?}", res);

    // // pull corresponding row from table
    // if let Some(rec) = res {
    //     let row_id = rec.values.last().unwrap().get_int_val();
    //     if let Some(row_id) = row_id {
    //         let podcasts_table = tables.get("podcasts").unwrap();
    //         let row = podcasts_table.get_row(VarInt::new(row_id));
    //         println!("{:?}", row);
    //     }
    // }

    Ok(())
}
