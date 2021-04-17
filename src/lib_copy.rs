use eyre::{eyre, Result};
// use std::process::exit;

pub mod btree;
pub mod connection;
pub mod pager;
pub mod statement;
pub mod table;

use crate::connection::Connection;

pub fn do_meta_command(conn: &mut Connection, input: String) -> Result<()> {
    // if input == ".exit" {
    //     exit(0);
    if input == ".constants" {
        println!("ROW_SIZE: {}", crate::table::ROW_SIZE);
        println!(
            "COMMON_NODE_HEADER_SIZE: {}",
            btree::COMMON_NODE_HEADER_SIZE
        );
        println!("LEAF_NODE_HEADER_SIZE: {}", btree::LEAF_NODE_HEADER_SIZE);
        println!("LEAF_NODE_CELL_SIZE: {}", btree::LEAF_NODE_CELL_SIZE);
        println!(
            "LEAF_NODE_SPACE_FOR_CELLS: {}",
            btree::LEAF_NODE_SPACE_FOR_CELLS
        );
        println!("LEAF_NODE_MAX_CELLS: {}", btree::LEAF_NODE_MAX_CELLS);
        return Ok(());
    } else if input == ".btree" {
        let node = conn.table.get_page(0).unwrap();
        if let btree::Node::Leaf(nd) = node.as_ref() {
            println!("Tree:");
            println!("{}", nd.print_node());
        }
        return Ok(());
    } else {
        return Err(eyre!("Unrecognized command {}.", input));
    }
}
