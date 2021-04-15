use eyre::{eyre, Result};
use std::mem;

use crate::table::Row;
use crate::table::PAGE_SIZE;

// Common node header layout
const NODE_TYPE_SIZE: usize = mem::size_of::<u8>();
const NODE_TYPE_OFFSET: usize = 0;
const IS_ROOT_SIZE: usize = mem::size_of::<bool>();
const IS_ROOT_OFFSET: usize = NODE_TYPE_SIZE;
const PARENT_POINTER_SIZE: usize = mem::size_of::<&u32>();
const PARENT_POINTER_OFFSET: usize = IS_ROOT_OFFSET + IS_ROOT_SIZE;
pub const COMMON_NODE_HEADER_SIZE: usize = NODE_TYPE_SIZE + IS_ROOT_SIZE + PARENT_POINTER_SIZE;

// Leaf node header layout
const LEAF_NODE_NUM_CELLS_SIZE: usize = mem::size_of::<u32>();
const LEAF_NODE_NUM_CELLS_OFFSET: usize = COMMON_NODE_HEADER_SIZE;
pub const LEAF_NODE_HEADER_SIZE: usize = COMMON_NODE_HEADER_SIZE + LEAF_NODE_NUM_CELLS_SIZE;

// Leaf node body layout
const LEAF_NODE_KEY_SIZE: usize = mem::size_of::<u32>();
const LEAF_NODE_KEY_OFFSET: usize = 0;
const LEAF_NODE_VALUE_SIZE: usize = crate::table::ROW_SIZE;
const LEAF_NODE_VALUE_OFFSET: usize = LEAF_NODE_KEY_OFFSET + LEAF_NODE_KEY_SIZE;
pub const LEAF_NODE_CELL_SIZE: usize = LEAF_NODE_KEY_SIZE + LEAF_NODE_VALUE_SIZE;
pub const LEAF_NODE_SPACE_FOR_CELLS: usize = crate::table::PAGE_SIZE - LEAF_NODE_HEADER_SIZE;
pub const LEAF_NODE_MAX_CELLS: usize = LEAF_NODE_SPACE_FOR_CELLS / LEAF_NODE_CELL_SIZE;

#[derive(Debug, Clone, Copy)]
struct Cell {
    key: u32,
    value: Row,
}

impl Cell {
    pub fn new(key: u32, value: Row) -> Self {
        return Self {
            key: key,
            value: value,
        };
    }
}

impl Default for Cell {
    fn default() -> Self {
        return Self {
            key: u32::default(),
            value: Row::default(),
        };
    }
}

#[derive(Debug)]
pub enum Node {
    Internal(InternalNode),
    Leaf(LeafNode),
}

impl Node {
    pub fn new(is_leaf: bool) -> Self {
        if is_leaf {
            return Self::Leaf(LeafNode::new());
        } else {
            return Self::Internal(InternalNode::new());
        }
    }

    pub fn serialize(&self) -> [u8; PAGE_SIZE] {
        // always output an array of PAGE_SIZE, even
        // if page is not full
        let mut output = [u8::default(); PAGE_SIZE];
        // TODO
        // for (i, row) in self.rows.iter().enumerate() {
        //     let bytes = row.serialize();
        //     let start_pos = i * ROW_SIZE;
        //     for j in 0..bytes.len() {
        //         output[start_pos + j] = bytes[j];
        //     }
        // }
        return output;
    }

    pub fn deserialize(bytes: &[u8]) -> Self {
        // TODO
        return Self::new(true);
        //     let mut rows = [Row::default(); ROWS_PER_PAGE];
        //     for i in 0..ROWS_PER_PAGE {
        //         let start = i * ROW_SIZE;
        //         let end = start + ROW_SIZE;
        //         if start >= bytes.len() || end >= bytes.len() {
        //             break;
        //         }
        //         rows[i] = Row::deserialize(&bytes[(i * ROW_SIZE)..(i * ROW_SIZE + ROW_SIZE)]);
        //     }
        //     return Self { rows: rows };
    }
}

#[derive(Debug)]
pub struct InternalNode {}

impl InternalNode {
    pub fn new() -> Self {
        return Self {};
    }
}

#[derive(Debug)]
pub struct LeafNode {
    pub is_root: bool,
    //parent_pointer: &LeafNode,
    pub num_cells: usize,
    cells: [Cell; LEAF_NODE_MAX_CELLS],
}

impl LeafNode {
    pub fn new() -> Self {
        return Self {
            is_root: false,
            num_cells: 0,
            cells: [Cell::default(); LEAF_NODE_MAX_CELLS],
        };
    }

    pub fn num_cells(&self) -> usize {
        return self.num_cells;
    }

    // pub fn get_cell(&self, cell_num: usize) -> &Cell {
    //     return &self.cells[cell_num];
    // }

    pub fn get_key(&self, cell_num: usize) -> &u32 {
        return &self.cells[cell_num].key;
    }

    pub fn get_value(&self, cell_num: usize) -> &Row {
        return &self.cells[cell_num].value;
    }

    pub fn insert(&mut self, cell_num: usize, key: u32, value: Row) -> Result<()> {
        if self.num_cells > LEAF_NODE_MAX_CELLS {
            // node full
            return Err(eyre!("Need to implement splitting a leaf node."));
        }

        if cell_num < self.num_cells {
            // make room for new cell
            for i in self.num_cells..cell_num {
                self.cells[i - 1] = self.cells[i];
            }
        }

        self.num_cells += 1;
        let cell = Cell::new(key, value);
        self.cells[cell_num] = cell;
        return Ok(());
    }

    pub fn print_node(&self) -> String {
        let mut output = String::new();
        output += &format!("Leaf (size {})\n", self.num_cells);
        for i in 0..self.num_cells {
            output += &format!("  {}: {}\n", i, self.get_key(i));
        }
        return output;
    }
}
