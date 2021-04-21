# SQLite clone

This is just a little toy project for me to learn more about how databases work. The idea is to create some functional subset of SQLite in Rust.

I originally started by following [this tutorial](https://cstack.github.io/db_tutorial/) but translating the C code to Rust, but...I'm now kinda just doing my own thing.

Right now this can handle reading in a basic SQLite file -- pull in and parse pages from disk and traverse a b-tree to select records. But it's read-only at the moment, does not handle views or triggers, and doesn't handle any of the hundred different config options that SQLite provides. It also doesn't parse SQL code. So uhh...not quite production-ready, I guess?