#![feature(iter_array_chunks)]
// TODO: fix
#![allow(dead_code)]
#![allow(clippy::let_unit_value)]

pub mod database;
pub mod iopool;
pub mod model;
pub mod mpmc;
pub mod scan;

pub use database::Database;
