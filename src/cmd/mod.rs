#![allow(clippy::let_unit_value)]

use anyhow::Result;
use structopt::StructOpt;

mod benchmark;
mod db;
mod diff;
mod hash;
mod index;
mod separate;

pub use benchmark::Benchmark;
pub use db::Db;
pub use diff::Diff;
pub use hash::Hash;
pub use index::Index;
pub use separate::Separate;

#[derive(Debug, StructOpt)]
#[structopt(name = "imagehash", about = "Index your files")]
pub enum MainCommand {
    Benchmark(Benchmark),
    Db(Db),
    Diff(Diff),
    Hash(Hash),
    Index(Index),
    Separate(Separate),
}

impl MainCommand {
    pub async fn run(&self) -> Result<()> {
        match self {
            MainCommand::Benchmark(cmd) => cmd.run().await,
            MainCommand::Db(cmd) => cmd.run().await,
            MainCommand::Diff(cmd) => cmd.run().await,
            MainCommand::Hash(cmd) => cmd.run().await,
            MainCommand::Index(cmd) => cmd.run().await,
            MainCommand::Separate(cmd) => cmd.run().await,
        }
    }
}
