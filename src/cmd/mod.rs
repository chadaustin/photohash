#![allow(clippy::let_unit_value)]

use anyhow::Result;
use structopt::StructOpt;

mod benchmark;
mod broken;
mod db;
mod diff;
mod exif;
mod hash;
mod index;
mod separate;

#[derive(Debug, StructOpt)]
#[structopt(name = "imagehash", about = "Index your files")]
pub enum MainCommand {
    Benchmark(benchmark::Benchmark),
    Broken(broken::Broken),
    Db(db::Db),
    Diff(diff::Diff),
    Exif(exif::Exif),
    Hash(hash::Hash),
    Index(index::Index),
    Separate(separate::Separate),
}

impl MainCommand {
    pub async fn run(self) -> Result<()> {
        match self {
            MainCommand::Benchmark(cmd) => cmd.run().await,
            MainCommand::Broken(cmd) => cmd.run().await,
            MainCommand::Db(cmd) => cmd.run().await,
            MainCommand::Diff(cmd) => cmd.run().await,
            MainCommand::Exif(cmd) => cmd.run().await,
            MainCommand::Hash(cmd) => cmd.run().await,
            MainCommand::Index(cmd) => cmd.run().await,
            MainCommand::Separate(cmd) => cmd.run().await,
        }
    }
}
