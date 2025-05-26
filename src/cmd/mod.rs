use clap::Subcommand;

mod benchmark;
mod broken;
mod db;
mod diff;
mod exif;
mod find;
mod hash;
mod index;
mod separate;
mod validate;

#[derive(Subcommand)]
pub enum MainCommand {
    #[command(subcommand)]
    Benchmark(benchmark::Benchmark),
    Broken(broken::Broken),
    #[command(subcommand)]
    Db(db::Db),
    Diff(diff::Diff),
    Exif(exif::Exif),
    Find(find::Find),
    Hash(hash::Hash),
    Index(index::Index),
    Separate(separate::Separate),
    Validate(validate::Validate),
}

impl MainCommand {
    pub async fn run(self) -> anyhow::Result<()> {
        match self {
            MainCommand::Benchmark(cmd) => cmd.run().await,
            MainCommand::Broken(cmd) => cmd.run().await,
            MainCommand::Db(cmd) => cmd.run().await,
            MainCommand::Diff(cmd) => cmd.run().await,
            MainCommand::Exif(cmd) => cmd.run().await,
            MainCommand::Find(cmd) => cmd.run().await,
            MainCommand::Hash(cmd) => cmd.run().await,
            MainCommand::Index(cmd) => cmd.run().await,
            MainCommand::Separate(cmd) => cmd.run().await,
            MainCommand::Validate(cmd) => cmd.run().await,
        }
    }
}
