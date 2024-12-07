#![allow(clippy::redundant_pattern_matching)]

use clap::Parser;

mod cmd;

#[cfg(not(target_env = "msvc"))]
#[global_allocator]
static GLOBAL: jemallocator::Jemalloc = jemallocator::Jemalloc;

#[derive(Parser)]
#[command(name = "photohash", about = "Index your files", version)]
struct Args {
    #[command(subcommand)]
    command: cmd::MainCommand,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let opt = Args::parse_from(wild::args_os());
    opt.command.run().await
}
