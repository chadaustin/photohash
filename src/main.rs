#![allow(clippy::redundant_pattern_matching)]

use clap::Parser;
use photohash::iopool;
use photohash::model::Hash32;
use std::io::Read;
use std::path::PathBuf;

mod cmd;

#[cfg(not(target_env = "msvc"))]
#[global_allocator]
static GLOBAL: jemallocator::Jemalloc = jemallocator::Jemalloc;

const READ_SIZE: usize = 65536;

async fn compute_blake3(path: PathBuf) -> anyhow::Result<Hash32> {
    // This assumes that computing blake3 is much faster than IO and
    // will not contend with other workers.
    iopool::run_in_io_pool(move || {
        let mut hasher = blake3::Hasher::new();
        let mut file = std::fs::File::open(path)?;
        let mut buffer = [0u8; READ_SIZE];
        loop {
            let n = file.read(&mut buffer)?;
            if n == 0 {
                break;
            }
            hasher.update(&buffer[..n]);
        }
        Ok(hasher.finalize().into())
    })
    .await
}

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
