#![allow(clippy::redundant_pattern_matching)]

use clap::Parser;
use digest::DynDigest;
use photohash::iopool;
use photohash::model::{ExtraHashes, Hash32};
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

async fn compute_extra_hashes(path: PathBuf) -> anyhow::Result<ExtraHashes> {
    // TODO: To save IO, this could be folded into compute_blake3 on
    // the initial computation.

    iopool::run_in_io_pool(move || {
        let mut extra_hashes = ExtraHashes::default();

        let mut hash_md5 = md5::Md5::default();
        let mut hash_sha1 = sha1::Sha1::default();
        let mut hash_sha256 = sha2::Sha256::default();
        let mut hashers: [(&mut dyn DynDigest, &mut [u8]); 3] = [
            (&mut hash_md5, extra_hashes.md5.get_or_insert_default()),
            (&mut hash_sha1, extra_hashes.sha1.get_or_insert_default()),
            (&mut hash_sha256, extra_hashes.sha256.get_or_insert_default()),
        ];
        let mut file = std::fs::File::open(path)?;
        let mut buffer = [0u8; READ_SIZE];
        loop {
            let n = file.read(&mut buffer)?;
            if n == 0 {
                break;
            }
            let buffer = &buffer[..n];
            for (h, _) in &mut hashers {
                h.update(buffer);
            }
        }


        for (h, dest) in &mut hashers {
            h.finalize_into_reset(dest).expect("invalid buffer size");
        }

        Ok(extra_hashes)
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
