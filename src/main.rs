use anyhow::Result;
use imagehash::iopool;
use imagehash::model::Hash32;
use std::io::Read;
use std::path::PathBuf;
use structopt::StructOpt;

mod cmd;

const READ_SIZE: usize = 65536;

async fn compute_blake3(path: PathBuf) -> Result<Hash32> {
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

#[tokio::main]
async fn main() -> Result<()> {
    let opt = crate::cmd::MainCommand::from_iter(wild::args_os());
    opt.run().await
}
