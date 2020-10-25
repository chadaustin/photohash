use walkdir::WalkDir;
//use tokio::prelude::*;
//use tokio::runtime::Runtime;
//use rayon::prelude::*;
use crossbeam_channel::unbounded;
//use std::thread;
use futures::channel::oneshot::channel;
use anyhow::Result;
use sha1::{Sha1, Digest};
use std::path::PathBuf;
use std::fs::File;
use std::io::Read;
use hex::ToHex;

const BUFFER_SIZE: usize = 65536;

type Hash = [u8; 20];

fn sha1(path: &PathBuf) -> Result<Hash> {
    let mut hasher = Sha1::new();
    let mut file = File::open(path)?;
    let mut buffer = [0u8; BUFFER_SIZE];
    loop {
        let n = file.read(&mut buffer)?;
        if n == 0 {
            break;
        }
        hasher.update(&buffer[..n]);
    }
    Ok(hasher.finalize().into())
}

#[tokio::main]
async fn main() -> Result<()> {
    let io_pool = rayon::ThreadPoolBuilder::new().num_threads(4).build()?;
    let cpu_pool = rayon::ThreadPoolBuilder::new().num_threads(num_cpus::get()).build()?;

    let (paths_sender, paths_receiver) = unbounded();

    rayon::spawn(move || {
        for entry in WalkDir::new(".") {
            if let Ok(e) = entry {
                if e.file_type().is_file() {
                    paths_sender.send(e.into_path()).unwrap();
                }
            }
        }
    });

    let (outputs_sender, outputs_receiver) = unbounded();

    rayon::spawn(move || {
        for path in paths_receiver {
            let (output_sender, output_receiver) = channel();
            outputs_sender.send(output_receiver).unwrap();
            io_pool.spawn_fifo(move || {
                let hash = sha1(&path);
                output_sender.send((path, hash)).unwrap();
            });
        }
    });

    for output in outputs_receiver {
        let (path, hash) = output.await?;
        match hash {
            Ok(hash) => println!("{} {}", hash.encode_hex::<String>(), path.display()),
            Err(e) => eprintln!("hashing {} failed: {}", path.display(), e)
        }
    }

    Ok(())
}
