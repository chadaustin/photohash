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
    let (paths_sender, paths_receiver) = unbounded();

    rayon::spawn(move || {
        for entry in WalkDir::new(".") {
            if let Ok(e) = entry {
                paths_sender.send(e.into_path()).unwrap();
            }
        }
    });

    let (outputs_sender, outputs_receiver) = unbounded();

    rayon::spawn(move || {
        for path in paths_receiver {
            let (output_sender, output_receiver) = channel();
            outputs_sender.send(output_receiver).unwrap();
            rayon::spawn(move || {
                let hash = sha1(&path);
                output_sender.send((path, hash)).unwrap();
            });
        }
    });

    for output in outputs_receiver {
        println!("{:?}", output.await?);
    }

    Ok(())
}
