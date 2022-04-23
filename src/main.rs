use walkdir::WalkDir;
//use tokio::prelude::*;
//use tokio::runtime::Runtime;
//use rayon::prelude::*;
use crossbeam_channel::unbounded;
//use std::thread;
use anyhow::{anyhow, Result};
use futures::channel::oneshot::channel;
use hex::ToHex;
use sha1::{Digest, Sha1};
use sqlite::Connection;
use std::cmp::min;
use std::ffi::OsStr;
use std::fmt;
use std::fs::File;
use std::io::{Read, Write};
use std::path::PathBuf;
use std::process::{Command, Stdio};

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

#[derive(Debug, Clone)]
struct CommandError(String);

impl fmt::Display for CommandError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "error executing command {}", self.0)
    }
}

impl std::error::Error for CommandError {}

fn hash_djpeg<T: Into<Stdio>>(stdin: T) -> Result<Hash> {
    let output = Command::new("djpeg")
        .args(&["-dct", "int", "-dither", "none", "-nosmooth", "-bmp"])
        .stdin(stdin)
        .stderr(Stdio::inherit())
        .output()?;

    if !output.status.success() {
        return Err(CommandError("failed to run djpeg".to_string()).into());
    }

    let mut hasher = Sha1::new();
    hasher.update(&output.stdout);
    Ok(hasher.finalize().into())
}

fn hash_jpegtran<T: Into<Stdio>>(stdin: T, args: &[&str]) -> Result<Hash> {
    let mut child = Command::new("jpegtran")
        .args(args)
        .stdin(stdin)
        .stdout(Stdio::piped())
        .stderr(Stdio::inherit())
        .spawn()?;

    let stdout = child.stdout.take().unwrap();
    let result = hash_djpeg(stdout);

    let status = child.wait().expect("child wasn't running");
    if !status.success() {
        return Err(CommandError("failed to run jpegtran".to_string()).into());
    }

    result
}

fn imagehash(path: &PathBuf) -> Result<Hash> {
    let ((h1, h2), (h3, h4)) = rayon::join(
        || {
            rayon::join(
                || -> Result<Hash> {
                    let file = File::open(path)?;
                    hash_djpeg(file)
                },
                || -> Result<Hash> {
                    let file = File::open(path)?;
                    hash_jpegtran(file, &["-rotate", "90"])
                },
            )
        },
        || {
            rayon::join(
                || -> Result<Hash> {
                    let file = File::open(path)?;
                    hash_jpegtran(file, &["-rotate", "180"])
                },
                || -> Result<Hash> {
                    let file = File::open(path)?;
                    hash_jpegtran(file, &["-rotate", "270"])
                },
            )
        },
    );
    Ok(min(min(h1?, h2?), min(h3?, h4?)))
}

enum PoolType {
    Cpu,
    Io,
}

fn get_hasher(path: &PathBuf) -> (PoolType, fn(&PathBuf) -> Result<Hash>) {
    let ext = path
        .extension()
        .and_then(&OsStr::to_str)
        .map(|s| s.to_ascii_lowercase())
        .unwrap_or("".into());
    if ext == "jpeg" || ext == "jpg" {
        (PoolType::Cpu, imagehash)
    } else {
        (PoolType::Io, sha1)
    }
}

fn get_path() -> Result<PathBuf> {
    match home::home_dir() {
        Some(path) => Ok(path),
        None => Err(anyhow!("Failed to get home directory")),
    }
}

fn open_database() -> Result<Connection> {
    Ok(Connection::open(get_path()?)?)
}

#[tokio::main]
async fn main() -> Result<()> {
    let io_pool = rayon::ThreadPoolBuilder::new().num_threads(4).build()?;
    let cpu_pool = rayon::ThreadPoolBuilder::new()
        .num_threads(num_cpus::get())
        .build()?;

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
            let (pool_type, hasher) = get_hasher(&path);
            let pool = match pool_type {
                PoolType::Cpu => &cpu_pool,
                PoolType::Io => &io_pool,
            };
            outputs_sender.send(output_receiver).unwrap();
            pool.spawn_fifo(move || {
                let hash = hasher(&path);
                output_sender.send((path, hash)).unwrap_or(())
            });
        }
    });

    let mut stdout = std::io::stdout();

    for output in outputs_receiver {
        let (path, hash) = output.await?;
        match hash {
            Ok(hash) => write!(
                &mut stdout,
                "{} *{}\n",
                hash.encode_hex::<String>(),
                path.display()
            )?,
            Err(e) => eprintln!("hashing {} failed: {}", path.display(), e),
        }
    }

    Ok(())
}
