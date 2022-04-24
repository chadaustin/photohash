use anyhow::{anyhow, Result};
use crossbeam_channel::unbounded;
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
use structopt::StructOpt;
use tokio::sync::mpsc;
use walkdir::WalkDir;

#[cfg(windows)]
use std::os::windows::fs::MetadataExt;

const BUFFER_SIZE: usize = 65536;

type Hash20 = [u8; 20];
type Hash32 = [u8; 32];

fn sha1(path: &PathBuf) -> Result<Hash20> {
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

fn blake3(path: &PathBuf) -> Result<Hash32> {
    let mut hasher = blake3::Hasher::new();
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

fn hash_djpeg<T: Into<Stdio>>(stdin: T) -> Result<Hash20> {
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

fn hash_jpegtran<T: Into<Stdio>>(stdin: T, args: &[&str]) -> Result<Hash20> {
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

fn imagehash(path: &PathBuf) -> Result<Hash20> {
    let ((h1, h2), (h3, h4)) = rayon::join(
        || {
            rayon::join(
                || -> Result<Hash20> {
                    let file = File::open(path)?;
                    hash_djpeg(file)
                },
                || -> Result<Hash20> {
                    let file = File::open(path)?;
                    hash_jpegtran(file, &["-rotate", "90"])
                },
            )
        },
        || {
            rayon::join(
                || -> Result<Hash20> {
                    let file = File::open(path)?;
                    hash_jpegtran(file, &["-rotate", "180"])
                },
                || -> Result<Hash20> {
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

fn get_hasher(path: &PathBuf) -> (PoolType, fn(&PathBuf) -> Result<Hash20>) {
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

fn get_database_path() -> Result<PathBuf> {
    let dirs = match directories::BaseDirs::new() {
        Some(dirs) => dirs,
        None => {
            return Err(anyhow!("Failed to find local config directory"));
        }
    };
    let mut path = PathBuf::from(dirs.config_dir());
    path.push(".imagehash.sqlite");
    eprintln!("the path is {}", path.display());
    Ok(path)
}

fn open_database() -> Result<Connection> {
    let conn = Connection::open(get_database_path()?)?;

    conn.execute(
        "
        CREATE TABLE IF NOT EXISTS files (
            path TEXT,
            inode INT,
            size INT,
            mtime INT,
            blake3 BLOB
        )
    ",
    )?;

    Ok(conn)
}

struct Indexer {
    connection: Connection,
}

impl Indexer {
    fn new() -> Result<Indexer> {
        let conn = open_database()?;

        Ok(Indexer { connection: conn })
    }
}

/// Platform-independent subset of file information to be stored in SQLite.
struct FileMetadata {
    /// 0 on Windows for now. May contain file_index() when the API is stabilized.
    inode: u64,
    size: u64,
    /// mtime in the local platform's units
    mtime: u64,
}

impl FileMetadata {
    #[cfg(windows)]
    fn fromDirEntry(entry: &walkdir::DirEntry) -> Result<FileMetadata> {
        let metadata = entry.metadata()?;
        Ok(FileMetadata {
            inode: 0,
            size: metadata.file_size(),
            mtime: metadata.last_write_time(),
        })
    }
}

#[derive(Debug, StructOpt)]
#[structopt(name = "index", about = "Scan directories and update the index")]
struct Index {
    #[structopt(parse(from_os_str))]
    dirs: Vec<PathBuf>,
}

const PATH_CHANNEL_SIZE: usize = 1000;

impl Index {
    async fn run(&self) -> Result<()> {
        let indexer = Indexer::new()?;

        let (path_tx, mut path_rx) = mpsc::channel(PATH_CHANNEL_SIZE);
        let dirs = if self.dirs.is_empty() {
            vec![".".into()]
        } else {
            self.dirs.clone()
        };
        let dirs: Vec<PathBuf> = dirs
            .iter()
            .map(|path| path.canonicalize())
            .collect::<Result<_, _>>()?;

        tokio::spawn(async move {
            for dir in dirs {
                for entry in WalkDir::new(dir) {
                    if let Ok(e) = entry {
                        if !e.file_type().is_file() {
                            continue;
                        }

                        // We could defer the stat() to the pulling thread, but:
                        // 1. We just read the directory, so maybe stat() is hot
                        // 2. Windows may provide some or all of this information
                        //    from the FindNextFile call.
                        let metadata = FileMetadata::fromDirEntry(&e);

                        if let Err(_) = path_tx.send((e.into_path(), metadata)).await {
                            eprintln!("receiver dropped");
                            return;
                        }
                    }
                }
            }
        });

        let handle = tokio::spawn(async move {
            while let Some((path, metadata)) = path_rx.recv().await {
                let metadata = match metadata {
                    Ok(metadata) => metadata,
                    Err(err) => {
                        eprintln!("failed to read metadata of {}: {}", path.display(), err);
                        continue;
                    }
                };

                let b3 = match blake3(&path) {
                    Ok(b3) => b3,
                    Err(err) => {
                        eprintln!("failed to read blake3 of {}: {}", path.display(), err);
                        continue;
                    }
                };

                println!(
                    "got = {}, size = {}, blake3 = {}",
                    path.display(),
                    metadata.size,
                    b3.encode_hex::<String>(),
                );
            }
        });

        handle.await?;

        /*
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
        */
        Ok(())
    }
}

#[derive(Debug, StructOpt)]
#[structopt(name = "imagehash", about = "Index your files")]
enum Opt {
    Index(Index),
}

impl Opt {
    async fn run(&self) -> Result<()> {
        match self {
            Opt::Index(index) => index.run().await,
        }
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    let opt = Opt::from_args();
    opt.run().await
}
