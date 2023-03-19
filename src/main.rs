// TODO: fix
#![allow(dead_code)]
#![allow(unused)]

use anyhow::{anyhow, Context, Result};
use crossbeam_channel::unbounded;
use futures::channel::oneshot::channel;
use hex::ToHex;
use libheif_rs::{Channel, ColorSpace, HeifContext, ItemId, RgbChroma};
use sha1::{Digest, Sha1};
use std::cmp::min;
use std::ffi::OsStr;
use std::fmt;
use std::fs::File;
use std::io::{Read, Write};
use std::path::PathBuf;
use std::process::{Command, Stdio};
use std::sync::Arc;
use std::time::SystemTime;
use structopt::StructOpt;
use tokio::sync::{mpsc, oneshot};
use tokio::task::JoinHandle;
use walkdir::WalkDir;

mod database;
mod model;

use database::Database;
use model::{ContentMetadata, FileInfo, ImageMetadata};
use model::{Hash20, Hash32};

#[cfg(unix)]
use std::os::unix::fs::MetadataExt;
#[cfg(unix)]
use std::os::unix::process::CommandExt;
#[cfg(windows)]
use std::os::windows::fs::MetadataExt;

const BUFFER_SIZE: usize = 65536;

const PATH_CHANNEL_SIZE: usize = 8;
const RESULT_CHANNEL_SIZE: usize = 8;

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

fn compute_blake3(path: &PathBuf) -> Result<Hash32> {
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

struct HeifPerceptualImage<'a> {
    plane: &'a libheif_rs::Plane<&'a [u8]>,
}

impl blockhash::Image for HeifPerceptualImage<'_> {
    type Pixel = blockhash::Rgb<u8>;

    fn dimensions(&self) -> (u32, u32) {
        (self.plane.width, self.plane.height)
    }

    fn get_pixel(&self, x: u32, y: u32) -> Self::Pixel {
        let offset = self.plane.stride * y as usize + 3 * x as usize;
        let data = &self.plane.data;
        if offset + 2 >= data.len() {
            eprintln!("out of bound access x={}, y={}", x, y);
        }
        return blockhash::Rgb([data[offset], data[offset + 1], data[offset + 2]]);
    }
}

fn perceptual_hash(path: &PathBuf) -> Result<ImageMetadata> {
    let file = File::open(path)?;
    let size = file.metadata()?.len();
    // libheif_rs does not allow customizing its multithreading behavior, and
    // allocates new threads per decoded image.
    // TODO: replace with libheif_sys and call heif_context_set_max_decoding_threads(0).
    let reader = libheif_rs::StreamReader::new(file, size);

    let ctx = HeifContext::read_from_reader(Box::new(reader))?;
    let handle = ctx.primary_image_handle()?;
    //eprintln!("width and height: {} x {}", handle.width(), handle.height());

    // Get Exif
    let mut meta_ids: Vec<ItemId> = vec![0; 1];
    let count = handle.metadata_block_ids(&mut meta_ids, b"Exif");
    assert_eq!(count, 1);
    let exif: Vec<u8> = handle.metadata(meta_ids[0])?;

    //eprintln!("exif done");

    // Decode the image
    // TODO: ignore_transformations = true, then rotate four
    let image = handle.decode(ColorSpace::Rgb(RgbChroma::Rgb), None)?;
    assert_eq!(image.color_space(), Some(ColorSpace::Rgb(RgbChroma::Rgb)));
    //assert_eq!(image.width(Channel::Interleaved)?, 3024);
    //assert_eq!(image.height(Channel::Interleaved)?, 4032);

    //eprintln!("decode done");

    let planes = image.planes();
    let interleaved_plane = planes.interleaved.unwrap();
    assert!(!interleaved_plane.data.is_empty());
    assert!(interleaved_plane.stride > 0);

    //eprintln!("plane.stride = {}", interleaved_plane.stride);

    //eprintln!("pixels done");

    // perceptual hash

    let phash = blockhash::blockhash256(&HeifPerceptualImage {
        plane: &interleaved_plane,
    });

    //eprintln!("perceptual hash done");

    Ok(ImageMetadata {
        image_width: handle.width(),
        image_height: handle.height(),
        blockhash256: phash.into(),
    })
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

#[cfg(not(unix))]
trait FakeInode {
    fn ino(&self) -> u64;
}

#[cfg(not(unix))]
impl FakeInode for std::fs::Metadata {
    fn ino(&self) -> u64 {
        0
    }
}

impl FileInfo {
    fn from_dir_entry(entry: &walkdir::DirEntry) -> Result<FileInfo> {
        let metadata = entry.metadata()?;
        Ok(FileInfo {
            inode: metadata.ino(),
            size: metadata.len(),
            mtime: metadata.modified()?,
        })
    }
}

#[derive(Debug, StructOpt)]
#[structopt(name = "index", about = "Scan directories and update the index")]
struct Index {
    #[structopt(parse(from_os_str))]
    dirs: Vec<PathBuf>,
}

struct ProcessResult {
    content_metadata: ContentMetadata,
    image_metadata: ImageMetadata,
    // todo: information about whether either should be written to the database
}

async fn process_file(
    db: Arc<Database>,
    path: PathBuf,
    file_info: FileInfo,
    db_metadata: Option<ContentMetadata>,
) -> Result<(ContentMetadata, ImageMetadata)> {
    let b3 = match db_metadata {
        Some(ref record) => {
            // If metadata matches our records, we can assume blake3 hasn't changed.
            if file_info == record.file_info {
                record.blake3
            } else {
                compute_blake3(&path)?
            }
        }
        None => {
            // No record of this file - blake3 must be computed.
            eprintln!("computing blake3 of {}", path.display());
            compute_blake3(&path)?
        }
    };

    // TODO: is this an image?

    let image_metadata = match db.get_image_metadata(&b3)? {
        Some(im) => im,
        None => perceptual_hash(&path)?,
    };

    Ok((
        ContentMetadata {
            path,
            file_info,
            blake3: b3,
        },
        image_metadata,
    ))
}

impl Index {
    async fn run(&self) -> Result<()> {
        let db = Arc::new(Database::open()?);

        let dirs = if self.dirs.is_empty() {
            vec![".".into()]
        } else {
            self.dirs.clone()
        };

        let dirs: Vec<PathBuf> = dirs
            .iter()
            .map(|path| {
                path.canonicalize()
                    .with_context(|| format!("failed to canonicalize {}", path.display()))
            })
            .collect::<Result<_, _>>()?;

        let (path_tx, mut path_rx) = mpsc::channel(PATH_CHANNEL_SIZE);

        // Crawler task pushes work into path_tx.
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
                        let metadata = FileInfo::from_dir_entry(&e);

                        if let Err(_) = path_tx.send((e.into_path(), metadata)).await {
                            eprintln!("receiver dropped");
                            return;
                        }
                    }
                }
            }
        });

        let (metadata_tx, mut metadata_rx) = mpsc::channel(RESULT_CHANNEL_SIZE);

        // Reads enumerated paths and computes necessary file metadata and content hashes.
        let db2 = db.clone();
        tokio::spawn(async move {
            while let Some((path, metadata)) = path_rx.recv().await {
                let metadata = match metadata {
                    Ok(metadata) => metadata,
                    Err(err) => {
                        eprintln!("failed to read metadata of {}: {}", path.display(), err);
                        continue;
                    }
                };

                // Check the database to see if there's anything to recompute.
                let db_metadata = match db.get_file(&path) {
                    Ok(record) => record,
                    Err(err) => {
                        eprintln!("failed to read record for {}, {}", path.display(), err);
                        continue;
                    }
                };

                let metadata_future = tokio::spawn({
                    let db = db.clone();
                    async move { process_file(db, path, metadata, db_metadata).await }
                });

                if let Err(_) = metadata_tx.send(metadata_future).await {
                    eprintln!("receiver dropped");
                    return;
                }
            }
        });

        let db = db2;
        while let Some(content_metadata_future) = metadata_rx.recv().await {
            let content_metadata_future = content_metadata_future.await?;
            let (content_metadata, image_metadata) = match content_metadata_future {
                Ok(r) => r,
                Err(e) => {
                    eprintln!("failed to read the thing {}", e);
                    continue;
                }
            };
            println!(
                "{}: size = {}, blake3 = {}, blockhash = {}",
                content_metadata.path.display(),
                content_metadata.file_info.size,
                content_metadata.blake3.encode_hex::<String>(),
                hex::encode(&image_metadata.blockhash256),
            );
            db.add_files(&[&content_metadata])?;
            db.add_image_metadata(&content_metadata.blake3, &image_metadata)?;
        }

        /*
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
#[structopt(name = "path", about = "Print database location")]
struct DbPath {}

impl DbPath {
    async fn run(&self) -> Result<()> {
        println!("{}", database::get_database_path()?.display());
        Ok(())
    }
}

#[derive(Debug, StructOpt)]
#[structopt(name = "open", about = "Interactively explore database")]
struct DbOpen {}

impl DbOpen {
    async fn run(&self) -> Result<()> {
        let mut cmd = Command::new("sqlite3");
        cmd.arg(database::get_database_path()?);
        Self::exec(cmd)
    }

    #[cfg(unix)]
    fn exec(mut cmd: Command) -> Result<()> {
        Err(cmd.exec().into())
    }

    #[cfg(not(unix))]
    fn exec(mut cmd: Command) -> Result<()> {
        let status = cmd.status()?;
        if status.success() {
            Ok(())
        } else {
            Err(anyhow!("Failed to run sqlite3 command"))
        }
    }
}

#[derive(Debug, StructOpt)]
#[structopt(name = "db", about = "Database administration")]
enum Db {
    #[structopt(name = "path")]
    DbPath(DbPath),
    #[structopt(name = "open")]
    DbOpen(DbOpen),
}

impl Db {
    async fn run(&self) -> Result<()> {
        match self {
            Db::DbPath(cmd) => cmd.run().await,
            Db::DbOpen(cmd) => cmd.run().await,
        }
    }
}

#[derive(Debug, StructOpt)]
#[structopt(name = "imagehash", about = "Index your files")]
enum Opt {
    Index(Index),
    Db(Db),
}

impl Opt {
    async fn run(&self) -> Result<()> {
        match self {
            Opt::Index(cmd) => cmd.run().await,
            Opt::Db(cmd) => cmd.run().await,
        }
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    let opt = Opt::from_args();
    opt.run().await
}
