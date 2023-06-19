#![feature(iter_array_chunks)]
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
use std::time::Instant;
use std::time::SystemTime;
use structopt::StructOpt;
use tokio::io::AsyncReadExt;
use tokio::sync::{mpsc, oneshot};
use tokio::task::JoinHandle;

mod cmd;
mod database;
mod model;
mod mpmc;
mod scan;

use database::Database;
use model::{ContentMetadata, FileInfo, ImageMetadata};
use model::{Hash20, Hash32};

#[cfg(unix)]
use std::os::unix::process::CommandExt;

const BUFFER_SIZE: usize = 65536;

async fn sha1(path: &PathBuf) -> Result<Hash20> {
    let mut hasher = Sha1::new();
    let mut file = tokio::fs::File::open(path).await?;
    let mut buffer = [0u8; BUFFER_SIZE];
    loop {
        let n = file.read(&mut buffer).await?;
        if n == 0 {
            break;
        }
        hasher.update(&buffer[..n]);
    }
    Ok(hasher.finalize().into())
}

async fn compute_blake3(path: PathBuf) -> Result<Hash32> {
    tokio::task::spawn_blocking(move || {
        let mut hasher = blake3::Hasher::new();
        let mut file = std::fs::File::open(path)?;
        let mut buffer = [0u8; BUFFER_SIZE];
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
    .unwrap()
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

fn jpeg_perceptual_hash(path: &PathBuf) -> Result<ImageMetadata> {
    Err(anyhow!("JPEGs not supported"))
}

fn heic_perceptual_hash(path: &PathBuf) -> Result<ImageMetadata> {
    let libheif = libheif_rs::LibHeif::new();

    let file = File::open(path)?;
    let size = file.metadata()?.len();
    // libheif_rs does not allow customizing its multithreading behavior, and
    // allocates new threads per decoded image.
    // TODO: replace with libheif_sys and call heif_context_set_max_decoding_threads(0).
    let reader = libheif_rs::StreamReader::new(file, size);

    let mut ctx = HeifContext::read_from_reader(Box::new(reader))?;
    ctx.set_max_decoding_threads(0);
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
    let image = libheif.decode(&handle, ColorSpace::Rgb(RgbChroma::Rgb), None)?;
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

/*
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
*/

struct ProcessResult {
    content_metadata: ContentMetadata,
    image_metadata: ImageMetadata,
    // todo: information about whether either should be written to the database
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
    Benchmark(cmd::Benchmark),
    Db(Db),
    Diff(cmd::Diff),
    Index(cmd::Index),
    Separate(cmd::Separate),
}

impl Opt {
    async fn run(&self) -> Result<()> {
        match self {
            Opt::Benchmark(cmd) => cmd.run().await,
            Opt::Db(cmd) => cmd.run().await,
            Opt::Diff(cmd) => cmd.run().await,
            Opt::Index(cmd) => cmd.run().await,
            Opt::Separate(cmd) => cmd.run().await,
        }
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    let opt = Opt::from_iter(wild::args_os());
    opt.run().await
}
