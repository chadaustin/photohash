// TODO: fix
#![allow(unused)]

use anyhow::{anyhow, bail, Context, Result};
use crossbeam_channel::unbounded;
use futures::channel::oneshot;
use hex::ToHex;
use image::buffer::ConvertBuffer;
use image::ImageBuffer;
use image_hasher::HashAlg;
use libheif_rs::{Channel, ColorSpace, HeifContext, ItemId, RgbChroma};
use std::cmp::min;
use std::convert::TryInto;
use std::ffi::OsStr;
use std::fmt;
use std::fs::File;
use std::io::{Read, Write};
use std::path::Path;
use std::path::PathBuf;
use std::process::{Command, Stdio};
use std::sync::Arc;
use std::sync::OnceLock;
use std::time::Instant;
use std::time::SystemTime;
use structopt::StructOpt;
use tokio::io::AsyncReadExt;
use tokio::runtime::Runtime;
use tokio::task::JoinHandle;
use turbojpeg::TransformOp;

mod cmd;

pub use imagehash::database::Database;
use imagehash::model::{ContentMetadata, FileInfo, ImageMetadata};
use imagehash::model::{Hash20, Hash32};

#[cfg(unix)]
use std::os::unix::process::CommandExt;

const READ_SIZE: usize = 65536;

static IO_POOL: OnceLock<rayon::ThreadPool> = OnceLock::new();
const IO_POOL_CONCURRENCY: usize = 4;

fn get_io_pool() -> &'static rayon::ThreadPool {
    IO_POOL.get_or_init(|| {
        rayon::ThreadPoolBuilder::new()
            .num_threads(IO_POOL_CONCURRENCY)
            .thread_name(|i| format!("io{i}"))
            .build()
            .unwrap()
    })
}

async fn run_in_io_pool<F, T>(f: F) -> T
where
    F: FnOnce() -> T + Send + 'static,
    T: Send + 'static,
{
    let (tx, rx) = oneshot::channel();

    get_io_pool().spawn(move || {
        _ = tx.send(f());
    });

    rx.await.unwrap()
}

async fn get_file_contents(path: PathBuf) -> Result<Vec<u8>> {
    Ok(run_in_io_pool(move || std::fs::read(path)).await?)
}

async fn compute_blake3(path: PathBuf) -> Result<Hash32> {
    /// This assumes that computing blake3 is much faster than IO and
    /// will not unnecessarily content with other workers.
    run_in_io_pool(move || {
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

pub async fn jpeg_rothash(path: PathBuf) -> Result<Hash32> {
    // jpeg_data outlives the async operation below: how do we tell the borrow checker?
    let jpeg_data = Arc::new(get_file_contents(path).await?);
    let jd0 = jpeg_data.clone();
    let jd90 = jpeg_data.clone();
    let jd180 = jpeg_data.clone();
    let jd270 = jpeg_data;

    let (rot0, rot90, rot180, rot270) = tokio::try_join!(
        tokio::spawn(async move {
            let image = turbojpeg::decompress(&jd0, turbojpeg::PixelFormat::RGB)?;
            Ok(blake3::hash(&image.pixels).into()) as Result<Hash32>
        }),
        tokio::spawn(async move {
            let jpeg_data = turbojpeg::transform(
                &turbojpeg::Transform {
                    op: turbojpeg::TransformOp::Rot90,
                    ..Default::default()
                },
                &jd90,
            )?;
            let image = turbojpeg::decompress(&jpeg_data, turbojpeg::PixelFormat::RGB)?;
            Ok(blake3::hash(&image.pixels).into()) as Result<Hash32>
        }),
        tokio::spawn(async move {
            let jpeg_data = turbojpeg::transform(
                &turbojpeg::Transform {
                    op: turbojpeg::TransformOp::Rot180,
                    ..Default::default()
                },
                &jd180,
            )?;
            let image = turbojpeg::decompress(&jpeg_data, turbojpeg::PixelFormat::RGB)?;
            Ok(blake3::hash(&image.pixels).into()) as Result<Hash32>
        }),
        tokio::spawn(async move {
            let jpeg_data = turbojpeg::transform(
                &turbojpeg::Transform {
                    op: turbojpeg::TransformOp::Rot270,
                    ..Default::default()
                },
                &jd270,
            )?;
            let image = turbojpeg::decompress(&jpeg_data, turbojpeg::PixelFormat::RGB)?;
            Ok(blake3::hash(&image.pixels).into()) as Result<Hash32>
        }),
    )?;

    Ok(min(min(rot0?, rot90?), min(rot180?, rot270?)))
}

struct JpegPerceptualImage<'a> {
    image: &'a turbojpeg::Image<Vec<u8>>,
}

impl blockhash::Image for JpegPerceptualImage<'_> {
    type Pixel = blockhash::Rgb<u8>;

    fn dimensions(&self) -> (u32, u32) {
        (self.image.width as u32, self.image.height as u32)
    }

    fn get_pixel(&self, x: u32, y: u32) -> Self::Pixel {
        let offset = self.image.pitch * y as usize + 3 * x as usize;
        let data = &self.image.pixels;
        blockhash::Rgb([data[offset], data[offset + 1], data[offset + 2]])
    }
}

async fn jpeg_perceptual_hash(path: PathBuf) -> Result<ImageMetadata> {
    let mut file_contents = get_file_contents(path).await?;

    let mut reader = file_contents.as_slice();
    let exif_segment = exif::get_exif_attr_from_jpeg(&mut reader)?;
    let (fields, _little_endian) = exif::parse_exif(&exif_segment)?;

    let mut transform = TransformOp::None;
    for field in fields {
        if let (exif::Tag::Orientation, exif::Value::Short(v)) = (field.tag, &field.value) {
            let Some(v) = v.first() else {
                continue;
            };
            transform = match v {
                1 => TransformOp::None,
                2 => TransformOp::Hflip,
                3 => TransformOp::Rot180,
                4 => TransformOp::Vflip,
                5 => TransformOp::Transpose,
                6 => TransformOp::Rot90,
                7 => TransformOp::Transverse,
                8 => TransformOp::Rot270,
                _ => TransformOp::None,
            };
        }
    }

    let image: image::RgbImage = if transform != TransformOp::None {
        let jpeg_data = turbojpeg::transform(
            &turbojpeg::Transform {
                op: transform,
                ..Default::default()
            },
            &file_contents,
        )?;
        turbojpeg::decompress_image(&jpeg_data)?
    } else {
        turbojpeg::decompress_image(&file_contents)?
    };

    let hasher = image_hasher::HasherConfig::new()
        .hash_alg(HashAlg::Blockhash)
        .hash_size(16, 16)
        .to_hasher();
    let h = hasher.hash_image(&image);
    Ok(ImageMetadata {
        image_width: image.width(),
        image_height: image.height(),
        blockhash256: h.as_bytes().try_into()?,
    })
    /*

        let phash = blockhash::blockhash256(&JpegPerceptualImage { image: &image });

        Ok(ImageMetadata {
            image_width: image.width as u32,
            image_height: image.height as u32,
            blockhash256: phash.into(),
        })
    */
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
        blockhash::Rgb([data[offset], data[offset + 1], data[offset + 2]])
    }
}

async fn heic_perceptual_hash(path: PathBuf) -> Result<ImageMetadata> {
    let libheif = libheif_rs::LibHeif::new();

    let file_contents = get_file_contents(path).await?;

    // libheif_rs does not allow customizing its multithreading behavior, and
    // allocates new threads per decoded image.
    let mut ctx = HeifContext::read_from_bytes(&file_contents)?;
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

    let plane = interleaved_plane;
    assert_eq!(plane.stride, 3 * plane.width as usize);

    let new_image: ImageBuffer<image::Rgb<u8>, Vec<u8>> =
        ImageBuffer::<image::Rgb<u8>, &[u8]>::from_raw(plane.width, plane.height, plane.data)
            .unwrap()
            .convert();

    let hasher = image_hasher::HasherConfig::new()
        .hash_alg(HashAlg::Blockhash)
        .hash_size(16, 16)
        .to_hasher();
    let h = hasher.hash_image(&new_image);

    /*
        let phash = blockhash::blockhash256(&HeifPerceptualImage {
            plane: &interleaved_plane,
        });
    */

    //eprintln!("perceptual hash done");

    Ok(ImageMetadata {
        image_width: handle.width(),
        image_height: handle.height(),
        blockhash256: h.as_bytes().try_into()?,
    })
}

async fn perceptual_hash(path: PathBuf) -> Result<ImageMetadata> {
    let Some(ext) = path.extension() else {
        bail!("not a photo");
    };
    if ext.eq_ignore_ascii_case("jpg") || ext.eq_ignore_ascii_case("jpeg") {
        jpeg_perceptual_hash(path).await
    } else if ext.eq_ignore_ascii_case("heic") {
        heic_perceptual_hash(path).await
    } else {
        bail!("not a photo");
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    let opt = crate::cmd::MainCommand::from_iter(wild::args_os());
    opt.run().await
}
