use crate::hash::unsupported_photo;
use crate::hash::ImageMetadataError;
use crate::hash::USE_IMAGE_HASHER_BLOCKHASH;
use crate::iopool;
use crate::model::Hash32;
use crate::model::ImageMetadata;
use anyhow::Context;
use anyhow::Result;
use image_hasher::HashAlg;
use std::cmp::min;
use std::convert::TryInto;
use std::path::Path;
use std::sync::Arc;
use turbojpeg::TransformOp;

const DECOMPRESS_ERROR: &str = "failed to decompress JPEG";
const TRANSFORM_ERROR: &str = "failed to transform JPEG";

struct JpegPerceptualImage<'a> {
    image: &'a turbojpeg::Image<Vec<u8>>,
}

impl blockhash::Image for JpegPerceptualImage<'_> {
    const MAX_BRIGHTNESS: u32 = 255 * 3;

    fn dimensions(&self) -> (u32, u32) {
        (self.image.width as u32, self.image.height as u32)
    }

    fn brightness(&self, x: u32, y: u32) -> u32 {
        let offset = self.image.pitch * y as usize + 3 * x as usize;
        let data: &[u8] = &self.image.pixels;
        let r: u32 = data[offset] as _;
        let g: u32 = data[offset + 1] as _;
        let b: u32 = data[offset + 2] as _;
        r + g + b
    }
}

pub async fn compute_image_hashes(
    path: &Path,
) -> std::result::Result<ImageMetadata, ImageMetadataError> {
    let file_contents = Arc::new(iopool::get_file_contents(path.to_owned()).await?);

    let fc = file_contents.clone();
    let rothash_jh = tokio::spawn(rothash(fc));

    let transform = read_transform_from_exif(&file_contents).map_err(unsupported_photo(path))?;

    if USE_IMAGE_HASHER_BLOCKHASH {
        let image: image::RgbImage = if transform != TransformOp::None {
            let jpeg_data =
                turbojpeg::transform(&turbojpeg::Transform::op(transform), &file_contents)
                    .context(TRANSFORM_ERROR)?;
            turbojpeg::decompress_image(&jpeg_data)
                .context("decompress_image")
                .map_err(unsupported_photo(path))?
        } else {
            turbojpeg::decompress_image(&file_contents)
                .context("decompress_image")
                .map_err(unsupported_photo(path))?
        };

        let hasher = image_hasher::HasherConfig::new()
            .hash_alg(HashAlg::Blockhash)
            .hash_size(16, 16)
            .to_hasher();
        let h = hasher.hash_image(&image);
        Ok(ImageMetadata {
            dimensions: Some((image.width(), image.height())),
            blockhash256: Some(h.as_bytes().try_into()?),
            jpegrothash: Some(rothash_jh.await??),
        })
    } else {
        let image = if transform != TransformOp::None {
            let jpeg_data =
                turbojpeg::transform(&turbojpeg::Transform::op(transform), &file_contents)
                    .context(TRANSFORM_ERROR)
                    .map_err(unsupported_photo(path))?;
            turbojpeg::decompress(&jpeg_data, turbojpeg::PixelFormat::RGB)
                .context(DECOMPRESS_ERROR)
                .map_err(unsupported_photo(path))?
        } else {
            turbojpeg::decompress(&file_contents, turbojpeg::PixelFormat::RGB)
                .context(DECOMPRESS_ERROR)
                .map_err(unsupported_photo(path))?
        };

        let phash = blockhash::blockhash256(&JpegPerceptualImage { image: &image });

        Ok(ImageMetadata {
            dimensions: Some((image.width as u32, image.height as u32)),
            blockhash256: Some(phash.into()),
            jpegrothash: Some(rothash_jh.await?.map_err(unsupported_photo(path))?),
        })
    }
}

fn read_transform_from_exif(mut file_contents: &[u8]) -> Result<TransformOp> {
    let exif_segment = match exif::get_exif_attr_from_jpeg(&mut file_contents) {
        Ok(s) => s,
        Err(exif::Error::NotFound(_) | exif::Error::InvalidFormat(_)) => {
            return Ok(TransformOp::None);
        }
        Err(e) => {
            return Err(e).context("get_exif_attr_from_jpeg");
        }
    };
    let (fields, _little_endian) = match exif::parse_exif(&exif_segment).context("parse_exif") {
        Ok(v) => v,
        Err(_) => return Ok(TransformOp::None),
    };

    let mut transform = TransformOp::None;
    for field in fields {
        if let (exif::Tag::Orientation, exif::Value::Short(v)) = (field.tag, &field.value) {
            let Some(v) = v.first() else {
                continue;
            };
            // http://sylvana.net/jpegcrop/exif_orientation.html
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
    Ok(transform)
}

async fn rothash(jpeg_data: Arc<Vec<u8>>) -> Result<Hash32> {
    let jd0 = jpeg_data.clone();
    let jd90 = jpeg_data.clone();
    let jd180 = jpeg_data.clone();
    let jd270 = jpeg_data;

    // TODO: Is it worth running one of these immediately? One fewer
    // allocation, and maybe fewer cache misses?
    let (rot0, rot90, rot180, rot270) = tokio::try_join!(
        tokio::spawn(async move {
            let image = turbojpeg::decompress(&jd0, turbojpeg::PixelFormat::RGB)
                .context(DECOMPRESS_ERROR)?;
            Ok(blake3::hash(&image.pixels).into()) as Result<Hash32>
        }),
        tokio::spawn(async move {
            let jpeg_data = turbojpeg::transform(
                &turbojpeg::Transform::op(turbojpeg::TransformOp::Rot90),
                &jd90,
            )
            .context(TRANSFORM_ERROR)?;
            let image = turbojpeg::decompress(&jpeg_data, turbojpeg::PixelFormat::RGB)
                .context(DECOMPRESS_ERROR)?;
            Ok(blake3::hash(&image.pixels).into()) as Result<Hash32>
        }),
        tokio::spawn(async move {
            let jpeg_data = turbojpeg::transform(
                &turbojpeg::Transform::op(turbojpeg::TransformOp::Rot180),
                &jd180,
            )
            .context(TRANSFORM_ERROR)?;
            let image = turbojpeg::decompress(&jpeg_data, turbojpeg::PixelFormat::RGB)
                .context(DECOMPRESS_ERROR)?;
            Ok(blake3::hash(&image.pixels).into()) as Result<Hash32>
        }),
        tokio::spawn(async move {
            let jpeg_data = turbojpeg::transform(
                &turbojpeg::Transform::op(turbojpeg::TransformOp::Rot270),
                &jd270,
            )
            .context(TRANSFORM_ERROR)?;
            let image = turbojpeg::decompress(&jpeg_data, turbojpeg::PixelFormat::RGB)
                .context(DECOMPRESS_ERROR)?;
            Ok(blake3::hash(&image.pixels).into()) as Result<Hash32>
        }),
    )?;

    Ok(min(min(rot0?, rot90?), min(rot180?, rot270?)))
}
