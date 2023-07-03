use crate::iopool;
use anyhow::Result;
use image_hasher::HashAlg;
use imagehash::model::Hash32;
use imagehash::model::ImageMetadata;
use std::cmp::min;
use std::convert::TryInto;
use std::path::Path;
use std::sync::Arc;
use turbojpeg::TransformOp;

const USE_IMAGE_HASHER_BLOCKHASH: bool = false;

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

pub async fn compute_image_hashes(path: &Path) -> Result<ImageMetadata> {
    let file_contents = Arc::new(iopool::get_file_contents(path.to_owned()).await?);

    let fc = file_contents.clone();
    let rothash_jh = tokio::spawn(async move {
        rothash(fc).await
    });

    let transform = read_transform_from_exif(&file_contents)?;

    if USE_IMAGE_HASHER_BLOCKHASH {
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
            dimensions: Some((image.width(), image.height())),
            blockhash256: Some(h.as_bytes().try_into()?),
            jpegrothash: Some(rothash_jh.await??),
        })
    } else {
        let image = if transform != TransformOp::None {
            let jpeg_data = turbojpeg::transform(
                &turbojpeg::Transform {
                    op: transform,
                    ..Default::default()
                },
                &file_contents,
            )?;
            turbojpeg::decompress(&jpeg_data, turbojpeg::PixelFormat::RGB)?
        } else {
            turbojpeg::decompress(&file_contents, turbojpeg::PixelFormat::RGB)?
        };

        let phash = blockhash::blockhash256(&JpegPerceptualImage { image: &image });

        Ok(ImageMetadata {
            dimensions: Some((image.width as u32, image.height as u32)),
            blockhash256: Some(phash.into()),
            jpegrothash: Some(rothash_jh.await??),
        })
    }
}

fn read_transform_from_exif(mut file_contents: &[u8]) -> Result<TransformOp> {
    let exif_segment = exif::get_exif_attr_from_jpeg(&mut file_contents)?;
    let (fields, _little_endian) = exif::parse_exif(&exif_segment)?;

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
