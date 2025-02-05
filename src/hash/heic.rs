use crate::hash::unsupported_photo;
use crate::hash::ImageMetadataError;
use crate::hash::USE_IMAGE_HASHER_BLOCKHASH;
use crate::iopool;
use crate::model::ImageMetadata;
use image::buffer::ConvertBuffer;
use image::ImageBuffer;
use image_hasher::HashAlg;
use libheif_rs::ColorSpace;
use libheif_rs::HeifContext;
use libheif_rs::ItemId;
use libheif_rs::LibHeif;
use libheif_rs::Plane;
use libheif_rs::RgbChroma;
use std::convert::TryInto;
use std::path::Path;

struct HeifPerceptualImage<'a> {
    plane: &'a Plane<&'a [u8]>,
}

impl blockhash::Image for HeifPerceptualImage<'_> {
    const MAX_BRIGHTNESS: u32 = 255 * 3;

    fn dimensions(&self) -> (u32, u32) {
        (self.plane.width, self.plane.height)
    }

    fn brightness(&self, x: u32, y: u32) -> u32 {
        let offset = self.plane.stride * y as usize + 3 * x as usize;
        let data: &[u8] = self.plane.data;
        if offset + 2 >= data.len() {
            eprintln!("out of bound access x={}, y={}", x, y);
        }
        let r: u32 = data[offset] as _;
        let g: u32 = data[offset + 1] as _;
        let b: u32 = data[offset + 2] as _;
        r + g + b
    }
}

pub async fn compute_image_hashes(path: &Path) -> Result<ImageMetadata, ImageMetadataError> {
    let libheif = LibHeif::new();

    let file_contents = iopool::get_file_contents(path.to_owned()).await?;

    // libheif_rs does not allow customizing its multithreading behavior, and
    // allocates new threads per decoded image.
    let mut ctx = HeifContext::read_from_bytes(&file_contents).map_err(unsupported_photo(path))?;
    ctx.set_max_decoding_threads(0);
    let handle = ctx
        .primary_image_handle()
        .map_err(unsupported_photo(path))?;
    //eprintln!("width and height: {} x {}", handle.width(), handle.height());

    // Get Exif
    let mut meta_ids: Vec<ItemId> = vec![0; 1];
    let count = handle.metadata_block_ids(&mut meta_ids, b"Exif");
    _ = count;
    //assert_eq!(count, 1);
    /*
    let exif: Vec<u8> = handle.metadata(meta_ids[0])?;
    _ = exif;
     */

    //eprintln!("exif done");

    // Decode the image
    // TODO: ignore_transformations = true, then rotate four
    let image = libheif
        .decode(&handle, ColorSpace::Rgb(RgbChroma::Rgb), None)
        .map_err(unsupported_photo(path))?;
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

    if USE_IMAGE_HASHER_BLOCKHASH {
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

        Ok(ImageMetadata {
            dimensions: Some((handle.width(), handle.height())),
            blockhash256: Some(h.as_bytes().try_into()?),
            jpegrothash: None,
        })
    } else {
        let phash = blockhash::blockhash256(&HeifPerceptualImage {
            plane: &interleaved_plane,
        });

        Ok(ImageMetadata {
            dimensions: Some((handle.width(), handle.height())),
            blockhash256: Some(phash.into()),
            jpegrothash: None,
        })
    }
}
