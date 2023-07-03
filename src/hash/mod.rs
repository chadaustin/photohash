use anyhow::{bail, Result};
use imagehash::model::ImageMetadata;
use std::path::Path;

mod heic;
mod jpeg;

const JPEG_EXTENSIONS: &[&str] = &["jpg", "jpeg", "jpe", "jif", "jfif", "jfi"];
const HEIC_EXTENSIONS: &[&str] = &["heif", "heic"];

fn has_any_extension(path: &Path, exts: &[&str]) -> bool {
    path.extension()
        .map(|e| exts.iter().any(|x| e.eq_ignore_ascii_case(x)))
        .unwrap_or(false)
}

pub fn is_jpeg(path: &Path) -> bool {
    has_any_extension(path, JPEG_EXTENSIONS)
}

pub fn is_heic(path: &Path) -> bool {
    has_any_extension(path, HEIC_EXTENSIONS)
}

pub async fn compute_image_hashes(path: &Path) -> Result<ImageMetadata> {
    if is_jpeg(path) {
        jpeg::compute_image_hashes(path).await
    } else if is_heic(path) {
        heic::compute_image_hashes(path).await
    } else {
        bail!("not a photo");
    }
}
