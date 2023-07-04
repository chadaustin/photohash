use crate::model::ImageMetadata;
use anyhow::bail;
use anyhow::Result;
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

/// Returns true if path has a common JPEG file extension.
pub fn is_jpeg<P: AsRef<Path>>(path: P) -> bool {
    has_any_extension(path.as_ref(), JPEG_EXTENSIONS)
}

/// Returns true if path has a common HEIC file extension.
pub fn is_heic<P: AsRef<Path>>(path: P) -> bool {
    has_any_extension(path.as_ref(), HEIC_EXTENSIONS)
}

/// Returns true if path represents a file that may have ImageMetadata.
pub fn may_have_metadata<P: AsRef<Path>>(path: P) -> bool {
    is_jpeg(&path) || is_heic(&path)
}

/// Computes and returns image metadata including dimensions and
/// perceptual hashes.
pub async fn compute_image_hashes(path: &Path) -> Result<ImageMetadata> {
    if is_jpeg(path) {
        jpeg::compute_image_hashes(path).await
    } else if is_heic(path) {
        heic::compute_image_hashes(path).await
    } else {
        bail!("not a supported photo file: {}", path.display());
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_is_jpeg() {
        assert!(!is_jpeg("foo"));
        assert!(!is_jpeg("jpeg"));
        assert!(!is_jpeg("cake.heic"));
        assert!(!is_jpeg("readme.txt"));

        assert!(is_jpeg("foo.jpeg"));
        assert!(is_jpeg("foo.JPG"));
        assert!(is_jpeg("Hello World.Jpeg"));
    }
}
