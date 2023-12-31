use crate::model::FileInfo;
use crate::model::ImageMetadata;
use std::path::Path;
use std::path::PathBuf;

mod heic;
mod jpeg;

const USE_IMAGE_HASHER_BLOCKHASH: bool = false;

const JPEG_EXTENSIONS: &[&str] = &["jpg", "jpeg", "jpe", "jif", "jfif", "jfi"];
const HEIC_EXTENSIONS: &[&str] = &["heif", "heic"];

// TODO: We could replace all of the custom impls with thiserror if we
// had a PathBuf wrapper type that Displayed with dunce::simplified.
#[derive(Debug)]
pub enum ImageMetadataError {
    //#[error("not a supported photo file: {path}")]
    UnsupportedPhoto {
        path: PathBuf,
        source: Option<anyhow::Error>,
    },
    //#[error("unexpected error reading image metadata")]
    Other(/*#[from]*/ anyhow::Error),
}

impl std::error::Error for ImageMetadataError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::UnsupportedPhoto { path, source } => {
                _ = path;
                source.as_ref().map(|e| e.as_ref())
            }
            Self::Other(source) => Some(source.as_ref()),
        }
    }
}

impl std::fmt::Display for ImageMetadataError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::UnsupportedPhoto { path, source } => {
                _ = source;
                // All of these custom instances are just to customize how path is displayed.
                write!(
                    f,
                    "not a supported photo file: {}",
                    dunce::simplified(path).display()
                )
            }
            Self::Other(source) => {
                _ = source;
                write!(f, "unexpected error reading image metadata")
            }
        }
    }
}

impl From<anyhow::Error> for ImageMetadataError {
    fn from(e: anyhow::Error) -> Self {
        Self::Other(e)
    }
}

// TODO: How can I avoid hand-writing these instances?
// std::error::Error overlaps with From<T> for T.

impl From<turbojpeg::Error> for ImageMetadataError {
    fn from(e: turbojpeg::Error) -> Self {
        Self::Other(e.into())
    }
}

impl From<std::array::TryFromSliceError> for ImageMetadataError {
    fn from(e: std::array::TryFromSliceError) -> Self {
        Self::Other(e.into())
    }
}

impl From<std::io::Error> for ImageMetadataError {
    fn from(e: std::io::Error) -> Self {
        Self::Other(e.into())
    }
}

impl From<tokio::task::JoinError> for ImageMetadataError {
    fn from(e: tokio::task::JoinError) -> Self {
        Self::Other(e.into())
    }
}

fn unsupported_photo<E: Into<anyhow::Error>>(
    path: &Path,
) -> impl FnOnce(E) -> ImageMetadataError + '_ {
    move |e| ImageMetadataError::UnsupportedPhoto {
        path: path.to_owned(),
        source: Some(e.into()),
    }
}

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
pub fn may_have_metadata<P: AsRef<Path>>(path: P, file_info: &FileInfo) -> bool {
    // Skip AppleDouble files.
    if path
        .as_ref()
        .file_name()
        .and_then(|b| b.to_str())
        .unwrap_or("")
        .starts_with("._")
    {
        return false;
    }
    // Too large to decode.
    if file_info.size > 40_000_000 {
        return false;
    }
    is_jpeg(&path) || is_heic(&path)
}

/// Computes and returns image metadata including dimensions and
/// perceptual hashes.
pub async fn compute_image_hashes<P: AsRef<Path>>(
    path: P,
) -> Result<ImageMetadata, ImageMetadataError> {
    if is_jpeg(&path) {
        Ok(jpeg::compute_image_hashes(path.as_ref()).await?)
    } else if is_heic(&path) {
        Ok(heic::compute_image_hashes(path.as_ref()).await?)
    } else {
        Err(ImageMetadataError::UnsupportedPhoto {
            path: path.as_ref().to_owned(),
            source: None,
        })
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
