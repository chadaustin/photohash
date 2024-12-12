use crate::iopool;
use crate::model::ExtraHashes;
use crate::model::FileInfo;
use crate::model::Hash32;
use crate::model::ImageMetadata;
use digest::DynDigest;
use enumset::EnumSetType;
use std::io::Read;
use std::path::Path;
use std::path::PathBuf;

mod heic;
mod jpeg;

const READ_SIZE: usize = 65536;

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

#[derive(Debug, EnumSetType)]
pub enum ContentHashType {
    BLAKE3,
    MD5,
    SHA1,
    SHA256,
}

pub type ContentHashSet = enumset::EnumSet<ContentHashType>;

pub fn get_hasher(hash: ContentHashType) -> Box<dyn DynDigest> {
    match hash {
        ContentHashType::BLAKE3 => Box::new(blake3::Hasher::new()),
        ContentHashType::MD5 => Box::new(md5::Md5::default()),
        ContentHashType::SHA1 => Box::new(sha1::Sha1::default()),
        ContentHashType::SHA256 => Box::new(sha2::Sha256::default()),
    }
}

/*
#[derive(Debug)]
pub struct ContentHashes {
    pub blake3: Option<Hash32>,
    pub extra_hashes: ExtraHashes,
}

pub async fn compute_content_hashes(path: PathBuf, which: ContentHashSet) -> anyhow::Result<ContentHashes> {
    iopool::run_in_io_pool(move || {
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
*/

pub async fn compute_blake3(path: PathBuf) -> anyhow::Result<Hash32> {
    // This assumes that computing blake3 is much faster than IO and
    // will not contend with other workers.
    iopool::run_in_io_pool(move || {
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

pub async fn compute_extra_hashes(path: PathBuf) -> anyhow::Result<ExtraHashes> {
    // TODO: To save IO, this could be folded into compute_blake3 on
    // the initial computation.

    iopool::run_in_io_pool(move || {
        let mut extra_hashes = ExtraHashes::default();

        let mut hash_md5 = md5::Md5::default();
        let mut hash_sha1 = sha1::Sha1::default();
        let mut hash_sha256 = sha2::Sha256::default();
        let mut hashers: [(&mut dyn DynDigest, &mut [u8]); 3] = [
            (&mut hash_md5, extra_hashes.md5.get_or_insert_default()),
            (&mut hash_sha1, extra_hashes.sha1.get_or_insert_default()),
            (
                &mut hash_sha256,
                extra_hashes.sha256.get_or_insert_default(),
            ),
        ];
        let mut file = std::fs::File::open(path)?;
        let mut buffer = [0u8; READ_SIZE];
        loop {
            let n = file.read(&mut buffer)?;
            if n == 0 {
                break;
            }
            let buffer = &buffer[..n];
            for (h, _) in &mut hashers {
                h.update(buffer);
            }
        }

        for (h, dest) in &mut hashers {
            h.finalize_into_reset(dest).expect("invalid buffer size");
        }

        Ok(extra_hashes)
    })
    .await
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
